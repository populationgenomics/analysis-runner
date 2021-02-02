"""The analysis-runner server, running Hail Batch pipelines on users' behalf."""

import os
import json
import logging
from aiohttp import web

from google.auth import jwt
from google.cloud import secretmanager

import hailtop.batch as hb
from hailtop.config import get_deploy_config

GITHUB_ORG = 'populationgenomics'

ALLOWED_REPOS = {
    'tob-wgs',
}

DRIVER_IMAGE = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:f2fc08ff8c5e'
)

# The GCP_PROJECT is the project ID, not the project name, and is therefore sometimes
# not identical to the dataset name.
GCP_PROJECT = os.getenv('GCP_PROJECT')
assert GCP_PROJECT
DATASET = os.getenv('DATASET')
assert DATASET

HAIL_BUCKET = f'cpg-{DATASET}-hail'
METADATA_FILE = '/tmp/metadata.json'

routes = web.RouteTableDef()

secret_manager = secretmanager.SecretManagerServiceClient()


def _email_from_id_token(auth_header: str) -> str:
    """Decodes the ID token (JWT) to get the email address of the caller.

    See http://bit.ly/2YAIkzy for details.

    This function assumes that the token has been verified beforehand."""

    id_token = auth_header[7:]  # Strip the 'bearer' / 'Bearer' prefix.
    id_info = jwt.decode(id_token, verify=False)
    return id_info['email']


def _shell_escape(arg: str) -> str:
    """Quote-escapes a single shell argument, to avoid command injection."""
    s = arg.replace("'", r"'\''")
    return f"'{s}'"


def _read_secret(name: str) -> str:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    secret_name = f'projects/{GCP_PROJECT}/secrets/{name}/versions/latest'
    response = secret_manager.access_secret_version(request={'name': secret_name})
    return response.payload.data.decode('UTF-8')


@routes.post('/')
async def index(request):
    """Main entry point, responds to the web root."""

    auth_header = request.headers.get('Authorization')
    if auth_header is None:
        raise web.HTTPUnauthorized(reason='Missing authorization header')

    email = _email_from_id_token(auth_header)

    # When accessing a missing entry in the params dict, the resulting KeyError
    # exception gets translated to a Bad Request error in the try block below.
    params = await request.json()
    try:
        output_path = params['output']
        if not output_path.startswith('gs://'):
            raise web.HTTPBadRequest(reason='Output path needs to start with "gs://"')

        repo = params['repo']
        allowed_repos = _read_secret('allowed-repos').split(',')
        if repo not in allowed_repos:
            raise web.HTTPForbidden(
                reason=(
                    f'Repository "{repo}" is not in list of allowed repositories: '
                    f'{", ".join(allowed_repos)}'
                )
            )

        hail_token = _read_secret('hail-token')

        backend = hb.ServiceBackend(
            billing_project=DATASET,
            bucket=HAIL_BUCKET,
            token=hail_token,
        )

        commit = params['commit']
        script = params['script']

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {repo}:{commit}/{script}'

        batch = hb.Batch(backend=backend, name=batch_name)

        job = batch.new_job(name='driver')
        job.image(DRIVER_IMAGE)

        job.env('HAIL_BILLING_PROJECT', DATASET)
        job.env('HAIL_BUCKET', HAIL_BUCKET)
        job.env('OUTPUT', output_path)

        # Note: for private GitHub repos we'd need to use a token to clone.
        # Any job commands here are evaluated in a bash shell, so user arguments should
        # be escaped to avoid command injection.
        job.command(
            f'git clone https://github.com/{GITHUB_ORG}/{_shell_escape(repo)}.git'
        )
        job.command(f'cd {_shell_escape(repo)}')
        job.command('git checkout main')
        # Check whether the given commit is in the main branch.
        job.command(f'git merge-base --is-ancestor {_shell_escape(commit)} HEAD')
        job.command(f'git checkout {_shell_escape(commit)}')
        # Make sure the file is in the repository.
        script_file = script.partition(' ')[0]
        job.command(f'test $(find . -name {_shell_escape(script_file)})')
        # Change the working directory (to make relative file look-ups more intuitive).
        job.command(f'cd $(dirname {_shell_escape(script_file)})')

        # This metadata dictionary gets stored at the output_path location.
        # TODO: also send this to Airtable.
        metadata = {
            'user': email,
            'repo': repo,
            'commit': commit,
            'script': script,
            'description': params['description'],
            'output': output_path,
        }

        job.command(f'echo {_shell_escape(json.dumps(metadata))} > {METADATA_FILE}')
        job.command(
            f'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
        )
        job.command(
            f'gsutil cp {METADATA_FILE} {_shell_escape(output_path)}/metadata.json'
        )

        # Finally, run the script.
        escaped_args = ' '.join(_shell_escape(s) for s in script[1:].split(' ') if s)
        job.command(f'python3 $(basename {_shell_escape(script_file)}) {escaped_args}')

        bc_batch = batch.run(wait=False)

        deploy_config = get_deploy_config()
        url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

        return web.Response(text=f'{url}\n')
    except KeyError as e:
        logging.error(e)
        raise web.HTTPBadRequest(reason='Missing request parameter')


async def init_func():
    """Initializes the app."""
    app = web.Application()
    app.add_routes(routes)
    return app
