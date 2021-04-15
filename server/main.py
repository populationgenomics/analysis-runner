"""The analysis-runner server, running Hail Batch pipelines on users' behalf."""

import datetime
import json
import logging
import os
from shlex import quote
from aiohttp import web, ClientSession

from google.auth import jwt
from google.cloud import secretmanager
from google.cloud import pubsub_v1

import hailtop.batch as hb
from hailtop.config import get_deploy_config

import cloud_identity

GITHUB_ORG = 'populationgenomics'
METADATA_PREFIX = '/tmp/metadata'
PUBSUB_TOPIC = 'projects/analysis-runner/topics/submissions'

DRIVER_IMAGE = os.getenv('DRIVER_IMAGE')
assert DRIVER_IMAGE

COMBINE_METADATA = """
import json
import sys

def load(filename):
    text = open(filename).read().strip()
    val = json.loads(text) if len(text) else []
    return val if type(val) is list else [val]

print(json.dumps(load(sys.argv[1]) + load(sys.argv[2])))
"""

routes = web.RouteTableDef()

secret_manager = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()


def _email_from_id_token(auth_header: str) -> str:
    """Decodes the ID token (JWT) to get the email address of the caller.

    See http://bit.ly/2YAIkzy for details.

    This function assumes that the token has been verified beforehand."""

    id_token = auth_header[7:]  # Strip the 'bearer' / 'Bearer' prefix.
    id_info = jwt.decode(id_token, verify=False)
    return id_info['email']


def _read_secret(name: str) -> str:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    secret_name = f'projects/analysis-runner/secrets/{name}/versions/latest'
    response = secret_manager.access_secret_version(request={'name': secret_name})
    return response.payload.data.decode('UTF-8')


async def _get_hail_version() -> str:
    deploy_config = get_deploy_config()
    url = deploy_config.url('query', f'/api/v1alpha/version')
    async with ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


# pylint: disable=too-many-statements
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
        output_dir = params['output']
        if not output_dir.startswith('gs://'):
            raise web.HTTPBadRequest(
                reason='Output directory needs to start with "gs://"'
            )

        dataset = params['dataset']
        config = json.loads(_read_secret('server-config'))
        if dataset not in config:
            raise web.HTTPForbidden(
                reason=(
                    f'Dataset "{dataset}" is not part of: '
                    f'{", ".join(config.keys())}'
                )
            )

        group_name = f'{dataset}-access@populationgenomics.org.au'
        if not cloud_identity.check_group_membership(email, group_name):
            raise web.HTTPForbidden(reason=f'{email} is not a member of {group_name}')

        repo = params['repo']
        allowed_repos = config[dataset]['allowedRepos']
        if repo not in allowed_repos:
            raise web.HTTPForbidden(
                reason=(
                    f'Repository "{repo}" is not one of the allowed repositories: '
                    f'{", ".join(allowed_repos)}'
                )
            )

        access_level = params['accessLevel']
        hail_token = config[dataset].get(f'{access_level}Token')
        if not hail_token:
            raise web.HTTPBadRequest(reason=f'Invalid access level "{access_level}"')

        hail_bucket = f'cpg-{dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=dataset,
            bucket=hail_bucket,
            token=hail_token,
        )

        commit = params['commit']
        if not commit or commit == 'HEAD':
            raise web.HTTPBadRequest(reason='Invalid commit parameter')

        script = params['script']
        if not script:
            raise web.HTTPBadRequest(reason='Invalid script parameter')

        if not isinstance(script, list):
            raise web.HTTPBadRequest(reason='Script parameter expects an array')

        script_file, *script_args = script

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {repo}:{commit}/{" ".join(script)}'

        batch = hb.Batch(backend=backend, name=batch_name)

        job = batch.new_job(name='driver')
        job.image(DRIVER_IMAGE)

        job.env('HAIL_BILLING_PROJECT', dataset)
        job.env('HAIL_BUCKET', hail_bucket)
        job.env('OUTPUT', output_dir)

        # Note: for private GitHub repos we'd need to use a token to clone.
        # Any job commands here are evaluated in a bash shell, so user arguments should
        # be escaped to avoid command injection.
        job.command(f'git clone https://github.com/{GITHUB_ORG}/{quote(repo)}.git')
        job.command(f'cd {quote(repo)}')
        # Except for the "test" access level, we check whether commits have been
        # reviewed by verifying that the given commit is in the main branch.
        if access_level != 'test':
            job.command('git checkout main')
            job.command(f'git merge-base --is-ancestor {quote(commit)} HEAD')
        job.command(f'git checkout {quote(commit)}')
        job.command(f'git submodule init')
        job.command(f'git submodule update')
        # Make sure the file is in a subdirectory of the repository.
        job.command(f'[[ -f {quote(script_file)} ]]')
        job.command(f'[[ $(realpath {quote(script_file)}) == $PWD* ]]')
        # Change the working directory (to make relative file look-ups more intuitive).
        job.command(f'cd $(dirname {quote(script_file)})')

        # This metadata dictionary gets stored at the output_dir location.
        hail_version = await _get_hail_version()
        timestamp = datetime.datetime.now().astimezone().isoformat()
        metadata = json.dumps(
            {
                'timestamp': timestamp,
                'dataset': dataset,
                'user': email,
                'accessLevel': access_level,
                'repo': repo,
                'commit': commit,
                'script': ' '.join(script),
                'description': params['description'],
                'output': output_dir,
                'hailVersion': hail_version,
                'driverImage': DRIVER_IMAGE,
            }
        )

        # Publish the metadata to Pub/Sub. Wait for the result before running the batch.
        publisher.publish(PUBSUB_TOPIC, metadata.encode('utf-8')).result()

        job.command(
            f'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
        )

        # Append metadata information, in case the same output directory gets used
        # multiple times.
        job.command(
            f'gsutil cp {quote(output_dir)}/metadata.json {METADATA_PREFIX}_old.json '
            f'|| touch {METADATA_PREFIX}_old.json'
        )
        job.command(f'echo {quote(metadata)} > {METADATA_PREFIX}_new.json')
        job.command(f'echo "{COMBINE_METADATA}" > {METADATA_PREFIX}_combiner.py')
        job.command(
            f'python3 {METADATA_PREFIX}_combiner.py {METADATA_PREFIX}_old.json '
            f'{METADATA_PREFIX}_new.json > {METADATA_PREFIX}.json'
        )
        job.command(
            f'gsutil cp {METADATA_PREFIX}.json {quote(output_dir)}/metadata.json'
        )

        # Finally, run the script.
        escaped_args = ' '.join(quote(s) for s in script_args if s)
        job.command(f'python3 $(basename {quote(script_file)}) {escaped_args}')

        bc_batch = batch.run(wait=False)

        deploy_config = get_deploy_config()
        url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

        if params.get('wait', False):
            status = bc_batch.wait()
            if status['state'] != 'success':
                raise web.HTTPBadRequest(reason=f'{url} failed')

        return web.Response(text=f'{url}\n')
    except KeyError as e:
        logging.error(e)
        raise web.HTTPBadRequest(reason='Missing request parameter')


async def init_func():
    """Initializes the app."""
    app = web.Application()
    app.add_routes(routes)
    return app
