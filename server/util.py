"""
Utility methods for analysis-runner server
"""
import os
import json
from shlex import quote

from aiohttp import web, ClientSession
from hailtop.config import get_deploy_config
from google.cloud import secretmanager, pubsub_v1

from cpg_utils.cloud import is_google_group_member, email_from_id_token

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

secret_manager = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()


def _read_secret(name: str) -> str:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    secret_name = f'projects/analysis-runner/secrets/{name}/versions/latest'
    response = secret_manager.access_secret_version(request={'name': secret_name})
    return response.payload.data.decode('UTF-8')


server_config = json.loads(_read_secret('server-config'))
CROMWELL_URL = 'https://cromwell.populationgenomics.org.au'


async def _get_hail_version() -> str:
    """ASYNC get hail version for the hail server in the local deploy_config"""
    deploy_config = get_deploy_config()
    url = deploy_config.url('query', f'/api/v1alpha/version')
    async with ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


def get_email_from_request(request):
    """
    Get 'Authorization' from request header,
    and parse the email address using cpg-util
    """
    auth_header = request.headers.get('Authorization')
    if auth_header is None:
        raise web.HTTPUnauthorized(reason='Missing authorization header')

    try:
        id_token = auth_header[7:]  # Strip the 'bearer' / 'Bearer' prefix.
        return email_from_id_token(id_token)
    except ValueError as e:
        raise web.HTTPForbidden(reason='Invalid authorization header') from e


def check_allowed_repos(dataset, repo):
    """Check that repo is the in server_config allowedRepos for the dataset"""
    allowed_repos = server_config[dataset]['allowedRepos']
    if repo not in allowed_repos:
        raise web.HTTPForbidden(
            reason=(
                f'Repository "{repo}" is not one of the allowed repositories: '
                f'{", ".join(allowed_repos)}'
            )
        )


def validate_output_dir(output_dir: str):
    """
    Check that output_dir:
        * is a bucket;
        * not the bucket root;
        * starts with 'cpg-';
        * then strip the trailing '/'.
    """
    if not output_dir.startswith('gs://cpg-'):
        raise web.HTTPBadRequest(
            reason='Output directory needs to start with "gs://cpg-"'
        )
    if output_dir.count('/') <= 2:
        raise web.HTTPBadRequest(reason='Output directory cannot be a bucket root')

    return output_dir.rstrip('/')  # Strip trailing slash.


def check_dataset_and_group(dataset, email):
    """Check that the email address is a member of the {dataset}-access@popgen group"""
    if dataset not in server_config:
        raise web.HTTPForbidden(
            reason=(
                f'Dataset "{dataset}" is not part of: '
                f'{", ".join(server_config.keys())}'
            )
        )

    group_name = f'{dataset}-access@populationgenomics.org.au'
    if not is_google_group_member(email, group_name):
        raise web.HTTPForbidden(reason=f'{email} is not a member of {group_name}')


# pylint: disable=too-many-arguments
def get_analysis_runner_metadata(
    timestamp,
    dataset,
    user,
    access_level,
    repo,
    commit,
    script,
    description,
    output,
    driver_image,
    cwd,
    **kwargs,
):
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """

    return {
        'timestamp': timestamp,
        'dataset': dataset,
        'user': user,
        'accessLevel': access_level,
        'repo': repo,
        'commit': commit,
        'script': script,
        'description': description,
        'output': output,
        'driverImage': driver_image,
        'cwd': cwd,
        **kwargs,
    }


def prepare_git_job(
    job, dataset, output_dir, access_level, repo, commit, metadata_str: str
):
    """
    Takes a hail job, and:
        * Sets the driver image
        * Sets HAIL_BILLING_PROJECT, DRIVER_IMAGE env variables
        * Activates the google service account
        * Clones the repository, and
            * if access_level != "test": check the desired commit is on 'main'
            * check out the specific commit
        * if metadata_str is provided (an already JSON-ified metadata obj), then:
            *  copy metadata.json to output bucket (
    """
    job.image(DRIVER_IMAGE)

    job.env('HAIL_BILLING_PROJECT', dataset)
    job.env('DRIVER_IMAGE', DRIVER_IMAGE)
    job.env('ACCESS_LEVEL', access_level)
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')

    # Use "set -x" to print the commands for easier debugging.
    job.command('set -x')

    # activate the google service account
    job.command(
        f'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
    )

    # Note: for private GitHub repos we'd need to use a token to clone.
    # Any job commands here are evaluated in a bash shell, so user arguments should
    # be escaped to avoid command injection.
    job.command(
        f'git clone --recurse-submodules https://github.com/{GITHUB_ORG}/{quote(repo)}.git'
    )
    job.command(f'cd {quote(repo)}')
    # Except for the "test" access level, we check whether commits have been
    # reviewed by verifying that the given commit is in the main branch.
    if access_level != 'test':
        job.command('git checkout main')
        job.command(
            f'git merge-base --is-ancestor {quote(commit)} HEAD || '
            '{ echo "error: commit not merged into main branch"; exit 1; }'
        )
    job.command(f'git checkout {quote(commit)}')
    job.command(f'git submodule update')
    # Change the working directory (usually to make relative scripts possible).

    if metadata_str:
        # Append metadata information, in case the same output directory gets used
        # multiple times.
        job.command(
            f'gsutil cp {quote(output_dir)}/metadata.json {METADATA_PREFIX}_old.json '
            f'|| touch {METADATA_PREFIX}_old.json'
        )
        job.command(f'echo {quote(metadata_str)} > {METADATA_PREFIX}_new.json')
        job.command(f'echo "{COMBINE_METADATA}" > {METADATA_PREFIX}_combiner.py')
        job.command(
            f'python3 {METADATA_PREFIX}_combiner.py {METADATA_PREFIX}_old.json '
            f'{METADATA_PREFIX}_new.json > {METADATA_PREFIX}.json'
        )
        job.command(
            f'gsutil cp {METADATA_PREFIX}.json {quote(output_dir)}/metadata.json'
        )

    return job


def run_batch_job_and_print_url(batch, wait):
    """Call batch.run(), return the URL, and wait for job to  finish if wait=True"""
    bc_batch = batch.run(wait=False)

    deploy_config = get_deploy_config()
    url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

    if wait:
        status = bc_batch.wait()
        if status['state'] != 'success':
            raise web.HTTPBadRequest(reason=f'{url} failed')

    return url
