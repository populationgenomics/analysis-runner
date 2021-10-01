"""
Utility methods for analysis-runner server
"""
import os
import json
from shlex import quote

from aiohttp import web, ClientSession
from hailtop.config import get_deploy_config
from google.cloud import secretmanager, pubsub_v1

from cpg_utils.cloud import email_from_id_token, read_secret

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
GITHUB_ORG = 'populationgenomics'
METADATA_PREFIX = '/tmp/metadata'
PUBSUB_TOPIC = f'projects/{ANALYSIS_RUNNER_PROJECT_ID}/topics/submissions'
CROMWELL_URL = 'https://cromwell.populationgenomics.org.au'
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


def get_server_config() -> dict:
    """Get the server-config from the secret manager"""
    return json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))


def get_cromwell_key(dataset, access_level):
    """Get Cromwell key from secrets"""
    secret_name = f'{dataset}-cromwell-{access_level}-key'
    return read_secret(ANALYSIS_RUNNER_PROJECT_ID, secret_name)


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


def check_allowed_repos(server_config, dataset, repo):
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
    """Checks that output_dir doesn't start with 'gs://' and strips trailing slashes."""
    if output_dir.startswith('gs://'):
        raise web.HTTPBadRequest(reason='Output directory cannot start with "gs://"')
    return output_dir.rstrip('/')  # Strip trailing slash.


def check_dataset_and_group(server_config, dataset, email):
    """Check that the email address is a member of the {dataset}-access@popgen group"""
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        raise web.HTTPForbidden(
            reason=(
                f'Dataset "{dataset}" is not part of: '
                f'{", ".join(server_config.keys())}'
            )
        )

    group_members = read_secret(
        dataset_config['projectId'], f'{dataset}-access-members-cache'
    ).split(',')
    if email not in group_members:
        raise web.HTTPForbidden(
            reason=f'{email} is not a member of the {dataset} access group'
        )


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
    output_suffix,
    driver_image,
    cwd,
    **kwargs,
):
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """
    bucket_type = 'test' if access_level == 'test' else 'main'
    output_dir = f'gs://cpg-{dataset}-{bucket_type}/{output_suffix}'

    return {
        'timestamp': timestamp,
        'dataset': dataset,
        'user': user,
        'accessLevel': access_level,
        'repo': repo,
        'commit': commit,
        'script': script,
        'description': description,
        'output': output_dir,
        'driverImage': driver_image,
        'cwd': cwd,
        **kwargs,
    }


def prepare_git_job(
    job,
    dataset,
    access_level,
    output_suffix,
    repo,
    commit,
    metadata_str: str,
    print_all_statements=True,
):
    """
    Takes a hail job, and:
        * Sets the driver image
        * Sets DRIVER_IMAGE, DATASET, ACCESS_LEVEL, and GOOGLE_APPLICATION_CREDENTIALS
          environment variables
        * Activates the google service account
        * Clones the repository, and
            * if access_level != "test": check the desired commit is on 'main'
            * check out the specific commit
        * if metadata_str is provided (an already JSON-ified metadata obj), then:
            *  copy analysis-runner.json to the metadata bucket
    """
    job.image(DRIVER_IMAGE)

    job.env('DRIVER_IMAGE', DRIVER_IMAGE)
    job.env('DATASET', dataset)
    job.env('ACCESS_LEVEL', access_level)
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')

    # Use "set -x" to print the commands for easier debugging.
    if print_all_statements:
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

    if metadata_str:
        # Append metadata information, in case the same output directory gets used
        # multiple times.
        bucket_type = 'test' if access_level == 'test' else 'main'
        metadata_path = f'gs://cpg-{dataset}-{bucket_type}-analysis/metadata/{output_suffix}/analysis-runner.json'
        job.command(
            f'gsutil cp {quote(metadata_path)} {METADATA_PREFIX}_old.json '
            f'|| touch {METADATA_PREFIX}_old.json'
        )
        job.command(f'echo {quote(metadata_str)} > {METADATA_PREFIX}_new.json')
        job.command(f'echo "{COMBINE_METADATA}" > {METADATA_PREFIX}_combiner.py')
        job.command(
            f'python3 {METADATA_PREFIX}_combiner.py {METADATA_PREFIX}_old.json '
            f'{METADATA_PREFIX}_new.json > {METADATA_PREFIX}.json'
        )
        job.command(f'gsutil cp {METADATA_PREFIX}.json {quote(metadata_path)}')

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
