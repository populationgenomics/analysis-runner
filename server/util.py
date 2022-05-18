"""
Utility methods for analysis-runner server
"""
import os
import json
from shlex import quote
from typing import Any, Dict
import uuid
import toml

from aiohttp import web, ClientSession
from cloudpathlib import AnyPath
from hailtop.config import get_deploy_config
from google.cloud import secretmanager, pubsub_v1
from cpg_utils.cloud import email_from_id_token, read_secret
from analysis_runner.constants import ANALYSIS_RUNNER_PROJECT_ID

GITHUB_ORG = 'populationgenomics'
METADATA_PREFIX = '/tmp/metadata'
PUBSUB_TOPIC = f'projects/{ANALYSIS_RUNNER_PROJECT_ID}/topics/submissions'
ALLOWED_CONTAINER_IMAGE_PREFIXES = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/',
    'australia-southeast1-docker.pkg.dev/cpg-common/',
)
DRIVER_IMAGE = os.getenv('DRIVER_IMAGE')
assert DRIVER_IMAGE
IMAGE_REGISTRY_PREFIX = 'australia-southeast1-docker.pkg.dev/cpg-common/images'
REFERENCE_PREFIX = 'gs://cpg-reference'
CONFIG_PATH_PREFIX = 'gs://cpg-config'

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


async def _get_hail_version() -> str:
    """ASYNC get hail version for the hail server in the local deploy_config"""
    deploy_config = get_deploy_config()
    url = deploy_config.url('batch', f'/api/v1alpha/version')
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
    output_prefix,
    driver_image,
    cwd,
    **kwargs,
):
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """
    bucket_type = 'test' if access_level == 'test' else 'main'
    output_dir = f'gs://cpg-{dataset}-{bucket_type}/{output_prefix}'

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


def write_metadata_to_bucket(
    job, access_level: str, dataset: str, output_prefix: str, metadata_str: str
):
    """
    Copy analysis-runner.json to the metadata bucket

    Append metadata information, in case the same
    output directory gets used multiple times.
    """

    bucket_type = 'test' if access_level == 'test' else 'main'
    metadata_path = f'gs://cpg-{dataset}-{bucket_type}-analysis/metadata/{output_prefix}/analysis-runner.json'
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


def validate_image(container: str, is_test: bool):
    """
    Check that the image is valid for the access_level
    """
    return is_test or any(
        container.startswith(prefix) for prefix in ALLOWED_CONTAINER_IMAGE_PREFIXES
    )


def write_config(config: Dict[str, Any]) -> str:
    """Writes the given config dictionary to a blob and returns its unique path."""
    config_path = AnyPath(CONFIG_PATH_PREFIX) / (str(uuid.uuid4()) + '.toml')
    with config_path.open('w') as f:
        toml.dump(config, f)
    return str(config_path)
