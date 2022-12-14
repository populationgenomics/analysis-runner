"""
Utility methods for analysis-runner server
"""
import os
import json
import uuid
import toml

from aiohttp import web, ClientSession
from cloudpathlib import AnyPath
from hailtop.config import get_deploy_config
from google.cloud import secretmanager, pubsub_v1
from cpg_utils.config import update_dict
from cpg_utils.cloud import email_from_id_token, read_secret
from cpg_utils.hail_batch import cpg_namespace
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

CONFIG_PATH_PREFIXES = {'gcp': 'gs://cpg-config'}

secret_manager = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()


def get_server_config() -> dict:
    """Get the server-config from the secret manager"""
    return json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))


async def _get_hail_version(environment: str) -> str:
    """ASYNC get hail version for the hail server in the local deploy_config"""
    if not environment == 'gcp':
        raise web.HTTPBadRequest(
            reason=f'Unsupported Hail Batch deploy config environment: {environment}'
        )

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


def check_dataset_and_group(server_config, environment: str, dataset, email) -> dict:
    """Check that the email address is a member of the {dataset}-access@popgen group"""
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        raise web.HTTPForbidden(
            reason=(
                f'Dataset "{dataset}" is not part of: '
                f'{", ".join(server_config.keys())}'
            )
        )

    if environment not in dataset_config:
        raise web.HTTPBadRequest(
            reason=f'Dataset {dataset} does not support the {environment} environment'
        )

    # do this to check access-members cache
    gcp_project = dataset_config.get('gcp', {}).get('projectId')

    if not gcp_project:
        raise web.HTTPBadRequest(
            reason=f'The analysis-runner does not support checking group members for the {environment} environment'
        )

    group_members = read_secret(
        dataset_config['projectId'], f'{dataset}-access-members-cache'
    ).split(',')
    if email not in group_members:
        raise web.HTTPForbidden(
            reason=f'{email} is not a member of the {dataset} access group'
        )

    return dataset_config[environment]


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
    config_path,
    cwd,
    environment,
    **kwargs,
):
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """
    output_dir = f'gs://cpg-{dataset}-{cpg_namespace(access_level)}/{output_prefix}'

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
        'configPath': config_path,
        'cwd': cwd,
        'environment': environment**kwargs,
    }


def run_batch_job_and_print_url(batch, wait, environment):
    """Call batch.run(), return the URL, and wait for job to  finish if wait=True"""
    bc_batch = batch.run(wait=False)

    deploy_config = get_deploy_config(environment)
    url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

    if wait:
        status = bc_batch.wait()
        if status['state'] != 'success':
            raise web.HTTPBadRequest(reason=f'{url} failed')

    return url


def validate_image(container: str, is_test: bool):
    """
    Check that the image is valid for the access_level
    """
    return is_test or any(
        container.startswith(prefix) for prefix in ALLOWED_CONTAINER_IMAGE_PREFIXES
    )


def write_config(config: dict, environment: str) -> str:
    """Writes the given config dictionary to a blob and returns its unique path."""
    prefix = CONFIG_PATH_PREFIXES.get(environment)
    if not prefix:
        raise web.HTTPBadRequest(reason=f'Bad environment for config: {environment}')

    config_path = AnyPath(prefix) / (str(uuid.uuid4()) + '.toml')
    with config_path.open('w') as f:
        toml.dump(config, f)
    return str(config_path)


def get_baseline_run_config(
    environment: str,
    gcp_project_id,
    dataset,
    access_level,
    output_prefix,
    driver: str | None = None,
) -> dict:
    """
    Returns the baseline config of analysis-runner specified default values,
    as well as pre-generated templates with common locations and resources.
    permits overriding the default driver image
    """
    config_prefix = CONFIG_PATH_PREFIXES.get(environment)
    if not config_prefix:
        raise web.HTTPBadRequest(reason=f'Bad environment for config: {environment}')

    baseline_config = {
        'hail': {
            'billing_project': dataset,
            # TODO: how would this work for Azure
            'bucket': f'cpg-{dataset}-hail',
        },
        'workflow': {
            'access_level': access_level,
            'dataset': dataset,
            'dataset_gcp_project': gcp_project_id,
            'driver_image': driver or DRIVER_IMAGE,
            'output_prefix': output_prefix,
        },
    }
    template_paths = [
        AnyPath(config_prefix) / 'templates' / suf
        for suf in [
            'images/images.toml',
            'references/references.toml',
            f'storage/{environment}/{dataset}-{cpg_namespace(access_level)}.toml',
        ]
    ]
    if missing := [p for p in template_paths if not p.exists()]:
        raise ValueError(f'Missing expected template configs: {missing}')

    for path in template_paths:
        with path.open() as f:
            update_dict(baseline_config, toml.load(f))
    return baseline_config
