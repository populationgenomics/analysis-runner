# pylint: disable=too-many-function-args
"""
Utility methods for analysis-runner server
"""
import json
import os
import random
import uuid
from typing import Any, Dict

import toml
from aiohttp import ClientSession, web
from cloudpathlib import AnyPath
from google.cloud import pubsub_v1, secretmanager

from hailtop.batch import Batch
from hailtop.config import get_deploy_config

from analysis_runner.constants import ANALYSIS_RUNNER_PROJECT_ID
from cpg_utils.cloud import email_from_id_token, is_member_in_cached_group, read_secret
from cpg_utils.config import AR_GUID_NAME, update_dict
from cpg_utils.hail_batch import cpg_namespace

GITHUB_ORG = 'populationgenomics'
METADATA_PREFIX = '/$TMPDIR/metadata'
PUBSUB_TOPIC = f'projects/{ANALYSIS_RUNNER_PROJECT_ID}/topics/submissions'
ALLOWED_CONTAINER_IMAGE_PREFIXES = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/',
    'australia-southeast1-docker.pkg.dev/cpg-common/images/',
)
DRIVER_IMAGE = os.getenv('DRIVER_IMAGE')
assert DRIVER_IMAGE and isinstance(DRIVER_IMAGE, str)

MEMBERS_CACHE_LOCATION = os.getenv('MEMBERS_CACHE_LOCATION')
assert MEMBERS_CACHE_LOCATION

CONFIG_PATH_PREFIXES = {'gcp': 'gs://cpg-config'}

secret_manager = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()


def generate_ar_guid():
    """Generate guid for tracking analysis-runner jobs"""
    guid = str(uuid.uuid4())
    # guids can't start with a number (GCP labels won't accept it)
    if guid[0].isdigit():
        # Standard pseudo-random generators are not suitable for cryptographic purposes
        # but that's not a problem for our purposes
        guid = random.choice('abcdef') + guid[1:]  # noqa: S311
    return guid.lower()


def get_server_config() -> dict:
    """Get the server-config from the secret manager"""
    server_config = os.getenv('SERVER_CONFIG')
    if server_config:
        return json.loads(server_config)

    server_config_value = read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    if server_config_value:
        return json.loads(server_config_value)

    raise web.HTTPInternalServerError(reason='Failed to read server-config secret')


async def _get_hail_version(environment: str) -> str:
    """ASYNC get hail version for the hail server in the local deploy_config"""
    if not environment == 'gcp':
        raise web.HTTPBadRequest(
            reason=f'Unsupported Hail Batch deploy config environment: {environment}',
        )

    deploy_config = get_deploy_config()
    url = deploy_config.url('batch', '/api/v1alpha/version')
    async with ClientSession() as session, session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


def get_email_from_request(request: web.Request):
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


def check_allowed_repos(dataset_config: Dict, repo: str):
    """Check that repo is the in server_config allowedRepos for the dataset"""
    allowed_repos = dataset_config['allowedRepos']
    if repo not in allowed_repos:
        raise web.HTTPForbidden(
            reason=(
                f'Repository "{repo}" is not one of the allowed repositories, you may'
                'need add it to the repository-map: '
                'https://github.com/populationgenomics/cpg-infrastructure-private/blob/main/tokens/repository-map.json'
            ),
        )


def validate_output_dir(output_dir: str):
    """Checks that output_dir doesn't start with 'gs://' and strips trailing slashes."""
    if output_dir.startswith('gs://'):
        raise web.HTTPBadRequest(reason='Output directory cannot start with "gs://"')
    return output_dir.rstrip('/')  # Strip trailing slash.


def check_dataset_and_group(
    server_config: Dict,
    environment: str,
    dataset: str,
    email: str,
) -> dict:
    """Check that the email address is a member of the {dataset}-access@popgen group"""
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        raise web.HTTPForbidden(
            reason=(
                f'The dataset "{dataset}" is not present in the server config, have you'
                'added it to the repository map: '
                'https://github.com/populationgenomics/cpg-infrastructure-private/blob/main/tokens/repository-map.json'
            ),
        )

    if environment not in dataset_config:
        raise web.HTTPBadRequest(
            reason=f'Dataset {dataset} does not support the {environment} environment',
        )

    # do this to check access-members cache
    gcp_project = dataset_config.get('gcp', {}).get('projectId')

    if not gcp_project:
        raise web.HTTPBadRequest(
            reason='The analysis-runner does not support checking group members for '
            f'the {environment} environment',
        )
    if not is_member_in_cached_group(
        f'{dataset}-analysis',
        email,
        members_cache_location=MEMBERS_CACHE_LOCATION,
    ):
        raise web.HTTPForbidden(
            reason=f'{email} is not a member of the {dataset} analysis group',
        )

    return dataset_config


# pylint: disable=too-many-arguments
def get_analysis_runner_metadata(
    *,
    ar_guid: str,
    name: str,
    timestamp: str,
    dataset: str,
    user: str,
    access_level: str,
    repo: str,
    commit: str,
    script: str,
    description: str,
    output_prefix: str,
    driver_image: str,
    config_path: str,
    cwd: str,
    environment: str,
    **kwargs: Any,
):
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """
    output_dir = f'gs://cpg-{dataset}-{cpg_namespace(access_level)}/{output_prefix}'

    return {
        AR_GUID_NAME: ar_guid,
        'name': name,
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
        'environment': environment,
        **kwargs,
    }


def run_batch_job_and_print_url(batch: Batch, wait: bool, environment: str):
    """Call batch.run(), return the URL, and wait for job to  finish if wait=True"""
    if not environment == 'gcp':
        raise web.HTTPBadRequest(
            reason=f'Unsupported Hail Batch deploy config environment: {environment}',
        )
    bc_batch = batch.run(wait=False)

    deploy_config = get_deploy_config()
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


def write_config(ar_guid: str, config: dict, environment: str) -> str:
    """Writes the given config dictionary to a blob and returns its unique path."""
    prefix = CONFIG_PATH_PREFIXES.get(environment)
    if not prefix:
        raise web.HTTPBadRequest(reason=f'Bad environment for config: {environment}')

    config_path = AnyPath(prefix) / (ar_guid + '.toml')
    with config_path.open('w') as f:
        toml.dump(config, f)
    return str(config_path)


def get_baseline_run_config(
    ar_guid: str,
    environment: str,
    gcp_project_id: str,
    dataset: str,
    access_level: str,
    output_prefix: str,
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
            AR_GUID_NAME: ar_guid,
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
            'infrastructure.toml',
            'images/images.toml',
            'references/references.toml',
            f'storage/{environment}/{dataset}-{cpg_namespace(access_level)}.toml',
            'infrastructure.toml',
        ]
    ]
    if missing := [p for p in template_paths if not p.exists()]:
        raise ValueError(f'Missing expected template configs: {missing}')

    for path in template_paths:
        with path.open() as f:
            update_dict(baseline_config, toml.load(f))
    return baseline_config
