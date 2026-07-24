"""
Utility methods for analysis-runner server
"""

import json
import os
import random
import uuid
from typing import Any

import toml
from aiohttp import ClientSession, web
from cachetools.func import ttl_cache
from cloudpathlib import AnyPath
from google.cloud import pubsub_v1, secretmanager

import hailtop.batch as hb
from hailtop.config import get_deploy_config

from cpg_utils.cloud import email_from_id_token, read_secret
from cpg_utils.config import AR_GUID_NAME, get_cpg_namespace, update_dict
from cpg_utils.membership import is_member_in_cached_group

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
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
SUPPORTED_CLOUD_ENVIRONMENTS = {'gcp'}
DEFAULT_STATUS_REPORTER = 'metamist'

ALLOWED = 'https://github.com/populationgenomics/cpg-infrastructure-private/blob/main/datasets/{}/repositories.yaml'

secret_manager = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()


def generate_ar_guid() -> str:
    """Generate guid for tracking analysis-runner jobs"""
    guid = str(uuid.uuid4())
    # guids can't start with a number (GCP labels won't accept it)
    if guid[0].isdigit():
        # Standard pseudo-random generators are not suitable for cryptographic purposes
        # but that's not a problem for our purposes
        guid = random.choice('abcdef') + guid[1:]  # noqa: S311
    return guid.lower()


# cache the result for 60 seconds, so we can call this function multiple times
@ttl_cache(maxsize=1, ttl=600)
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


def get_email_from_request(request: web.Request) -> str:
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


def check_allowed_repos(dataset_config: dict, repo: str, dataset: str):
    """Check that repo is the in server_config allowedRepos for the dataset"""
    allowed_repos = dataset_config['allowedRepos']
    if repo not in allowed_repos:
        allowed_path = ALLOWED.format(dataset)
        raise web.HTTPForbidden(
            reason=(
                f'Repository "{repo}" is not one of the allowed repositories, you may '
                f'need to add it to the permitted repository list: {allowed_path}'
            ),
        )


def validate_output_dir(output_dir: str) -> str:
    """Checks that output_dir doesn't start with 'gs://' and strips trailing slashes."""
    if output_dir.startswith('gs://'):
        raise web.HTTPBadRequest(reason='Output directory cannot start with "gs://"')
    return output_dir.rstrip('/')  # Strip trailing slash.


def check_dataset_and_group(
    server_config: dict,
    environment: str,
    dataset: str,
    email: str,
) -> dict:
    """Check that the email address is a member of the {dataset}-access@popgen group"""
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        allowed_path = ALLOWED.format(dataset)
        raise web.HTTPForbidden(
            reason=(
                f'The dataset "{dataset}" is not present in the server config, have you '
                f'added it to the repository list: {allowed_path}'
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


def get_analysis_runner_metadata(
    *,
    ar_guid: str,
    name: str,
    timestamp: str,
    dataset: str,
    user: str,
    access_level: str,
    repo: str | None,
    commit: str | None,
    script: str,
    description: str,
    output_prefix: str,
    driver_image: str,
    config_path: str,
    cwd: str | None,
    environment: str,
    **kwargs: Any,
) -> dict:
    """
    Get well-formed analysis-runner metadata, requiring the core listed keys
    with some flexibility to provide your own keys (as **kwargs)
    """
    output_dir = f'gs://cpg-{dataset}-{get_cpg_namespace(access_level)}/{output_prefix}'

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


def validate_image(container: str, is_test: bool) -> bool:
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
            'status_reporter': DEFAULT_STATUS_REPORTER,
        },
    }
    template_paths = [
        AnyPath(config_prefix) / 'templates' / suf
        for suf in [
            'infrastructure.toml',
            'images/images.toml',
            'references/references.toml',
            f'storage/{environment}/{dataset}-{get_cpg_namespace(access_level)}.toml',
            'infrastructure.toml',
        ]
    ]
    if missing := [p for p in template_paths if not p.exists()]:
        raise ValueError(f'Missing expected template configs: {missing}')

    for path in template_paths:
        with path.open() as f:
            update_dict(baseline_config, toml.load(f))
    return baseline_config


def get_and_check_script(params: dict) -> list[str]:
    script = params.get('script')
    if not script:
        raise web.HTTPBadRequest(reason='Missing script parameter')
    if not isinstance(script, list):
        raise web.HTTPBadRequest(reason='Script parameter expects an array')

    return script


def get_and_check_repository(
    params: dict,
    dataset_config: dict,
    dataset: str,
) -> str | None:
    if not (repo := params.get('repo')):
        return None

    check_allowed_repos(
        dataset_config=dataset_config,
        repo=repo,
        dataset=dataset,
    )
    return repo


def get_and_check_commit(params: dict, repo: str | None) -> str | None:
    commit = params.get('commit')
    if not commit and repo:
        raise web.HTTPBadRequest(reason='Missing commit parameter')
    if commit and not repo:
        raise web.HTTPBadRequest(reason='Missing repo parameter')

    if commit == 'HEAD':
        raise web.HTTPBadRequest(reason='Invalid commit parameter')

    return commit


def get_and_check_cloud_environment(params: dict) -> str:
    cloud_environment = params.get('cloud_environment', 'gcp')
    if cloud_environment not in SUPPORTED_CLOUD_ENVIRONMENTS:
        raise web.HTTPBadRequest(
            reason=f'analysis-runner does not yet support the {cloud_environment} environment',
        )
    return cloud_environment


def get_and_check_image(params: dict, is_test: bool) -> str:
    image = params.get('image') or DRIVER_IMAGE
    if not image or not validate_image(image, is_test):
        raise web.HTTPBadRequest(reason=f'Invalid image "{image}"')

    # TODO: find the docker digest for the image
    # to turn the image:floating-tag into a image@sha256:digest, so it's completely provenant

    return image


def get_hail_token(dataset: str, environment_config: dict, access_level: str) -> str:
    hail_token = environment_config.get(f'{access_level}Token')
    if not hail_token:
        raise web.HTTPBadRequest(
            reason=f'Invalid access level ({access_level}) for {dataset}"',
        )

    return hail_token


def add_environment_variables(
    job: hb.batch.job.BashJob,
    environment_variables: dict | None,
):
    if not environment_variables:
        return

    if not isinstance(environment_variables, dict):
        raise ValueError('Expected environment_variables to be dictionary')

    invalid_env_vars = [
        f'{k}={v}' for k, v in environment_variables.items() if not isinstance(v, str)
    ]

    if len(invalid_env_vars) > 0:
        raise ValueError(
            'Some environment_variables values were not strings, got '
            + ', '.join(invalid_env_vars),
        )

    for k, v in environment_variables.items():
        job.env(k, v)
