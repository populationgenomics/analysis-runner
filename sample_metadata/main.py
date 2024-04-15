"""
A Cloud Function to store analysis-runner submission metadata
in the Sample-Metadata database.
"""

# ruff: noqa: ARG001

import base64
import json
from typing import Any, Dict, Literal

import requests

AUDIENCE = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'


def sample_metadata(data: Dict[Literal['data'], str], unused_context: Any):
    """Puts analysis in sample-metadata"""

    metadata = json.loads(base64.b64decode(data['data']).decode('utf-8'))

    # remove them from the metadata object so we can pass the remaining values as meta
    project = metadata.pop('dataset')
    access_level = metadata.get('accessLevel')
    ar_guid = metadata.pop('ar-guid')
    user = metadata.pop('user')
    access_level = metadata.pop('accessLevel')
    repo = metadata.pop('repo')
    commit = metadata.pop('commit')
    script = metadata.pop('script')
    description = metadata.pop('description')
    output_prefix = metadata.pop('output')
    driver_image = metadata.pop('driverImage')
    config_path = metadata.pop('configPath')
    cwd = metadata.pop('cwd')
    environment = metadata.pop('environment')
    hail_version = metadata.pop('hailVersion', None)
    batch_url = metadata.pop('batch_url', None)
    meta = metadata.pop('meta', {}) or {}

    if access_level == 'test':
        project += '-test'

    query_params = {
        'ar_guid': ar_guid,
        'access_level': access_level,
        'repository': repo,
        'commit': commit,
        'script': script,
        'description': description,
        'driver_image': driver_image,
        'config_path': config_path,
        'cwd': cwd,
        'environment': environment,
        'hail_version': hail_version,
        'batch_url': batch_url,
        'submitting_user': user,
        'output_path': output_prefix,
    }
    q = '&'.join([f'{k}={v}' for k, v in query_params.items()])

    try:
        token = get_identity_token()
        r = requests.put(
            f'{AUDIENCE}/api/v1/analysis-runner/{project}/?' + q,
            json=meta,
            headers={'Authorization': f'Bearer {token}'},
            timeout=60,
        )
        r.raise_for_status()
        analysis_id = r.text
        print(f'Created analysis with ID = {analysis_id}')
        return analysis_id
    except requests.exceptions.HTTPError as err:
        print(f'Failed with response: {err.response.text}')
        raise err


def get_identity_token() -> str:
    """
    Get identity token
    Source: https://cloud.google.com/functions/docs/securing/function-identity#identity_tokens
    """
    meta_url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity'
    url = f'{meta_url}?audience={AUDIENCE}&format=full'
    r = requests.get(url=url, headers={'Metadata-Flavor': 'Google'}, timeout=30)
    r.raise_for_status()
    return r.text
