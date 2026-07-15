"""
A Cloud Function to store analysis-runner submission metadata
in the Sample-Metadata database.
"""

# ruff: noqa: ARG001
import base64
import json
import logging
import os
from typing import Any, Literal
from urllib.parse import urlencode

import google.cloud.logging
import requests

# 1. Initialize the Cloud Logging client
client = google.cloud.logging.Client()
client.setup_logging(
    log_level=logging.INFO,
    format='%(levelname)s %(message)s',
    force=True,
)

DEFAULT_AUDIENCE_URL = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'


def main(data: dict[Literal['data'], str], unused_context: Any):
    """Puts analysis in sample-metadata"""

    logging.getLogger('metamist_ar_meta_handler')
    logging.info('Main function entered with data: %s', data)
    metadata = json.loads(base64.b64decode(data['data']).decode('utf-8'))

    # Extract audienceApiUrl with fallback chain: payload -> env var -> default
    audience = (
        metadata.pop('audienceApiUrl', None)
        or os.getenv('AUDIENCE_URL')
        or DEFAULT_AUDIENCE_URL
    )

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
    q = urlencode(query_params)

    try:
        token = get_identity_token(audience)
        r = requests.put(
            f'{audience}/api/v1/analysis-runner/{project}/?' + q,
            json=meta,
            headers={'Authorization': f'Bearer {token}'},
            timeout=60,
        )
        r.raise_for_status()
        analysis_id = r.text
        logging.info(f'Created analysis with ID = {analysis_id}')
        return analysis_id
    except requests.exceptions.HTTPError as err:
        logging.exception(f'Failed with response: {err.response.text}')
        raise err


def get_identity_token(audience: str) -> str:
    """
    Get identity token
    Source: https://cloud.google.com/functions/docs/securing/function-identity#identity_tokens
    """
    meta_url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity'
    url = f'{meta_url}?audience={audience}&format=full'
    r = requests.get(url=url, headers={'Metadata-Flavor': 'Google'}, timeout=30)
    r.raise_for_status()
    return r.text
