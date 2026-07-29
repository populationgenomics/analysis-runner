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

import google.cloud.logging
from metamist.api_client import ApiClient
from metamist.apis import AnalysisRunnerApi
from metamist.configuration import Configuration

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

    project = metadata['dataset']
    access_level = metadata['accessLevel']
    if access_level == 'test':
        project += '-test'

    kwargs = {
        'project': project,
        'ar_guid': metadata.get('ar-guid', ''),
        'access_level': access_level,
        'repository': metadata.get('repo', ''),
        'commit': metadata.get('commit', ''),
        'script': metadata.get('script', ''),
        'description': metadata.get('description', ''),
        'driver_image': metadata.get('driverImage', ''),
        'config_path': metadata.get('configPath', ''),
        'environment': metadata.get('environment', ''),
        'batch_url': metadata.get('batch_url', ''),
        'submitting_user': metadata.get('user', ''),
        'output_path': metadata.get('output', ''),
        'request_body': metadata.get('meta', {}),
        'cwd': metadata.get('cwd'),
        'hail_version': metadata.get('hailVersion'),
    }

    api_client = ApiClient(Configuration(host=audience))
    ar_api = AnalysisRunnerApi(api_client)

    try:
        analysis_id = ar_api.create_analysis_runner_log(**kwargs)
        logging.info(f'Created analysis with ID = {analysis_id}')
        return analysis_id
    except Exception:
        logging.exception('Failed to create analysis-runner log')
        raise
