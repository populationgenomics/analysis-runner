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


def main(data: dict[Literal['data'], str], unused_context: Any):
    """Puts analysis in sample-metadata"""

    logging.getLogger('metamist_ar_meta_handler')
    logging.info('Main function entered with data: %s', data)
    metadata = json.loads(base64.b64decode(data['data']).decode('utf-8'))

    project = metadata.get('dataset')
    access_level = metadata.get('accessLevel')
    if access_level == 'test':
        project += '-test'

    kwargs = {
        'project': project,
        'ar_guid': str(metadata.get('ar-guid')),
        'access_level': str(access_level),
        'repository': str(metadata.get('repo')),
        'commit': str(metadata.get('commit')),
        'script': str(metadata.get('script')),
        'description': str(metadata.get('description')),
        'driver_image': str(metadata.get('driverImage')),
        'config_path': str(metadata.get('configPath')),
        'environment': str(metadata.get('environment')),
        'batch_url': str(metadata.get('batch_url')),
        'submitting_user': str(metadata.get('user')),
        'output_path': str(metadata.get('output')),
        'request_body': metadata.get('meta', {}) or {},
        'hail_version': str(metadata.get('hailVersion')),
        'cwd': str(metadata.get('cwd')),
    }

    ar_api = AnalysisRunnerApi()

    try:
        analysis_id = ar_api.create_analysis_runner_log(**kwargs)
        logging.info(f'Created analysis with ID = {analysis_id}')
        return analysis_id
    except Exception:
        logging.exception('Failed to create analysis-runner log')
        raise
