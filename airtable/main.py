"""A Cloud Function to store analysis-runner submission metadata in Airtable."""

import base64
import json
import os
from google.cloud import secretmanager
from airtable import Airtable

GCP_PROJECT = os.getenv('GCP_PROJECT')
AIRTABLE_SECRET = f'projects/{GCP_PROJECT}/secrets/airtable-config/versions/latest'

secret_manager = secretmanager.SecretManagerServiceClient()


def airtable(data, unused_context):
    """Main entry point for the Cloud Function."""
    secret = secret_manager.access_secret_version(request={'name': AIRTABLE_SECRET})
    config = json.loads(secret.payload.data.decode('UTF-8'))
    base = Airtable(config['baseKey'], config['tableName'], config['apiKey'])
    metadata = json.loads(base64.b64decode(data['data']).decode('utf-8'))
    base.insert(metadata)
