"""Web server which proxies requests to per-dataset "web" buckets."""

import json
import logging
import mimetypes
import os
from flask import Flask, abort, request, Response
import google.cloud.storage
from cpg_utils.cloud import email_from_id_token, read_secret

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

BUCKET_SUFFIX = os.getenv('BUCKET_SUFFIX')
assert BUCKET_SUFFIX

app = Flask(__name__)

storage_client = google.cloud.storage.Client()
logger = logging.getLogger('gunicorn.error')


@app.route('/<dataset>/<path:filename>')
def handler(dataset=None, filename=None):
    """Main entry point for serving."""
    if not dataset or not filename:
        logger.warning('Invalid request parameters')
        abort(400)

    id_token = request.headers.get('x-goog-iap-jwt-assertion')
    if not id_token:
        logger.warning('Missing x-goog-iap-jwt-assertion header')
        abort(403)

    try:
        email = email_from_id_token(id_token)
    except ValueError:
        logger.warning('Failed to extract email from ID token')
        abort(403)

    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        logger.warning(f'Invalid dataset "{dataset}"')
        abort(400)

    dataset_project_id = dataset_config['projectId']
    members = read_secret(
        dataset_project_id, f'{dataset}-web-access-members-cache'
    ).split(',')
    if email not in members:
        logger.warning(f'{email} is not a member of the {dataset} access group')
        abort(403)

    bucket_name = f'cpg-{dataset}-{BUCKET_SUFFIX}'
    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        abort(404)

    response = Response(blob.download_as_string())
    response.headers['Content-Type'] = (
        mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    )
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
