"""Web server which proxies requests to per-dataset "web" buckets."""

import logging
import mimetypes
import os
from flask import Flask, abort, request, Response
import google.cloud.storage
import google.cloud.secretmanager
import google.api_core.exceptions
from cpg_utils.cloud import email_from_id_token

BUCKET_SUFFIX = os.getenv('BUCKET_SUFFIX')
assert BUCKET_SUFFIX

PROJECT_ID = 'analysis-runner'

app = Flask(__name__)

storage_client = google.cloud.storage.Client()
secret_manager = google.secretmanager.SecretManagerServiceClient()
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

    try:
        secret_name = f'{dataset}-access-members-cache'
        secret_path = secret_manager.secret_path(PROJECT_ID, secret_name)
        response = secret_manager.access_secret_version(
            request={'name': f'{secret_path}/versions/latest'}
        )

        members = response.payload.data.decode('UTF-8').split(',')
        if email not in members:
            logger.warning(f'{email} is not a member of the {dataset} access group')
            abort(403)
    except google.api_core.exceptions.ClientError as e:
        logger.warning(f'Error reading access group cache secret for "{dataset}": {e}')
        abort(400)

    bucket_name = f'cpg-{dataset}-{BUCKET_SUFFIX}'
    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        abort(404)

    response = Response(blob.download_as_bytes())
    response.headers['Content-Type'] = (
        mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    )
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
