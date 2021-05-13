"""Web server which proxies requests to per-dataset "web" buckets."""

import logging
import os
from flask import Flask, abort, request
import google.cloud.storage
from cpg_utils.cloud import is_google_group_member, email_from_id_token

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

    group_name = f'{dataset}-access@populationgenomics.org.au'
    if not is_google_group_member(email, group_name):
        logger.warning(f'{email} is not a member of {group_name}')
        abort(403)

    bucket_name = f'cpg-{dataset}-web'
    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        abort(404)

    return blob.download_as_text()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
