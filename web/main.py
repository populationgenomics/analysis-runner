"""Web server which proxies requests to per-dataset "web" buckets."""

import json
import logging
import mimetypes
import os
from flask import Flask, abort, request, Response
from cpg_utils.cloud import read_secret
import google.cloud.storage
import google.auth.transport.requests
import google.oauth2.id_token

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

BUCKET_SUFFIX = os.getenv('BUCKET_SUFFIX')
assert BUCKET_SUFFIX

# See https://cloud.google.com/iap/docs/signed-headers-howto
IAP_EXPECTED_AUDIENCE = os.getenv('IAP_EXPECTED_AUDIENCE')
assert IAP_EXPECTED_AUDIENCE

app = Flask(__name__)

storage_client = google.cloud.storage.Client()
logger = logging.getLogger('gunicorn.error')


@app.route('/<dataset>/<path:filename>')
def handler(dataset=None, filename=None):
    """Main entry point for serving."""
    if not dataset or not filename:
        logger.warning('Invalid request parameters')
        abort(400)

    iap_jwt = request.headers.get('x-goog-iap-jwt-assertion')
    if not iap_jwt:
        logger.warning('Missing x-goog-iap-jwt-assertion header')
        abort(403)

    try:
        decoded_jwt = google.oauth2.id_token.verify_token(
            iap_jwt,
            google.auth.transport.requests.Request(),
            audience=IAP_EXPECTED_AUDIENCE,
            certs_url='https://www.gstatic.com/iap/verify/public_key',
        )
        logging.warning(f'*** {decoded_jwt=}')
        email = decoded_jwt['email']
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Failed to extract email from ID token: {e}')
        abort(403)

    # Don't allow reading `.access` files.
    if os.path.basename(filename) == '.access':
        abort(403)

    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    dataset_config = server_config.get(dataset)
    if not dataset_config:
        logger.warning(f'Invalid dataset "{dataset}"')
        abort(400)

    bucket_name = f'cpg-{dataset}-{BUCKET_SUFFIX}'
    bucket = storage_client.bucket(bucket_name)

    dataset_project_id = dataset_config['projectId']
    members = read_secret(
        dataset_project_id, f'{dataset}-web-access-members-cache'
    ).split(',')

    if email not in members:
        # Second chance: if there's a '.access' file in the file's directory,
        # check if the email is listed there.
        access_filename = os.path.join(os.path.dirname(filename), '.access')
        blob = bucket.get_blob(access_filename)
        if blob is None:
            logger.warning(f'{email} is not a member of the {dataset} access group')
            abort(403)
        access_list = blob.download_as_string().splitlines()
        if email not in access_list:
            logger.warning(
                f'{email} is not in {dataset} access group or {access_filename}'
            )
            abort(403)

    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
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
