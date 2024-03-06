# pylint: disable=too-many-return-statements
"""Web server which proxies requests to per-dataset "web" buckets."""

import json
import logging
import mimetypes
import os
from typing import Optional

import google.auth.transport.requests
import google.cloud.storage
import google.oauth2.id_token
from flask import Flask, Response, abort, request, stream_with_context

from cpg_utils.cloud import is_member_in_cached_group, read_secret

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

BUCKET_SUFFIX = os.getenv('BUCKET_SUFFIX')
assert BUCKET_SUFFIX

# See https://cloud.google.com/iap/docs/signed-headers-howto
IAP_EXPECTED_AUDIENCE = os.getenv('IAP_EXPECTED_AUDIENCE')
assert IAP_EXPECTED_AUDIENCE

MEMBERS_CACHE_LOCATION = os.getenv('MEMBERS_CACHE_LOCATION')
assert MEMBERS_CACHE_LOCATION

app = Flask(__name__)

storage_client = google.cloud.storage.Client()
logger = logging.getLogger('gunicorn.error')


@app.route('/<dataset>/<path:filename>')
def handler(  # noqa: C901
    dataset: Optional[str] = None,
    filename: Optional[str] = None,
):
    """Main entry point for serving."""
    if not dataset or not filename:
        logger.warning('Invalid request parameters')
        return abort(400, 'Either the dataset or filename was not present')

    iap_jwt = request.headers.get('x-goog-iap-jwt-assertion')
    if not iap_jwt:
        logger.warning('Missing x-goog-iap-jwt-assertion header')
        return abort(403)

    try:
        decoded_jwt = google.oauth2.id_token.verify_token(
            iap_jwt,
            google.auth.transport.requests.Request(),
            audience=IAP_EXPECTED_AUDIENCE,
            certs_url='https://www.gstatic.com/iap/verify/public_key',
        )
        # Use allAuthenticatedUsers for the IAP configuration to make this
        # work for arbitrary users.
        email = decoded_jwt['email'].lower()
    except Exception:  # pylint: disable=broad-except
        logger.exception('Failed to extract email from ID token')
        return abort(403)

    # Don't allow reading `.access` files.
    if os.path.basename(filename) == '.access':
        return abort(403, 'Unable to read .access files')

    server_config_raw = read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    if not server_config_raw:
        logger.exception('Failed to read server-config secret')
        return abort(500, 'Failed to read server-config secret')
    server_config = json.loads(server_config_raw)
    if dataset not in server_config:
        logger.warning(f'Invalid dataset "{dataset}"')
        return abort(403, 'Invalid dataset')

    bucket_name = f'cpg-{dataset}-{BUCKET_SUFFIX}'
    bucket = storage_client.bucket(bucket_name)

    in_web_access_group = False
    try:
        in_web_access_group = is_member_in_cached_group(
            f'{dataset}-web-access',
            email,
            members_cache_location=MEMBERS_CACHE_LOCATION,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f'Failed to access group membership cache: {e}')

    if not in_web_access_group:
        # Second chance: if there's a '.access' file in the first subdirectory,
        # check if the email is listed there.
        split_subdir = filename.split('/', maxsplit=1)
        if len(split_subdir) == 2 and split_subdir[0]:  # noqa: PLR2004
            access_list_filename = f'{split_subdir[0]}/.access'
            blob = bucket.get_blob(access_list_filename)
            if blob is None:
                logger.warning(f'{email} is not a member of the {dataset} access group')
                return abort(403)
            access_list = blob.download_as_text().lower().splitlines()
            if email not in access_list:
                logger.warning(
                    f'{email} is not in {dataset} access group or {access_list_filename}',
                )
                return abort(403)

    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
    blob = bucket.get_blob(filename)
    if blob is None:
        return abort(404, 'File was not found')

    # Stream the response to allow large files to be served.
    response = Response(stream_with_context(blob.open('rt')))
    response.headers['Content-Type'] = (
        mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    )
    return response


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=int(os.environ.get('PORT', 8080)))
