"""Web server which proxies requests to per-dataset "web" buckets."""

import io
import json
import logging
import mimetypes
import os
from typing import Generator, Optional

import google.auth.transport.requests
import google.cloud.storage
import google.oauth2.id_token
from cachetools import TTLCache, cached
from flask import Flask, abort, request

from cpg_utils.cloud import read_secret
from cpg_utils.membership import is_member_in_cached_group

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


# These calls for server config and permissions are quite expensive
# so cache them for a little while to avoid long load times for pages
# with lots of subsequent requests
@cached(cache=TTLCache(maxsize=64, ttl=60))
def get_server_config(project_id: str):
    server_config_raw = read_secret(project_id, 'server-config')
    if not server_config_raw:
        raise Exception('failed to read server-config secret')
    return json.loads(server_config_raw)


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def has_permission(email: str, dataset: str, access_file: str | None, bucket_name: str):
    bucket = storage_client.bucket(bucket_name)

    if is_member_in_cached_group(
        f'{dataset}-web-access',
        email,
        members_cache_location=MEMBERS_CACHE_LOCATION,
    ):
        return True

    # Second chance: if there's a '.access' file in the first subdirectory,
    # check if the email is listed there.
    if access_file:
        blob = bucket.get_blob(access_file)
        if blob is None:
            return False
        access_list = blob.download_as_text().lower().splitlines()
        if email in access_list:
            return True

        logger.warning(
            f'{email} is not in {dataset} access group or {access_file}',
        )
        return False
    return False


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
    except Exception:
        logger.exception('Failed to extract email from ID token')
        return abort(403)

    # Don't allow reading `.access` files.
    if os.path.basename(filename) == '.access':
        return abort(403, 'Unable to read .access files')

    try:
        server_config = get_server_config(ANALYSIS_RUNNER_PROJECT_ID)
    except Exception as e:
        logger.exception(f'Failed to read server-config secret: {e}')
        return abort(500, 'Failed to read server-config secret')

    if dataset not in server_config:
        logger.warning(f'Invalid dataset "{dataset}"')
        return abort(403, 'Invalid dataset')

    bucket_name = f'cpg-{dataset}-{BUCKET_SUFFIX}'
    bucket = storage_client.bucket(bucket_name)

    access_file: str | None = None
    split_subdir = filename.split('/', maxsplit=1)
    if len(split_subdir) == 2 and split_subdir[0]:  # noqa: PLR2004
        access_file = f'{split_subdir[0]}/.access'

    if not has_permission(email, dataset, access_file, bucket_name):
        logger.warning(
            f'{email} is not in {dataset} access group or access list',
        )
        return abort(403)

    logger.info(f'Fetching blob gs://{bucket_name}/{filename}')
    blob = bucket.get_blob(filename)
    if blob is None:
        return abort(404, 'File was not found')

    content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'

    file_obj = io.BytesIO()
    blob.download_to_file(file_obj)
    file_obj.seek(0)

    def iterfile() -> Generator[bytes, None, None]:
        yield from file_obj

    return iterfile(), {'Content-Type': content_type}


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=int(os.environ.get('PORT', 8080)))
