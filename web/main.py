"""Web server which proxies requests to per-dataset "web" buckets."""

import os
from flask import Flask, abort, request
from google.cloud import storage
from cpg_utils.cloud import is_google_group_member, email_from_id_token

app = Flask(__name__)

storage_client = storage.Client()


@app.route('/<dataset>/<path:filename>')
def handler(dataset=None, filename=None):
    """Main entry point for serving."""
    if not dataset or not filename:
        abort(400)

    auth_header = request.headers.get('Authorization')
    if not auth_header:
        abort(403)

    try:
        email = email_from_id_token(auth_header)
    except ValueError:
        abort(403)

    group_name = f'{dataset}-access@populationgenomics.org.au'
    if not is_google_group_member(email, group_name):
        abort(403)

    bucket_name = f'cpg-{dataset}-web'
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        abort(404)

    return blob.download_as_text()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
