#!/usr/bin/env python3

"""
Upload files (usually zip archives) from GCS paths to the specified
(already created) Zenodo deposit.

Typical usage:

analysis-runner --dataset DATASET --description 'Upload to Zenodo' \
    --access-level standard --output-dir unused --storage SIZE \
    --env ZENODO_TOKEN=TOKEN \
    python3 scripts/upload_to_zenodo.py --deposit ID GCSFILE...

Each GCSFILE may optionally be suffixed with #NEWNAME to give the file
a different name when it is attached to the Zenodo deposit.

The script will need storage space for one zip archive at a time,
so storage should be set sufficient for the size of the largest archive.
"""

import hashlib
import os
import tempfile
import time

import click
import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter, Retry

storage_client = storage.Client()

# Zenodo intermittently drops long-running uploads (SSLEOFError / connection reset),
# so each upload is retried from the start and verified against a local MD5.
RETRYABLE_STATUSES = (429, 500, 502, 503, 504)
RETRYABLE_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
MD5_CHUNK = 8 * 1024 * 1024


def make_session() -> requests.Session:
    """Session with automatic retries for the small metadata calls."""
    retry = Retry(
        total=5,
        connect=5,
        read=3,
        status=5,
        backoff_factor=2,
        status_forcelist=RETRYABLE_STATUSES,
        allowed_methods=frozenset({'GET'}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


def md5_of(path: str) -> str:
    """Hex MD5 of a file, read in chunks."""
    digest = hashlib.md5(usedforsecurity=False)
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(MD5_CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def download_from_gcs(local_path: str, gcs_path: str):
    (bucket_name, path) = gcs_path.removeprefix('gs://').split('/', maxsplit=1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)
    print(f'Downloading {gcs_path} into {local_path}', flush=True)
    blob.download_to_filename(local_path)


def attempt_upload(
    url: str,
    local_path: str,
    params: dict,
    timeout: float,
    local_md5: str,
) -> tuple[int | None, str | None]:
    """
    One upload attempt. Returns (size, None) on verified success, or
    (None, problem) when the attempt should be retried.

    Raises on non-retryable HTTP errors (4xx), which won't improve on retry.
    Uses a bare requests.put, not the retrying session, so attempts aren't
    multiplied and the file is always re-sent from the start.
    """
    try:
        with open(local_path, 'rb') as fp:
            response = requests.put(url, data=fp, params=params, timeout=timeout)
    except RETRYABLE_ERRORS as exc:
        return None, f'connection error: {exc}'

    if response.status_code in RETRYABLE_STATUSES:
        return None, f'HTTP {response.status_code}: {response.text[:200]}'

    response.raise_for_status()
    body = response.json()
    remote_md5 = body.get('checksum', '').removeprefix('md5:')
    if remote_md5 != local_md5:
        return None, (
            f'checksum mismatch (local {local_md5}, '
            f'Zenodo {remote_md5 or "absent"})'
        )
    return body['size'], None


def upload_with_retries(
    url: str,
    local_path: str,
    params: dict,
    timeout: float,
    attempts: int,
) -> int:
    """
    Upload a file to a Zenodo bucket URL, retrying on dropped connections.

    The bucket API overwrites by filename, so retrying a partial upload is
    safe. Returns the size Zenodo reports once the MD5 matches the local file.
    """
    print(f'Computing MD5 of {local_path}', flush=True)
    local_md5 = md5_of(local_path)

    for attempt in range(1, attempts + 1):
        print(f'Upload attempt {attempt}/{attempts}', flush=True)
        size, problem = attempt_upload(url, local_path, params, timeout, local_md5)
        if problem is None:
            print(f'Checksum verified ({local_md5})', flush=True)
            return size
        if attempt == attempts:
            raise RuntimeError(
                f'Upload failed after {attempts} attempts; last: {problem}'
            )
        delay = 5 * 2**attempt
        print(f'Attempt {attempt} failed ({problem}); retrying in {delay}s', flush=True)
        time.sleep(delay)

    raise AssertionError('unreachable')  # pragma: no cover


@click.command(no_args_is_help=True)
@click.option(
    '--deposit',
    required=True,
    help='Deposit ID to which files will be uploaded',
)
@click.option(
    '--sandbox',
    is_flag=True,
    help='Upload to sandbox instead of real Zenodo',
)
@click.option(
    '--timeout',
    default=600.0,
    help='Request timeout (in seconds)',
)
@click.option(
    '--attempts',
    default=5,
    help='Maximum upload attempts per file',
)
@click.option(
    '--token',
    envvar='ZENODO_TOKEN',
    help='Authentication token for Zenodo',
)
@click.argument(
    'files',
    nargs=-1,
)
def main(
    deposit: str,
    sandbox: bool,
    timeout: float,
    attempts: int,
    token: str,
    files: tuple[str],
):
    """
    Each zip archive listed in FILES is uploaded to the specified Zenodo deposit.
    The authentication token can also be specified via the ZENODO_TOKEN environment variable.
    """
    zenodo_host = 'sandbox.zenodo.org' if sandbox else 'zenodo.org'
    params = {'access_token': token}
    session = make_session()

    deposit_query = f'https://{zenodo_host}/api/deposit/depositions/{deposit}'
    response = session.get(deposit_query, params=params, timeout=timeout)
    response.raise_for_status()
    deposit_bucket = response.json()['links']['bucket']

    tmpdir = os.environ.get('BATCH_TMPDIR') or tempfile.gettempdir()

    for file_newname in files:
        file, _, newname = file_newname.partition('#')
        basename = file.rsplit('/', maxsplit=1)[-1]
        tmp_filename = os.path.join(tmpdir, basename)

        download_from_gcs(tmp_filename, file)

        if newname:
            print(f'Uploading {basename} to {zenodo_host} as {newname}', flush=True)
            upload_url = f'{deposit_bucket}/{newname}'
        else:
            print(f'Uploading {basename} to {zenodo_host}', flush=True)
            upload_url = f'{deposit_bucket}/{basename}'

        size = upload_with_retries(
            upload_url, tmp_filename, params, timeout, attempts
        )
        print(f'Uploaded {size} bytes', flush=True)
        print(flush=True)

        os.remove(tmp_filename)


if __name__ == '__main__':
    main()
