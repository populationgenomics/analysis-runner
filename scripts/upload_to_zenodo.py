#!/usr/bin/env python3

"""
Upload files (usually zip archives) from GCS paths to the specified
(already created) Zenodo deposit.

Typical usage:

analysis-runner --dataset DATASET --description 'Upload to Zenodo' \
    --access-level standard --output-dir unused --env ZENODO_TOKEN=TOKEN \
    python3 scripts/upload_to_zenodo.py --deposit ID GCSFILE...

The script will need storage space for one zip archive at a time,
so storage should be set sufficient for the size of the largest archive.
"""

import os
import tempfile

import click
import requests
from google.cloud import storage

storage_client = storage.Client()


def download_from_gcs(local_path: str, gcs_path: str):
    (bucket_name, path) = gcs_path.removeprefix('gs://').split('/', maxsplit=1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)
    print(f'Downloading {gcs_path} into {local_path}')
    blob.download_to_filename(local_path)


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
    token: str,
    files: tuple[str],
):
    """
    Each zip archive listed in FILES is uploaded to the specified Zenodo deposit.
    The authentication token can also be specified via the ZENODO_TOKEN environment variable.
    """
    zenodo_host = 'sandbox.zenodo.org' if sandbox else 'zenodo.org'
    params = {'access_token': token}

    deposit_query = f'https://{zenodo_host}/api/deposit/depositions/{deposit}'
    response = requests.get(deposit_query, params=params, timeout=timeout)
    response.raise_for_status()
    deposit_bucket = response.json()['links']['bucket']

    tmpdir = os.environ.get('BATCH_TMPDIR') or tempfile.gettempdir()

    for file in files:
        basename = file.rsplit('/', maxsplit=1)[-1]
        tmp_filename = os.path.join(tmpdir, basename)

        download_from_gcs(tmp_filename, file)

        with open(tmp_filename, 'rb') as fp:
            print(f'Uploading {basename} to {zenodo_host}')
            upload_url = f'{deposit_bucket}/{basename}'
            response = requests.put(upload_url, data=fp, params=params, timeout=timeout)

        response.raise_for_status()
        print(f'Uploaded {response.json()["size"]} bytes')
        print()

        os.remove(tmp_filename)


if __name__ == '__main__':
    main()
