#!/usr/bin/env python3

"""
Create zip archives of the files in the specified GCS tree and
upload them to the specified (already created) Zenodo deposit.

Typical usage:

analysis-runner --dataset DATASET --description 'Upload to Zenodo' \
    --access-level standard --output-dir unused \
    --env ZENODO_TOKEN=TOKEN --storage 10G \
    python3 scripts/zip_to_zenodo.py \
        --basedir gs://BUCKET/PATH --deposit ID \
        ABC DEF GHI

The script will need storage space for one zip archive at a time,
so storage should be set to the expected size of the largest archive.
"""

import os
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import click
import requests
from google.cloud import storage

storage_client = storage.Client()


def zip_tree(
    zip_fname: str,
    bucket: str,
    prefix: str,
    subset: str,
    maxfiles: int | None,
):
    """
    Create a zip archive containing the files within the directory subtree.
    """
    print(f'Creating {zip_fname} from gs://{bucket}/{prefix}/{subset}/**')
    with ZipFile(zip_fname, mode='w') as zipf:
        nfiles = 0
        for blob in storage_client.list_blobs(bucket, prefix=f'{prefix}/{subset}'):
            subname = blob.name.removeprefix(prefix).removeprefix('/')
            print(f'Adding {subname} to archive')

            contents = blob.download_as_bytes()
            info = ZipInfo(filename=subname, date_time=blob.updated.utctimetuple())
            zipf.writestr(info, contents, compress_type=ZIP_DEFLATED, compresslevel=9)

            nfiles += 1
            if maxfiles is not None and nfiles >= maxfiles:
                break

    print(f'Added {nfiles} files to {zip_fname}')


@click.command(no_args_is_help=True)
@click.option(
    '--basedir',
    required=True,
    help='Base of the GCS directory tree to upload',
)
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
    '--limit',
    default=-1,
    help='Maximum number of files to add [for testing]',
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
    'subsets',
    nargs=-1,
)
def main(
    basedir: str,
    deposit: str,
    sandbox: bool,
    limit: int,
    timeout: float,
    token: str,
    subsets: tuple[str],
):
    """
    Each SUBSET specifies a subdirectory of BASEDIR whose contents will be individually zipped.
    These zip archives are then uploaded to the specified Zenodo deposit.
    The authentication token can also be specified via the ZENODO_TOKEN environment variable.
    """
    zenodo_host = 'sandbox.zenodo.org' if sandbox else 'zenodo.org'
    params = {'access_token': token}

    deposit_query = f'https://{zenodo_host}/api/deposit/depositions/{deposit}'
    response = requests.get(deposit_query, params=params, timeout=timeout)
    response.raise_for_status()
    deposit_bucket = response.json()['links']['bucket']

    (bucket, prefix) = basedir.removeprefix('gs://').split('/', maxsplit=1)
    for subset in subsets:
        zip_fname = f'{subset}.zip'
        zip_tree(zip_fname, bucket, prefix, subset, limit if limit > 0 else None)

        with open(zip_fname, 'rb') as fp:
            print(f'Uploading {zip_fname} to {zenodo_host}')
            upload_url = f'{deposit_bucket}/{zip_fname}'
            response = requests.put(upload_url, data=fp, params=params, timeout=timeout)

        response.raise_for_status()
        print(f'Uploaded {response.json()["size"]} bytes')
        print()

        os.remove(zip_fname)


if __name__ == '__main__':
    main()
