#!/usr/bin/env python3


"""
Given a single TAG.GZ, extract and upload
"""

import logging
import os
import pathlib
import subprocess
import sys
import click

# pylint: disable=E0401,E0611
from google.cloud import storage

client = storage.Client()


@click.command()
@click.option('--bucket', help='', required=True)
@click.option('--subdir', help='', required=True)
@click.option('--blob_name', help='', required=True)
@click.option('--outdir', help='', required=True)
@click.option('--extracted', help='', required=True)
def main(bucket: str, subdir: str, blob_name: str, outdir: str, extracted: str):
    """
    runs an extraction and upload for a single file

    Args:
        bucket (str): name of the bucket to connect to
        subdir (str): name of the path within the bucket
        blob_name (str): name of the tarball
        outdir (str): where to write to
        extracted (str): attached storage path to extract into

    Returns:

    """

    bucket_client = client.get_bucket(bucket)

    # Extract the tarball
    logging.info(f'Untaring {blob_name}')

    subprocess.run(
        ['tar', '-xzf', f'{blob_name}', '-C', extracted],
        check=True,
    )
    logging.info(f'Untared {blob_name}')

    # Recursively get all paths to everything extracted from tarball
    extracted_file_paths = [
        str(path) for path in pathlib.Path(extracted).rglob('*') if not path.is_dir()
    ]

    # Check if the tarball compressed a single directory, if yes then get files inside
    logging.info(
        f'Extracted {[os.path.basename(path) for path in extracted_file_paths]}'
    )

    # Iterate through extracted files, upload them to bucket, then delete them
    for filepath in extracted_file_paths:
        file = os.path.basename(filepath)
        output_blob = bucket_client.blob(os.path.join(subdir, outdir, file))

        output_blob.upload_from_filename(filepath)
        logging.info(f'Uploaded {file} to gs://{bucket}/{subdir}/{outdir}/')

        # Delete file after upload
        subprocess.run(['rm', f'{filepath}'], check=True)
        logging.info(f'Deleted {file} from disk')

    logging.info('All tarballs extracted and uploaded. Finishing...')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
