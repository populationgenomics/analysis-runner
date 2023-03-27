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
def main(bucket: str, subdir: str, blob_name: str, outdir: str):
    """
    runs an extraction and upload for a single file

    TODO - could make this even easier by having the wrapper
    TODO - copy the single tarball into the job image
    TODO - leaving extraction and upload as the only tasks

    :param bucket: str, name of the bucket to connect to
    :param subdir: str, name of the path within the bucket
    :param blob_name: str, name of the tarball
    :param outdir: str, where to write to
    """

    input_bucket = client.get_bucket(bucket)

    # Make the extraction directory on the disk
    if not os.path.exists(f'./{subdir}'):
        os.makedirs(f'./{subdir}')
        os.makedirs(f'./{subdir}/extracted')

    # Download and extract the tarball
    input_bucket.get_blob(blob_name).download_to_filename(blob_name)
    logging.info(f'Untaring {blob_name}')
    subprocess.run(
        ['tar', '-xzf', f'{blob_name}', '-C', f'./{subdir}/extracted'],
        check=True,
    )
    logging.info(f'Untared {blob_name}')

    # Recursively get all paths to everything extracted from tarball
    extracted_from_tarball = pathlib.Path(f'./{subdir}/extracted')
    all_extracted_paths = [
        str(path) for path in list(extracted_from_tarball.rglob('*'))
    ]

    # Discard paths that are directories, keep paths to files
    extracted_file_paths = []
    for extracted_path in all_extracted_paths:
        if not os.path.isdir(extracted_path):
            extracted_file_paths.append(extracted_path)

    # Check if the tarball compressed a single directory, if yes then get files inside
    logging.info(
        f'Extracted {[os.path.basename(path) for path in extracted_file_paths]}'
    )

    # Iterate through extracted files, upload them to bucket, then delete them
    for filepath in extracted_file_paths:
        file = os.path.basename(filepath)
        output_blob = input_bucket.blob(os.path.join(subdir, outdir, file))

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
