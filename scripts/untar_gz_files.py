#!/usr/bin/env python3


"""
Given a gcp directory, extract all .tar.gz files in the directory
into output_dir defined in the call to analysis runner
"""

import logging
import os
import re
import subprocess
import sys
import click

# pylint: disable=E0401,E0611
from cpg_utils.config import get_config
from google.cloud import storage

RMATCH_STR = r'gs://(?P<bucket>[\w-]+)/(?P<suffix>.+)/'
path_pattern = re.compile(RMATCH_STR)

client = storage.Client()


def get_path_components_from_path(path):
    """
    Returns the bucket_name and subdir for GS only paths
    Uses regex to match the bucket name and the subdirectory.
    """

    path_components = (path_pattern.match(path)).groups()

    bucket_name = path_components[0]
    subdir = path_components[1]

    return bucket_name, subdir


def get_tarballs_from_path(bucket_name: str, subdir: str):
    """
    Checks a gs://bucket/subdir/ path for .tar.gz files
    Returns a list of .tar.gz blob paths found in the subdirectory
    """

    blob_names = []
    for blob in client.list_blobs(bucket_name, prefix=(subdir + '/'), delimiter='/'):
        if not blob.name.endswith('.tar.gz'):
            continue
        blob_names.append(blob.name)

    logging.info(f'{len(blob_names)} .tar.gz files found in {subdir} of {bucket_name}')

    return blob_names


def untar_gz_files(
    bucket_name: str,
    subdir: str,
    blob_names: list[str],
    destination: str,
):
    """
    Downloads the .tar.gz files in blob_names to the disk
    Extracts the files from the tarball into a folder called extracted
    Uploads and then deletes each extracted file, one by one.
    Deletes the original downloaded tarball after all its files are uploaded.
    
    The uploaded files end up in a directory denoted by the destination
    appended to the original gs:// search path.
    """
    input_bucket = client.get_bucket(bucket_name)

    for blob_name in blob_names:
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

        extracted_from_tarball = os.listdir(f'./{subdir}/extracted')
                
        # Check if the tarball compressed a single directory, if yes then get files inside
        if os.path.isdir(f'./{subdir}/extracted/{extracted_from_tarball[0]}'):
            is_directory = True
            folder = extracted_from_tarball[0]
            extracted_files = os.listdir(f'./{subdir}/extracted/{folder}')
        else:
            is_directory = False
            extracted_files = os.listdir(f'./{subdir}/extracted')
        logging.info(f'Extracted {extracted_files}')

        # Iterate through extracted files, upload them to bucket, then delete them
        for file in extracted_files:
            output_blob = input_bucket.blob(os.path.join(subdir, destination, file))

            if is_directory:
                filepath = f'./{subdir}/extracted/{folder}/{file}'
            else:
                filepath = f'./{subdir}/extracted/{file}'
            
            output_blob.upload_from_filename(filepath)
            logging.info(
                f'Uploaded {file} to gs://{bucket_name}/{blob_name}/{destination}/'
            )

            # Delete file after upload
            subprocess.run(['rm', f'{filepath}'], check=True)
            logging.info(f'Deleted {file} from disk')

        # Delete tarball after all extracted files uploaded & deleted    
        subprocess.run(['rm', f'{blob_name}'], check=True)
        logging.info(f'Deleted tarball {blob_name}')

    logging.info('All tarballs extracted and uploaded. Finishing...')

    # with tarfile.open(fileobj=io.BytesIO(input_blob)) as tar:
    #     logging.info(f'Untaring {blob_name}')
    #     for member in tar.getnames():
    #         tar.extract(member, path=f'./{blob_name}')
    #         output_blob = input_bucket.blob(
    #             os.path.join(subdir, destination, member)
    #         )
    #         output_blob.upload_from_filename(f'./{blob_name}/{member}')
    #         logging.info(
    #             f'{member} extracted to gs://{bucket_name}/{subdir}/{destination}/{member}'
    #         )


@click.command()
@click.option('--search-path', '-p', help='GCP bucket/directory to search', default='')
def main(search_path: str):
    """
    Parameters
    ----------
    search_path :   The GCP directory containing the .tar.gz files
    """
    config = get_config()
    output_dir = config['workflow']['output_prefix']

    bucket_name, subdir = get_path_components_from_path(search_path)

    blobs = get_tarballs_from_path(bucket_name, subdir)

    untar_gz_files(bucket_name, subdir, blobs, output_dir)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
