#!/usr/bin/env python3


"""
Given a gcp directory, unzips and untars all .tar.gz files in the directory
"""

import logging
import sys
import click
import re
import io
import os
import tarfile

# pylint: disable=E0401,E0611
from cpg_utils.config import get_config
from google.cloud import storage

rmatch_str = r'gs://(?P<bucket>[\w-]+)/(?P<suffix>.+)/'
path_pattern = re.compile(rmatch_str)

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
    Returns a list of .tar.gz file paths in the directory path
    """

    tarballs_in_bucket_subdir = []
    blob_names = []
    for blob in client.list_blobs(bucket_name, prefix=(subdir + '/'), delimiter='/'):
        if not blob.name.endswith('.tar.gz'):
            continue
        tarballs_in_bucket_subdir.append(f'gs://{bucket_name}/{blob.name}')
        blob_names.append(blob.name)

    logging.info(
        f'{len(tarballs_in_bucket_subdir)} .tar.gz files found in {subdir} of {bucket_name}'
    )

    return tarballs_in_bucket_subdir, blob_names


def untar_gz_files(
    bucket_name: str,
    subdir: str,
    paths: list[str],
    blob_names: list[str],
    destination: str,
):
    """
    Opens and extracts .tar.gz files provided as a list of gs:// paths
    Dumps the extracted data into a destination directory appended to the
    original gs:// search path.
    """
    input_bucket = client.get_bucket(bucket_name)

    for idx, path in enumerate(paths):
        input_blob = input_bucket.get_blob(blob_names[idx]).download_as_string()
        tar = tarfile.open(fileobj=io.BytesIO(input_blob))
        logging.info(f'Untaring {path}')

        for member in tar.getnames():
            file_object = tar.extractfile(member)
            output_blob = input_bucket.blob(os.path.join(subdir, destination, member))
            output_blob.upload_from_file(file_object)
            logging.info(f'{member} extracted to gs://{bucket_name}/{subdir}/')


@click.command()
@click.option('--search-path', '-p', help='GCP bucket/directory to search', default='')
def main(search_path: str):
    """
    Parameters
    ----------
    search_path :   The GCP directory containing the .tar.gz files
    """
    config = get_config()
    output_dir = config['workflow']['output_dir']

    bucket_name, subdir = get_path_components_from_path(search_path)

    tarballs, blobs = get_tarballs_from_path(bucket_name, subdir)

    untar_gz_files(bucket_name, subdir, tarballs, blobs, output_dir)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
