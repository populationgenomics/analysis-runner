#!/usr/bin/env python3

"""
wrapper script for the un-tar script
wrapped to modulate the batch job storage
"""

import logging
import os
import re
import subprocess
import sys

import click
from google.cloud import storage

from cpg_utils import to_path
from cpg_utils.config import get_config
from cpg_utils.git import prepare_git_job
from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    copy_common_env,
    get_batch,
)

CLIENT = storage.Client()
RMATCH_STR = r'gs://(?P<bucket>[\w-]+)/(?P<suffix>.+)/'
PATH_PATTERN = re.compile(RMATCH_STR)
GB = 1024 * 1024 * 1024  # dollars
UNZIP_SCRIPT = os.path.join(os.path.dirname(__file__), 'untar_gz_files.py')
COMMIT_HASH = subprocess.check_output(['git', 'describe', '--always']).strip().decode()

def get_path_components_from_path(path):
    """
    Returns the bucket_name and subdir for GS only paths
    Uses regex to match the bucket name and the subdirectory.
    """

    path_components = (PATH_PATTERN.match(path)).groups()

    bucket_name = path_components[0]
    subdir = path_components[1]

    return bucket_name, subdir

def get_tarballs_from_path(bucket_name: str, subdir: str) -> list[str]:
    """
    Checks a gs://bucket/subdir/ path for .tar and .tar.gz files
    Returns a list of:
        - .tar and .tar.gz blob paths found in the subdirectory
        - the size of image to use when unpacking the tarball
    """
    file_paths = [f.as_uri() for f in to_path(f'gs://{bucket_name}/{subdir}').iterdir()]
    if not file_paths:
        logging.info(f'No files found in {subdir} of {bucket_name}')
        return []
    
    tarfiles = [f for f in file_paths if f.endswith(('.tar', '.tar.gz', '.tar.bz2'))]
    logging.info(f'{len(tarfiles)} tar files found in gs://{bucket_name}/{subdir}')
    
    return tarfiles

@click.command()
@click.option(
    '--search-path', '-p', help='GCP bucket/directory to search', required=True
)
def main(search_path: str):
    """
    Who runs the world? main()

    Args:
        search_path (str): path to find tarballs in
    """
    bucket_name, subdir = get_path_components_from_path(search_path)
    blobs = get_tarballs_from_path(bucket_name, subdir)
    if len(blobs) == 0:
        logging.info('Nothing to do, quitting')
        sys.exit(0)

    # iterate over targets, set each one off in parallel
    for blobname in blobs:
        # create and config job
        job = get_batch().new_job(name=f'Unzip {blobname}')
        job.image(get_config()['workflow']['driver_image'])
        job.cpu(4)
        authenticate_cloud_credentials_in_job(job)
        copy_common_env(job)
        prepare_git_job(
            job,
            organisation='populationgenomics',
            repo_name='analysis-runner',
            commit=COMMIT_HASH,
        )
        job.cloudfuse(bucket_name, f'/{bucket_name}').command(
            f"""
            tar -xzf ~/{blobname.removeprefix('gs://')} -C ~/{bucket_name}/{subdir}/extracted/'
            """
        )

    get_batch().run(wait=False)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
