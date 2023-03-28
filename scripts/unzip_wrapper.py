#!/usr/bin/env python3

"""
wrapper script for the un-tar script
wrapped to modulate the batch job storage
"""

import logging
import os
import re
import sys

import click
import hailtop.batch.job
from google.cloud import storage

from cpg_workflows.batch import get_batch
from cpg_utils.config import get_config
from cpg_utils.git import (
    prepare_git_job,
    get_git_commit_ref_of_current_repository,
    get_organisation_name_from_current_directory,
    get_repo_name_from_current_directory,
)
from cpg_utils.hail_batch import authenticate_cloud_credentials_in_job, copy_common_env


CLIENT = storage.Client()
GB = 1024 * 1024 * 1024
PATH_PATTERN = re.compile(r'gs://(?P<bucket>[\w-]+)/(?P<suffix>.+)/')
UNZIP_SCRIPT = os.path.join(os.path.dirname(__file__), 'untar_gz_files.py')


def get_path_components_from_path(path):
    """
    Returns the bucket_name and subdir for GS only paths
    Uses regex to match the bucket name and the subdirectory.
    """

    path_components = (PATH_PATTERN.match(path)).groups()

    bucket_name = path_components[0]
    subdir = path_components[1]

    return bucket_name, subdir


def get_tarballs_from_path(bucket_name: str, subdir: str) -> list[tuple[str, int]]:
    """
    Checks a gs://bucket/subdir/ path for .tar.gz files
    Returns found object and sizes

    Args:
        bucket_name (str): name of the bucket to search
        subdir (str): subdirectory path in the bucket

    Returns:
        a list of tuples, each tuple contains
        (full path to tar.gz file, size of storage to use unpacking)
    """

    blob_details = []
    for blob in CLIENT.list_blobs(bucket_name, prefix=(subdir + '/'), delimiter='/'):
        if not blob.name.endswith('.tar.gz'):
            continue

        # image size is double and a half the tar size in GB, or 30GB
        # whichever is larger
        job_gb = max([30, int((blob.size // GB) * 2.5)])
        full_blob_name = f'gs://{bucket_name}/{blob.name}'

        blob_details.append((full_blob_name, job_gb))

    logging.info(
        f'{len(blob_details)} .tar.gz files found in {subdir} of {bucket_name}'
    )

    return blob_details


def set_up_batch_job(blobname: str, blobsize: int) -> hailtop.batch.job.Job:
    """

    Args:
        blobname ():
        blobsize (int):

    Returns:

    """
    job = get_batch().new_job(name=f'decompress {blobname}')
    job.cpu(4)
    job.image(get_config()['workflow']['driver_image'])
    job.storage(f'{blobsize}Gi')
    authenticate_cloud_credentials_in_job(job)
    copy_common_env(job)
    prepare_git_job(
        job=job,
        organisation=get_organisation_name_from_current_directory(),
        repo_name=get_repo_name_from_current_directory(),
        commit=get_git_commit_ref_of_current_repository(),
    )
    return job


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

    config = get_config()
    output_dir = config['workflow']['output_prefix']

    bucket_name, subdir = get_path_components_from_path(search_path)

    blobs = get_tarballs_from_path(bucket_name, subdir)

    if len(blobs) == 0:
        logging.info('Nothing to do, quitting')
        sys.exit(0)

    # iterate over targets, set each one off in parallel
    for blobname, blobsize in blobs:
        # create and config job
        job = set_up_batch_job(blobname, blobsize)

        # read this blob into the batch
        batch_blob = get_batch().read_input(path=blobname)

        # set the command
        job.command(
            f'python3 {UNZIP_SCRIPT} '
            f'--bucket {bucket_name} '
            f'--subdir {subdir} '
            f'--blob_name {batch_blob} '
            f'--extracted {job.extracted} '
            f'--outdir {output_dir} '
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
