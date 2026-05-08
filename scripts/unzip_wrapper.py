#!/usr/bin/env python3
# ruff: noqa: S607
"""
wrapper script for the un-tar script
wrapped to modulate the batch job storage
"""

import logging
import os
from pathlib import Path
import re
import subprocess
import sys

import click
from google.cloud import storage

from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    copy_common_env,
    get_batch,
    prepare_git_job,
)
from cpg_utils import Path, to_path

CLIENT = storage.Client()
RMATCH_STR = r'gs://(?P<bucket>[\w-]+)/(?P<suffix>.+)/'
PATH_PATTERN = re.compile(RMATCH_STR)
GB = 1024 * 1024 * 1024  # dollars
UNZIP_SCRIPT = os.path.join(os.path.dirname(__file__), 'untar_gz_files.py')


def get_commit_hash():
    return (
        subprocess.check_output(
            ['git', 'describe', '--always'],  # noqa: S603
        )
        .strip()
        .decode()
    )


def get_path_components_from_path(path: str):
    """
    Returns the bucket_name and subdir for GS only paths
    Uses regex to match the bucket name and the subdirectory.
    """

    match = PATH_PATTERN.match(path)
    if not match:
        raise ValueError(f'Cannot find bucket, path for gs:// path: {path}')
    path_components = match.groups()

    bucket_name = path_components[0]
    subdir = path_components[1]

    return bucket_name, subdir


def get_tarballs_from_path(
    bucket_name: str,
    subdir: str,
    single_path: bool,
) -> list[tuple[str, int]]:
    """
    Checks a gs://bucket/subdir/ path for .tar and .tar.gz files
    If single_path == True, only returns the first tarball found in the path, otherwise returns all tarballs found
    Returns a list of:
        - .tar and .tar.gz blob paths found in the subdirectory
        - the size of image to use when unpacking the tarball
    """
    blob_details = []

    if not single_path:
        for blob in CLIENT.list_blobs(
            bucket_name,
            prefix=(subdir + '/'),
            delimiter='/',
        ):
            if not blob.name.endswith(('.tar', '.tar.gz', '.tar.bz2')):
                continue

            # image size is double and a half the tar size in GB, or 30GB
            # whichever is larger
            job_gb = max([30, int((blob.size // GB) * 2.5)])

            blob_details.append((blob.name, job_gb))


    logging.info(f'{len(blob_details)} tar files found in {subdir} of {bucket_name}')

    return blob_details


@click.command()
@click.option(
    '--search-path',
    '-p',
    help='GCP bucket/directory to search',
    required=False,
)
@click.option(
    '--single-path',
    '-s',
    type=str,
    required=False,
    help='Provide a single path to a tarball, rather than a directory.',
)
def main(search_path: str, single_path: str):
    """
    Who runs the world? main()

    Args:
        search_path (str): path to find tarballs in
        single_path (str): whether to restrict unzipping to a single tar ball
    """
    config = config_retrieve(['workflow'])
    output_dir = config.get('output_prefix')
    driver_image = config.get('driver_image')

    if search_path:
        bucket_name, subdir = get_path_components_from_path(search_path)

        blobs = get_tarballs_from_path(bucket_name, subdir, search_path)

        if len(blobs) == 0:
            logging.info('Nothing to do, quitting')
            sys.exit(0)

        # iterate over targets, set each one off in parallel
        for blobname, blobsize in blobs:
            # create and config job
            create_job(blobname, blobsize, bucket_name, subdir, output_dir, driver_image)

    elif single_path:
        file_path = to_path(single_path)
        blobsize = file_path.stat().st_size
        bucket = file_path.bucket
        subdir = '/'.join(file_path.parts[2:-1])
        blobname = f'{subdir}/{file_path.name}'
        create_job(blobname, blobsize, bucket, subdir, output_dir, driver_image)

    get_batch().run(wait=False)


def create_job(blobname: str, blobsize: int, bucket_name: str, subdir: str, output_dir: str, driver_image: str):

    job = get_batch().new_job(name=f'decompress {blobname}')
    job.image(driver_image)
    job.cpu(4)
    job.storage(f'{blobsize}Gi')
    authenticate_cloud_credentials_in_job(job)
    copy_common_env(job)
    prepare_git_job(
        job,
        organisation='populationgenomics',
        repo_name='analysis-runner',
        commit=get_commit_hash(),
    )
    job.command('cd /io')
    job.command(
        f"""
        python3 {UNZIP_SCRIPT} \
            --bucket {bucket_name} \
            --subdir {subdir} \
            --blob_name {blobname} \
            --outdir {output_dir}
    """,
    )

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()
