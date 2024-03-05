#!/usr/bin/env python3

"""
Transfer datasets from presigned URLs to a dataset's GCP main-upload bucket.
"""

import os
from shlex import quote
from typing import List

import click
from cloudpathlib import AnyPath
from cpg_utils import to_path
from cpg_utils.config import get_config
from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    dataset_path,
    get_batch,
)
import hailtop.batch.job as hb_job


def check_output_path(output_path: str):
    """
    Check if the output path exists and if it contains any files
    Args:
        output_path (str): the output path to check
    """
    if found_files := {p.as_uri() for p in to_path(output_path).iterdir()}:
        print(f'Files found in {output_path}:')
        print(found_files)
        return found_files
    return set()


def parse_presigned_url_file(
    file_path: str, filenames: bool, garvan_manifest: bool
) -> dict[str, str]:
    """
    Parse a presigned URL file to extract the filenames and presigned URLs
    Returns:
        dict: a dictionary with presigned URLs as keys and filenames as values
    """
    urls_filenames = {}
    if garvan_manifest:
        urls_filenames = parse_garvan_manifest(file_path)
    else:
        urls_filenames = parse_generic_manifest(file_path, filenames)

    if not urls_filenames:
        raise ValueError('No URLs found in the file')
    if incorrect_urls := [
        url for url in urls_filenames if not url.startswith('https://')
    ]:
        raise ValueError(f'Incorrect URLs: {incorrect_urls}')

    return urls_filenames


def parse_generic_manifest(file_path: str, filenames: bool) -> dict[str, str]:
    """
    Parse a signed URL file to extract the presigned URLs (and filenames, if present)
    """
    urls_filenames = {}
    with AnyPath(file_path).open() as file:
        for i, line in enumerate(file.readlines()):
            if not line.strip():
                continue
            line = line.strip()
            if filenames:
                parts = line.split(' ')
                urls_filenames[parts[1]] = parts[0]
            else:
                urls_filenames[line] = f'{i}_{line.split("?")[0]}'
    return urls_filenames


def parse_garvan_manifest(file_path: str) -> dict[str, str]:
    """
    Parse a Garvan manifest file to extract the filenames and presigned URLs

    Args:
        file_path (str): path to the Garvan manifest file
    Returns:
        dict: a dictionary with presigned URLs as keys and filenames as values
    """
    urls_filenames = {}
    with AnyPath(file_path).open() as file:
        for line in file.readlines():
            if line.strip():
                parts = line.strip().split(' ')
                urls_filenames[parts[3].strip("'")] = parts[2]
    return urls_filenames


@click.command('Transfer_datasets from signed URLs')
@click.option(
    '--filenames',
    is_flag=True,
    default=False,
    help='Use filenames defined before each url',
)
@click.option('--garvan-manifest', '-g', is_flag=True, help='File is a Garvan manifest')
@click.option(
    '--untar',
    is_flag=True,
    help=(
        'Untar tarballs and upload the contents. Note this will mean the repeats of the same tar file that has previously been downloaded will be redownloaded and re-extracted because only the contents are uploaded, and the tar file will not be detected as already existing in the bucket.'
    ),
)
@click.option(
    '--storage',
    default=100,
    help='Storage in GiB for each cURL job. If untar, ensure this is at least 2x the tarball size.',
)
@click.option(
    '--concurrent-job-cap',
    default=20,
    help=(
        'To limit the number of concurrent jobs, hopefully preventing cURL errors due to too many open connections'
    ),
)
@click.option('--presigned-url-file-path')
def main(
    presigned_url_file_path: str,
    filenames: bool,
    garvan_manifest: bool = False,
    untar: bool = False,
    storage: int = 100,
    concurrent_job_cap: int = 20,
):
    """
    Given a list of presigned URLs, download the files to disk and then upload them to GCS.

    --presigned-url-file-path is a file with a list of presigned URLs, one per line.

    ** Allowed formats for the presigned url file **
      - Just URLs, no filenames (default):
        ```
        https://example.com/file1
        https://example.com/file2
        ```
      - With filenames (use --filenames flag):
        ```
        file1 https://example.com/file1
        file2 https://example.com/file2
        ```
      - Garvan manifest (use --garvan-manifest / -g flag):
        ```
        curl -Lo Sample01.tar.gz 'https://filesender.aarnet.edu.au/download.php?token=abc&files_ids=123456'
        curl -Lo SummaryFiles.tar.gz 'https://filesender.aarnet.edu.au/download.php?token=xyz&files_ids=123457'
        ```

    --output-dir (from analysis-runner submission) defines the output file prefix in target GCP bucket.
    --storage is the amount of storage in GiB for each cURL job.
    --concurrent-job-cap is used to limit the number of concurrent jobs.
      - Too many concurrent jobs can cause cURL errors due to too many open connections.
        - e.g. curl: (55) OpenSSL SSL_write: Connection reset by peer, errno 104
    """

    env_config = get_config()
    cpg_driver_image = env_config['workflow']['driver_image']
    billing_project = env_config['hail']['billing_project']
    dataset = env_config['workflow']['dataset']
    output_prefix = env_config['workflow']['output_prefix']
    assert all({billing_project, cpg_driver_image, dataset, output_prefix})

    output_path = dataset_path(output_prefix, 'upload')
    existing_files = check_output_path(output_path)
    urls_filenames = parse_presigned_url_file(
        presigned_url_file_path, filenames, garvan_manifest
    )

    all_jobs: List[hb_job.Job] = []

    def manage_concurrency(new_job: hb_job.Job):
        """
        Manage concurrency, so that there is a cap on simultaneous jobs
        Args:
            new_job (hb_job.Job): a new job to add to the stack
        """
        if len(all_jobs) > concurrent_job_cap:
            new_job.depends_on(all_jobs[-concurrent_job_cap])
        all_jobs.append(new_job)

    batch = get_batch(name=f'transfer {dataset}', default_image=cpg_driver_image)
    for url, filename in urls_filenames.items():
        if to_path(os.path.join(output_path, filename)).as_uri() in existing_files:
            print(f'File {filename} already exists in {output_path}, skipping...')
            continue

        # Create a new job for each file
        j = batch.new_job(f'cURL ({filename})')
        if filename == 'SummaryFiles.tar.gz':  # Common to Garvan manifests
            j.storage(f'30Gi')
        else:
            j.storage(f'{storage}Gi')

        quoted_url = quote(url)
        authenticate_cloud_credentials_in_job(job=j)
        # catch errors during the cURL
        j.command('set -euxo pipefail')
        j.command(f'curl -C - -Lf {quoted_url} -o {filename}')
        if filename.endswith('.tar.gz') and untar:
            j.command(f'tar -xf {filename}')
            j.command(f'rm {filename}')
            j.command(f'gsutil -m cp -r * {output_path}/')
        else:
            j.command(f'gsutil -m cp {filename} {output_path}/{filename}')
        manage_concurrency(j)

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
