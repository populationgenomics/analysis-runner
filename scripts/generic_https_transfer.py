#!/usr/bin/env python3

"""
Transfer datasets from presigned URLs to a dataset's GCP main-upload bucket.
"""

import os
from shlex import quote

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
from typing import List


def parse_presigned_url_file(file_path: str, filenames: bool = False):
    """
    Parse a file containing presigned URLs to extract the URLs
    
    Args:
        file_path (str): path to the file containing presigned URLs
    Returns:
        list: a list of presigned URLs
    """
    with AnyPath(file_path).open() as file:
        return [line.strip() for line in file.readlines() if line.strip()]

def parse_garvan_manifest(file_path: str):
    """
    Parse a Garvan manifest file to extract the filenames and presigned URLs
    
    Example of a Garvan manifest file:
    
    ```
    curl -Lo Sample01.tar.gz 'https://filesender.aarnet.edu.au/download.php?token=abc&files_ids=123456' # Expires 30 Mar 2024
    curl -Lo SummaryFiles.tar.gz 'https://filesender.aarnet.edu.au/download.php?token=xyz&files_ids=123457' # Expires 30 Mar 2024
    ```
    
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
@click.option('--concurrent-job-cap', default=5, help='To limit the number of concurrent jobs, hopefully preventing cURL errors due to too many open connections')
@click.option('--presigned-url-file-path')
def main(presigned_url_file_path: str, filenames: bool, garvan_manifest: bool = False, concurrent_job_cap: int = 5):
    """
    Given a list of presigned URLs, download the files and upload them to GCS.
    If each signed url is prefixed by a filename and a space, use the --filenames flag
    GCP suffix in target GCP bucket is defined using analysis-runner's --output
    
    If the file is a Garvan manifest, then parse the filenames and URLs from the manifest.
    ex: download_links.txt, which contains two lines:
    
    
    concurrent-job-cap is used to limit the number of concurrent jobs. Too many concurrent jobs can cause cURL errors due to too many open connections.
    ex. curl: (55) OpenSSL SSL_write: Connection reset by peer, errno 104
    """

    env_config = get_config()
    cpg_driver_image = env_config['workflow']['driver_image']
    billing_project = env_config['hail']['billing_project']
    dataset = env_config['workflow']['dataset']
    output_prefix = env_config['workflow']['output_prefix']
    assert all({billing_project, cpg_driver_image, dataset, output_prefix})
    
    if garvan_manifest:
        urls_filenames = parse_garvan_manifest(presigned_url_file_path)
    else:        
        with AnyPath(presigned_url_file_path).open() as file:
            if filenames:
                urls_filenames = {line.strip().split(' ')[1]: line.strip().split(' ')[0] for line in file.readlines() if line.strip()}
            else:
                urls_filenames = {line.strip(): '' for line in file.readlines() if line.strip()}

    incorrect_urls = [url for url in urls_filenames if not url.startswith('https://')]
    if incorrect_urls:
        raise ValueError(f'Incorrect URLs: {incorrect_urls}')
    
    batch = get_batch(name=f'transfer {dataset}', default_image=cpg_driver_image)
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

    output_path = dataset_path(output_prefix, 'upload')
    files_in_output = [f.as_uri() for f in to_path(output_path).iterdir()]
    if files_in_output:
        print(f'Files found in {output_path}:')
        print(files_in_output)

    # may as well batch them to reduce the number of VMs
    for url, filename in urls_filenames.items():
        if filenames or garvan_manifest:
            if to_path(os.path.join(output_path, filename)).as_uri() in files_in_output:
                print(f'File {filename} already exists in {output_path}')
                continue 
        j = batch.new_job(f'cURL ({filename})')
        j.storage('100Gi')
        quoted_url = quote(url)
        authenticate_cloud_credentials_in_job(job=j)
        # catch errors during the cURL
        j.command('set -euxo pipefail')
        j.command(f'curl -C - -Lf {quoted_url} -o {filename}')
        j.command(f'gsutil cp {filename} {output_path}')
        manage_concurrency(j)
            
    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
