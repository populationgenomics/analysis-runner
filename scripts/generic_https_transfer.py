#!/usr/bin/env python3

"""
Transfer datasets from presigned URLs to a dataset's GCP main-upload bucket.
"""

import os
from shlex import quote

import click
from cloudpathlib import AnyPath
from cpg_utils.config import get_config
from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    dataset_path,
    get_batch,
)


@click.command('Transfer_datasets from signed URLs')
@click.option(
    '--filenames',
    is_flag=True,
    default=False,
    help='Use filenames defined before each url',
)
@click.option('--presigned-url-file-path')
def main(presigned_url_file_path: str, filenames: bool):
    """
    Given a list of presigned URLs, download the files and upload them to GCS.
    If each signed url is prefixed by a filename and a space, use the --filenames flag
    GCP suffix in target GCP bucket is defined using analysis-runner's --output
    """

    env_config = get_config()
    cpg_driver_image = env_config['workflow']['driver_image']
    billing_project = env_config['hail']['billing_project']
    dataset = env_config['workflow']['dataset']
    output_prefix = env_config['workflow']['output_prefix']
    assert all({billing_project, cpg_driver_image, dataset, output_prefix})
    names = None
    with AnyPath(presigned_url_file_path).open() as file:
        if filenames:
            names = [
                line.strip().split(' ')[0] for line in file.readlines() if line.strip()
            ]
            #  reset readlines() to start of file
            print(f'seeking to line {file.seek(0)}')
            presigned_urls = [
                line.strip().split(' ')[1] for line in file.readlines() if line.strip()
            ]
        else:
            presigned_urls = [line.strip() for line in file.readlines() if line.strip()]

    incorrect_urls = [url for url in presigned_urls if not url.startswith('https://')]
    if incorrect_urls:
        raise ValueError(f'Incorrect URLs: {incorrect_urls}')

    batch = get_batch(name=f'transfer {dataset}')

    output_path = dataset_path(output_prefix, 'upload')

    # may as well batch them to reduce the number of VMs
    for idx, url in enumerate(presigned_urls):
        if names:
            filename = names[idx]
        else:
            filename = os.path.basename(url).split('?')[0]
        j = batch.new_job(f'URL {idx} ({filename})')
        quoted_url = quote(url)
        authenticate_cloud_credentials_in_job(job=j)
        # catch errors during the cURL
        j.command('set -euxo pipefail')
        j.command(
            f'curl -L {quoted_url} | gsutil cp - {os.path.join(output_path, filename)}'
        )

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
