#!/usr/bin/env python3

"""
Given a project, billing-project ID, and path to a file
containing urls, copies all the urls from the file into
the project's release bucket.
"""

import logging
import os
import sys
import subprocess
import time
import click

from google.cloud import storage

# pylint: disable=E0401,E0611
from cpg_utils.config import get_config
from cpg_utils.cloud import get_path_components_from_gcp_path

client = storage.Client()


def check_paths_exist(paths: list[str]):
    """
    Checks a list of gs:// paths to see if they point to an existing blob
    Logs the invalid paths if any are found
    """
    invalid_paths = False
    for path in paths:
        # gsutil ls <path> returns '<path>\n' if path exists
        result = subprocess.run(
            ['gsutil', 'ls', path], check=True, capture_output=True, text=True
        ).stdout.strip('\n')
        if result == path:
            continue
        # If path does not exist, log the path and set invalid_paths to True
        logging.info(f'Invalid path: {path}')
        invalid_paths = True

    if invalid_paths:
        return False
    return True


def copy_to_release(project: str, billing_project: str, paths: list[str]):
    """
    Copy many files from main bucket paths to the release bucket with todays date as directory
    """
    today = time.strftime('%Y-%m-%d')
    release_path = f'gs://cpg-{project}-release/{today}/'

    subprocess.run(
        [
            'gcloud',
            'storage',
            '--billing-project',
            billing_project,
            'cp',
            *paths,
            release_path,
        ],
        check=True,
    )
    logging.info(f'Copied {paths} into {release_path}')


@click.command()
@click.option('--project', '-p', help='Metamist name of the project', default='')
@click.option('--billing-project', '-b', help='The GCP billing project to use')
@click.argument('url_file')
def main(project: str, billing_project: str, url_file: str):
    """

    Parameters
    ----------
    project :   a metamist project name, optional as it can be pulled from the AR config
    billing_project :    a GCP project ID to bill to
    bucket :    the GCP bucket containing the data to copy
    url_file :   a full GS path to a file containing the links to move into the release bucket
    """
    if not project:
        config = get_config()
        project = config['workflow']['dataset']

    if not billing_project:
        billing_project = project

    if not url_file.startswith(f'gs://'):
        raise ValueError('url_file must be a fully qualified GS path')

    path_components = get_path_components_from_gcp_path(url_file)

    bucket = path_components['bucket']
    file_name = os.path.join(path_components['suffix'], path_components['file'])

    input_bucket = client.get_bucket(bucket)
    blob = input_bucket.get_blob(file_name)
    if not blob:
        raise RuntimeError(f'No file found at url_file path {url_file}')
    blob.download_to_filename(file_name)

    with open(file_name, 'r', encoding='ascii') as f:
        paths = [line.rstrip() for line in f]

    # Check if all paths are valid and execute the copy commands if they are
    if check_paths_exist(paths):
        copy_to_release(project, billing_project, paths)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
