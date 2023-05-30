#!/usr/bin/env python3


"""
Given a project and sample IDs, copies cram files for
each sample listed into the project's release bucket.
"""

import logging
import sys
import subprocess
import time
import click

# pylint: disable=E0401,E0611
from cpg_utils.config import get_config
from sample_metadata.apis import AnalysisApi
from sample_metadata.models import AnalysisType


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
@click.argument('samples', nargs=-1)
def main(project: str, billing_project: str, samples):
    """

    Parameters
    ----------
    project :   a metamist project name, optional as it can be pulled from the AR config
    samples :   a list of sample ids to copy to the release bucket
    """
    if not project:
        config = get_config()
        project = config['workflow']['dataset']

    if not billing_project:
        billing_project = project

    sample_ids = list(samples)

    # Retrieve latest crams for selected samples
    latest_crams = AnalysisApi().get_latest_analysis_for_samples_and_type(
        AnalysisType('cram'), project, request_body=sample_ids
    )

    # Get all paths of files to be copied to release
    cram_paths = []
    for cram in latest_crams:
        cram_paths.append(cram['output'])
        cram_paths.append(cram['output'] + '.crai')

    # Check if all paths are valid and execute the copy commands if they are
    if check_paths_exist(cram_paths):
        copy_to_release(project, billing_project, cram_paths)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
