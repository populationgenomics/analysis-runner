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


def copy_to_release(project: str, paths: list[str]):
    """
    Copy many files from main bucket paths to the release bucket with todays date as directory
    """
    today = time.strftime('%Y-%m-%d')
    #release_path = f'gs://cpg-{project}-release/{today}/'
    release_path = f'gs://cpg-{project}-test/{today}/'

    subprocess.run(['gcloud', 'storage', '--billing-project', project, 'cp', *paths, release_path], check=True)
    logging.info(f'Copied {paths} into {release_path}')


@click.command()
@click.option('--project', '-p', help='Metamist name of the project', default="")
@click.argument('samples', nargs=-1)
def main(project: str, samples):
    """

    Parameters
    ----------
    project :   a metamist project name, optional as it can be pulled from the AR config
    samples :   a list of sample ids to copy to the release bucket
    """
    if not project:
        config = get_config()
        project = config['workflow']['dataset']

    # sample_ids = list(samples)

    # # Retrieve latest crams for selected samples
    # latest_crams = AnalysisApi().get_latest_analysis_for_samples_and_type(
    #     AnalysisType('cram'), project, request_body=sample_ids
    # )

    # # Get all paths of files to be copied to release
    # cram_paths = []
    # for cram in latest_crams:
    #     #cram_paths.append(cram['output'])
    #     cram_paths.append(cram['output'] + '.crai')
    cram_paths = ['gs://cpg-perth-neuro-test/ed_test_20230314/test1.rtf',
                  'gs://cpg-perth-neuro-test/ed_test_20230314/test2.rtf',
                  'gs://cpg-perth-neuro-test/ed_test_20230314/test3.rtf',
                  'gs://cpg-perth-neuro-test/ed_test_20230314/test4.rtf']
    copy_to_release(project, cram_paths)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
