#!/usr/bin/env python3


"""
Given a bucket path and sample IDs, deletes cram files in
the path for each sample listed.
"""

import logging
import sys
import subprocess
from os.path import basename
import click


def get_cram_paths(samples: list[str], search_path: str) -> list[str]:
    """
    Return all paths to CRAMs and related files (e.g. cram.crai, cram.md5)
    found in the search path for the list of samples.
    """
    cram_paths = []
    for sample_id in samples:
        result = (
            subprocess.run(
                ['gsutil', 'ls', f'{search_path}/{sample_id}*'],
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.strip('\n')
            .split('\n')
        )
        cram_paths.extend(result)
        logging.info(
            f'{sample_id}: found {[basename(path) for path in result]} in {search_path}'
        )

    return cram_paths


def delete_from_bucket(paths: list[str]):
    """
    Delete CRAM and CRAM.crai files from bucket paths
    """

    subprocess.run(
        ['gsutil', 'rm', *paths],
        check=True,
    )
    logging.info(f'Deleted items: {paths}')


@click.command()
@click.option('--delete-path', '-d', help='GCP path to CRAMs to delete', required=True)
@click.argument('samples', nargs=-1)
def main(delete_path: str, samples):
    """

    Parameters
    ----------
    delete_path :   a gcp path containing the crams to delete.
                        e.g. gs://cpg-project-main/exome/cram
    samples     :   a list of sample ids to copy to the release bucket
    """

    samples = list(samples)

    # Get the paths to all the CRAMs and related files for the samples
    cram_paths = get_cram_paths(samples, delete_path)

    delete_from_bucket(cram_paths)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
