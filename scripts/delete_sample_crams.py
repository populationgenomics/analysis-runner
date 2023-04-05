#!/usr/bin/env python3


"""
Given a bucket path and sample IDs, deletes cram files in
the path for each sample listed.
"""

import logging
import sys
import subprocess
import click


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
@click.option(
    '--somaliers',
    '-s',
    help='Also delete cram.somalier files',
    is_flag=True,
    default=False,
)
@click.argument('samples', nargs=-1)
def main(delete_path: str, somaliers: bool, samples):
    """

    Parameters
    ----------
    delete_path :   a gcp path containing the crams to delete.
                        e.g. gs://cpg-project-main/exome/cram
    samples     :   a list of sample ids to copy to the release bucket
    """

    sample_ids = list(samples)

    # Generate the paths to the crams and crais to be deleted
    cram_paths = []
    for sample in sample_ids:
        cram_paths.append(f'{delete_path}/{sample}.cram')
        cram_paths.append(f'{delete_path}/{sample}.cram.crai')
        if somaliers:
            cram_paths.append(f'{delete_path}/{sample}.cram.somalier')

    # Check if all paths are valid and execute the rm commands if they are
    if check_paths_exist(cram_paths):
        delete_from_bucket(cram_paths)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
