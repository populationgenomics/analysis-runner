#!/usr/bin/env python3

"""
Given a dataset and date, creates a manifest of files in the date folder
of the release bucket, generating signed URLs for each file and saving
them to the manifest.
"""

import csv
from datetime import datetime, timedelta
import logging
import os
import subprocess
import sys

import click
from cpg_utils import to_path
from cpg_utils.config import get_config
from google.api_core.exceptions import Forbidden
from metamist.graphql import gql, query


PRIVATE_KEY_PATH = os.environ['ACCESS_CREDS_PATH']

RELEASE_MANIFEST_HEADERS = [
    'Project',
    'Family',
    'Participant',
    'Sample',
    'CPG ID',
    'Type',
    'Size',
    'Signed URL',
    'Expiration Date',
]

FILE_EXTENSION_TYPE_MAP = {
    'cram': 'CRAM',
    'crai': 'CRAM Index',
    'cram.crai': 'CRAM Index',
    'cram.md5': 'CRAM MD5',
    'g.vcf.gz': 'GVCF',
    'g.vcf.bgz': 'GVCF',
    'g.vcf.gz.md5': 'GVCF MD5',
    'g.vcf.bgz.md5': 'GVCF MD5',
    'g.vcf.gz.tbi': 'GVCF',
    'vcf.gz': 'VCF',
    'vcf.bgz': 'VCF',
    'vcf.gz.md5': 'VCF MD5',
    'vcf.bgz.md5': 'VCF MD5',
    'vcf.gz.tbi': 'VCF Index',
}

SG_ID_MAPPING_QUERY = gql(
    """
    query SequencingGroup($dataset: String!, $sequencingGroups: [String!]) {
        project(name: $dataset) {
            sequencingGroup(id: {in_: $sequencingGroups}) {
                id
                type
                sample {
                    id
                    externalId
                    participant {
                        id
                        externalId
                        families {
                            id
                            externalId
                        }
                    }
                }
            }
        }
    }
    """
)


def get_sg_id_maps(
    dataset: str,
    sg_ids: list[str],
):
    """Get the sequencing group IDs for a list of external sample IDs and/or external participant IDs"""
    sequencing_groups = query(
        SG_ID_MAPPING_QUERY,
        dataset=dataset,
        sequencing_groups=sg_ids,
    )['project']['sequencingGroup']

    return {
        sg['id']: {
            'type': sg['type'],
            'sample': sg['sample']['externalId'],
            'participant': sg['sample']['participant']['externalId'],
            'family': sg['sample']['participant']['families']['externalId'],
        }
        for sg in sequencing_groups
        if sg['id'] in sg_ids
    }


def get_release_files(date: str, dataset: str):
    """Get the files staged in the release bucket"""
    release_path = f'gs://cpg-{dataset}-release/{date}'
    return {f.as_uri() for f in to_path(release_path).iterdir()}


def get_release_file_sizes(release_files: set[str]):
    """Get the file sizes of the files staged in the release bucket"""
    return {path: to_path(path).stat().st_size for path in release_files}


def get_release_files_to_cpg_id_map(release_files: set[str]):
    """Get the CPG sequencing group ID corresponding to each file, only if the ID (CPGXXX) is the filename"""
    path_to_sg_id_map = {}
    for f in release_files:
        if os.path.basename(f).split('.')[0].startswith('CPG'):
            path_to_sg_id_map[f] = os.path.basename(f).split('.')[0]
    return path_to_sg_id_map


def generate_signed_url(path: str, shared_project: str):
    """Generates a signed url with a 7 day expiry for a given path"""
    url = subprocess.run(
        [
            'gcloud',
            'storage',
            'sign-url',
            path,
            f'--private-key-file={os.path.join(PRIVATE_KEY_PATH, f"{shared_project}.json")}',
            '--duration=7d',
            '--region=australia-southeast1',
            f'--query-params=userProject={shared_project}',
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return url.stdout.split('signed_url: ')[1].rstrip('\n')


@click.command
@click.option('--date', default=datetime.now().strftime('%Y-%m-%d'))
@click.option('--manifest-path', help='Path to save the manifest to')
@click.option('--project', help='GCP project name, if different from dataset name')
@click.argument('dataset')
def main(date: str, manifest_path: str | None, project: str | None, dataset: str):
    """
    Create a manifest of files in the release bucket in the date folder, containing the following columns:

    Project             (metamist dataset)
    Family              (external ID)
    Individual          (participant external ID)
    Sample              (external ID)
    CPG ID              (sequencing group ID)
    Type                (CRAM, CRAM Index, CRAM MD5, GVCF, GVCF MD5, VCF, VCF MD5, VCF Index)
    Size                (in bytes)
    Signed URL          (signed URL for the file)
    Expiration Date     (7 days from URL generation)

    Save the manifest as a csv file in the main bucket

    Parameters
    ----------
    date : str
        The date of the release in YYYY-MM-DD format (default: today's date)
    manifest_path : str
        The path to save the manifest to (default: cpg-dataset-{main|test}/release_manifests/YYYY-MM-DD.csv)
    project: str
        The GCP project name, if different from the dataset name
    dataset : str
        The name of the dataset
    """
    if not manifest_path:
        manifest_path = (
            get_config()['storage'][dataset]['default']
            + f'/release_manifests/{datetime.now().strftime("%Y-%m-%d")}.csv'
        )

    manifest_path = to_path(manifest_path)
    try:
        manifest_path.touch()
    except Forbidden as e:
        logging.error(f'Permission denied when trying to create file: {e}')
        sys.exit(1)

    if not project:
        project = dataset

    release_files = get_release_files(date, dataset)
    release_file_sizes = get_release_file_sizes(release_files)
    release_file_sg_id_map = get_release_files_to_cpg_id_map(release_files)

    sg_id_maps = get_sg_id_maps(dataset, release_file_sg_id_map.values())

    signed_urls = {
        path: generate_signed_url(path, f'{project}-shared') for path in release_files
    }

    with open(manifest_path, 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=RELEASE_MANIFEST_HEADERS)
        for path, sg_id in release_file_sg_id_map.items():
            writer.writerow(
                {
                    'Project': dataset,
                    'Family': sg_id_maps[sg_id]['family'],
                    'Participant': sg_id_maps[sg_id]['participant'],
                    'Sample': sg_id_maps[sg_id]['sample'],
                    'CPG ID': sg_id,
                    'File Type': FILE_EXTENSION_TYPE_MAP[
                        os.path.basename(path).split('.')[-1]
                    ],
                    'File Size': release_file_sizes[path],
                    'File Signed URL': signed_urls[path],
                    'Expiration ': (datetime.now() + timedelta(days=7)).strftime(
                        '%Y-%m-%d'
                    ),
                }
            )

    logging.info(f'Release manifest written to {manifest_path}')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
