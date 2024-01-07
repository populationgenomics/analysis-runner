#!/usr/bin/env python3

"""
Given a project, billing-project ID, and path to a file
containing urls, copies all the urls from the file into
the project's release bucket.
"""

import logging
import sys
import subprocess
from collections import defaultdict
from datetime import datetime

import click
from google.cloud import storage

# pylint: disable=E0401,E0611
from cpg_utils.config import get_config
from cpg_utils import to_path
from metamist.apis import AnalysisApi
from metamist.graphql import gql, query

client = storage.Client()

ANALYSIS_OUTPUTS_QUERY = gql(
    """
    query AnalysisOutputs($dataset: String!, $type: String!, $sequencingGroups: [String!]) {
        project(name: $dataset) {
            sequencingGroups(id: { in_: $sequencingGroups }) {
                id
                analysis(type: { eq: $type }, status: { eq: 'COMPLETED' }) {
                    id
                    type
                    output
                    timestampCompleted
                }
            }
        }
    }
    """
)

SG_ID_MAPPING_QUERY = gql(
    """
    query SequencingGroup($dataset: String!) {
        project(name: $dataset) {
            sequencingGroup {
                id
                type
                sample {
                    id
                    externalId
                    participant {
                        id
                        externalId
                    }
                }
            }
        }
    }
    """
)


def get_analyses_output_paths(analysis_ids: list[int]):
    """Get the output paths for a list of analysis IDs"""
    aapi = AnalysisApi()
    return [aapi.get_analysis_by_id(analysis_id)['output'] for analysis_id in analysis_ids]


def get_sg_ids_from_external_ids(
    dataset: str,
    external_sample_ids: list[str] | None,
    external_participant_ids: list[str] | None,
):
    """Get the sequencing group IDs for a list of external sample IDs and/or external participant IDs"""
    sequencing_groups = query(
        SG_ID_MAPPING_QUERY,
        dataset=dataset,
    )[
        'project'
    ]['sequencingGroup']

    sample_seq_group_map = defaultdict(list)
    participant_seq_group_map = defaultdict(list)
    for sg in sequencing_groups:
        sample_seq_group_map[sg['sample']['externalId']].append(sg['id'])
        participant_seq_group_map[sg['sample']['participant']['externalId']].append(
            sg['id']
        )

    sg_ids = set()
    if external_sample_ids:
        sg_ids.update(
            sample_seq_group_map[external_sample_id]
            for external_sample_id in external_sample_ids
        )
    if external_participant_ids:
        sg_ids.update(
            participant_seq_group_map[external_participant_id]
            for external_participant_id in external_participant_ids
        )

    return list(sg_ids)


def get_sg_analyses(dataset: str, sg_ids: list[str], analysis_types: list[str]):
    """Get the latest completed analysis output of each analysis type for each sequencing group"""
    latest_analyses_by_type = defaultdict(dict)
    for analysis_type in analysis_types:
        sg_analyses = query(
            ANALYSIS_OUTPUTS_QUERY,
            dataset=dataset,
            type=analysis_type,
            sequencingGroups=sg_ids,
        )['project']['sequencingGroups']

        latest_analysis_by_sg = {}
        for sg in sg_analyses:
            sg_id = sg['id']
            analyses = sg['analysis']
            latest_analysis = get_latest_analysis(analyses)
            latest_analysis_by_sg[sg_id] = latest_analysis

        latest_analyses_by_type[analysis_type] = latest_analysis_by_sg

    return latest_analyses_by_type


def get_latest_analysis(analyses: list[dict]):
    """Sorts completed analyses by timestamp and returns the latest one"""
    if not analyses:
        return {}
    return sorted(
        analyses,
        key=lambda analysis: datetime.strptime(
            analysis['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
        ),
    )[-1]


def get_filepaths_from_analyses(latest_analyses_by_type: dict[str, dict]):
    """Return the list of analysis output paths, including index files for crams and gvcfs"""
    paths = []
    for analysis_type, analyses in latest_analyses_by_type.items():
        for analysis in analyses.values():
            if not analysis:
                continue
            paths.append(analysis['output'])
            if analysis_type == 'cram':
                paths.append(analysis['output'] + '.crai')
            elif analysis_type == 'gvcf':
                paths.append(analysis['output'] + '.tbi')

    return paths


def check_paths_exist(paths: list[str]):
    """
    Checks a list of gs:// paths to see if they point to an existing blob
    Logs the invalid paths if any are found

    eg. paths = ['gs://cpg-test/exome/cram/sample.cram', 'gs://cpg-test/exome/gvcf/sample.g.vcf.gz']
    """
    # Get the common path prefixes for all paths
    path_prefixes = set()
    for path in paths:
        path_prefixes.add('/'.join(path.split('/')[:-1]))

    files = set()
    for path_prefix in path_prefixes:
        files.update({f.as_uri() for f in to_path(path_prefix.rstrip('/')).iterdir()})

    invalid_paths = False
    if set(paths) - files:
        logging.warning(f'Invalid paths: {set(paths) - files}')
        invalid_paths = True

    return not invalid_paths


def copy_to_release(project: str, billing_project: str, paths: list[str]):
    """
    Copy the input file paths to the release bucket with todays date in the prefix
    """
    release_path = f'gs://cpg-{project}-release/{datetime.now().strftime("%Y-%m-%d")}/'
    logging.info(f'Copying {len(paths)} files to {release_path}:')
    for path in paths:
        logging.info(path)
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
    logging.info(f'{len(paths)} files copied to {release_path}')


@click.command()
@click.option('--project', '-p', help='Metamist name of the project', default='')
@click.option('--billing-project', '-b', help='The GCP billing project to use')
@click.option('--urls-file-path', '-u', help='A file containing the urls to copy')
@click.option(
    '--use-metamist',
    '-m',
    help='Use the Metamist GraphQL API to get the urls',
    is_flag=True,
)
def main(
    project: str, billing_project: str, urls_file_path: str | None, use_metamist: bool
):
    """

    Parameters
    ----------
    project :   a metamist dataset name, optional as it can be pulled from the AR config
    billing_project :    a GCP project ID to bill to
    urls_file_path :   a full GS path to a file containing the links to move into the release bucket
    use_metamist :  use the Metamist GraphQL API to get the urls - note that you must include a list
                    of analysis IDs OR external sample IDs, external participant IDs, or sequencing 
                    group IDs in the config file, as well as the analysis types to copy. 
                    If this flag is not set, the urls_file_path option must be set.

    use_metamist config parameters
    ------------------------------
    [workflow]
    analysis_ids = <list of analysis IDs>  # takes precedence over the other options
    
    analysis_types = <list of analysis types>

    # One of the following three must be set
    sequencing_group_ids = <list of sequencing group IDs>
    external_sample_ids = <list of external sample IDs>
    external_participant_ids = <list of external participant IDs>
    """
    if not urls_file_path and not use_metamist:
        raise ValueError('Either urls_file_path or use_metamist must be set')

    config = get_config()
    if not project:
        project = config['workflow']['dataset']

    if not billing_project:
        billing_project = project

    if use_metamist:
        while not paths:
            if config['workflow']['analysis_ids']:
                paths = get_analyses_output_paths(config['workflow']['analysis_ids'])
            elif not config['workflow']['analysis_types']:
                raise ValueError('analysis_types must be set in the config file if analysis_ids is not set')
            if (
                    not config['workflow']['sequencing_group_ids']
                and not config['workflow']['external_sample_ids']
                and not config['workflow']['external_participant_ids']
            ):
                raise ValueError(
                    'One of sequencing_group_ids, external_sample_ids, or external_participant_ids must be set in the config file'
                )

            if sequencing_group_ids := config['workflow']['sequencing_group_ids']:
                sg_ids = sequencing_group_ids
            elif external_sample_ids := config['workflow']['external_sample_ids']:
                sg_ids = get_sg_ids_from_external_ids(project, external_sample_ids, None)
            elif external_participant_ids := config['workflow']['external_participant_ids']:
                sg_ids = get_sg_ids_from_external_ids(
                    project, None, external_participant_ids
                )

            paths = get_filepaths_from_analyses(
                get_sg_analyses(project, sg_ids, config['workflow']['analysis_types'])
            )

    else:
        with to_path(urls_file_path).open(encoding='utf-8') as f:
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
