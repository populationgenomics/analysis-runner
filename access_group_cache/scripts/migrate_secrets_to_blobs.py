"""
Secret versions exploded during an issue with group member resolution
this script should clean up the extra versions generated during this time
"""
import json
from datetime import datetime
from typing import List, Tuple
import logging

from google.cloud import secretmanager
from cloudpathlib import AnyPath

import cpg_utils.permissions

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

logging.basicConfig(filename='migrate-secrets-to-blobs.log', level=logging.INFO)

# Create the Secret Manager client.
secret_manager = secretmanager.SecretManagerServiceClient()


def _write_group_membership_list(group: str, members: List[str], date: datetime):

    version = date.isoformat().split('.')[0]

    filename = cpg_utils.permissions.group_name_to_filename(group, version)
    f = AnyPath(filename).open('w+')
    f.write(','.join(members))
    f.close()


def migrate_and_destroy_secrets(project_id, secret_name, should_delete=False):
    """
    Destroy DISABLED secret versions if create_time is between the , making the payload irrecoverable.
    """
    assert project_id and secret_name
    secret_path = secret_manager.secret_path(project_id, secret_name)

    # Destroy previous versions that fall between start_time and end_time (inclusive)
    for version in secret_manager.list_secret_versions(request={'parent': secret_path}):
        create_time = version.create_time
        if version.state == secretmanager.SecretVersion.State.DISABLED:
            secret_manager.enable_secret_version(request={'name': version.name})
        elif version.state == secretmanager.SecretVersion.State.DESTROYED:
            logging.info(f'Skipping {version.name} because it is already destroyed')
            continue

        # should be enabled now
        members = (
            secret_manager.access_secret_version(request={'name': version.name})
            .payload.data.decode('UTF-8')
            .split(',')
        )
        group_name = secret_name.replace('-members-cache', '@populationgenomics.org.au')
        logging.info(f'Migrated group {group_name} from {version.name}')

        _write_group_membership_list(group_name, members, create_time)

        if should_delete:
            secret_manager.destroy_secret_version(request={'name': version.name})

            logging.info(
                f'Destroyed secret version: {version.name} (created: {version.create_time})'
            )


def get_secret_names() -> List[Tuple[str, str]]:
    """
    Load server config and generate (project_id, secret_name) pairs
    """
    secret_path = secret_manager.secret_path(
        ANALYSIS_RUNNER_PROJECT_ID, 'server-config'
    )
    response = secret_manager.access_secret_version(
        request={'name': f'{secret_path}/versions/latest'}
    )
    config = json.loads(response.payload.data.decode('UTF-8'))

    group_types = [
        'access',
        'web-access',
        'test',
        'standard',
        'full',
    ]
    # add SM group types
    group_types.extend(
        f'sample-metadata-{env}-{rs}'
        for env in ('main', 'test')
        for rs in ('read', 'write')
    )
    project_id_and_secret_name: List[Tuple[str, str]] = []
    for dataset in config:
        project_id = config[dataset]['projectId']
        for group_type in group_types:
            group = f'{dataset}-{group_type}'
            project_id_and_secret_name.append((project_id, f'{group}-members-cache'))

    return project_id_and_secret_name


def main():
    """DRIVE program"""
    project_id_and_secret_names = get_secret_names()
    for project_id, secret_name in project_id_and_secret_names:
        migrate_and_destroy_secrets(project_id, secret_name)


if __name__ == '__main__':
    main()
