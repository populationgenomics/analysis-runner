"""
Secret versions exploded during an issue with group member resolution
this script should clean up the extra versions generated during this time
"""
import json
from datetime import datetime
from typing import List, Tuple

from google.cloud import secretmanager

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

# Create the Secret Manager client.
secret_manager = secretmanager.SecretManagerServiceClient()

START_TO_DELETE_FROM = datetime(2021, 12, 3)
FINISH_TO_DELETE_TO = datetime(2021, 12, 17)


def destroy_old_secret_versions(
    project_id,
    secret_name,
    start_time=START_TO_DELETE_FROM,
    finish_time=FINISH_TO_DELETE_TO,
):
    """
    Destroy DISABLED secret versions if create_time is between the , making the payload irrecoverable.
    """
    assert project_id and secret_name
    secret_path = secret_manager.secret_path(project_id, secret_name)

    # Destroy previous versions that fall between start_time and end_time (inclusive)
    for version in secret_manager.list_secret_versions(request={'parent': secret_path}):
        should_delete = version.state == secretmanager.SecretVersion.State.DISABLED
        if start_time or finish_time:
            tzinfo = (start_time or finish_time).tzinfo
            create_time = version.create_time.replace(tzinfo=tzinfo)
            after_start: bool = start_time is None or start_time <= create_time
            before_end: bool = finish_time is None or create_time <= finish_time

            should_delete = should_delete and after_start and before_end

        if should_delete:
            secret_manager.destroy_secret_version(request={'name': version.name})

            print(
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
        destroy_old_secret_versions(project_id, secret_name)


if __name__ == '__main__':
    main()
