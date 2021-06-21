"""Stores group membership information as secrets, for faster lookups."""

from typing import Optional
import json
import multiprocessing
from google.cloud import secretmanager
import googleapiclient.discovery
import google.api_core.exceptions

PROJECT_ID = 'analysis-runner'

CLOUD_IDENTITY_SERVICE_NAME = 'cloudidentity.googleapis.com'
CLOUD_IDENTITY_API_VERSION = 'v1'
DISCOVERY_URL = (
    f'https://{CLOUD_IDENTITY_SERVICE_NAME}/$discovery/rest?'
    f'version={CLOUD_IDENTITY_API_VERSION}'
)

cloud_identity_service = googleapiclient.discovery.build(
    CLOUD_IDENTITY_SERVICE_NAME,
    CLOUD_IDENTITY_API_VERSION,
    discoveryServiceUrl=DISCOVERY_URL,
)

secret_manager = secretmanager.SecretManagerServiceClient()


def _read_secret(name: str) -> Optional[str]:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    try:
        response = secret_manager.access_secret_version(
            request={
                'name': f'{secret_manager.secret_path(PROJECT_ID, name)}/versions/latest'
            }
        )
    # except google.api_core.exceptions.FailedPrecondition as e:
    except google.api_core.exceptions.ClientError as e:
        # Fail gracefully if there's no secret version yet.
        print(f'Problem accessing secret {name}: {e}')
        return None
    return response.payload.data.decode('UTF-8')


def _process_dataset_group(dataset: str) -> None:
    group_name = f'{dataset}-access@populationgenomics.org.au'

    print(f'Fetching members for {group_name}...')

    # See https://bit.ly/37WcB1d for the API calls.
    # Pylint can't resolve the methods in Resource objects.
    # pylint: disable=E1101
    parent = (
        cloud_identity_service.groups().lookup(groupKey_id=group_name).execute()['name']
    )

    members = []
    page_token = None
    while True:
        response = (
            cloud_identity_service.groups()
            .memberships()
            .list(parent=parent, pageToken=page_token)
            .execute()
        )

        for member in response['memberships']:
            members.append(member['preferredMemberKey']['id'])

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    all_members = ','.join(members)

    # Check whether the current secret version is up-to-date.
    secret_name = f'{dataset}-access-members-cache'
    current_secret = _read_secret(secret_name)

    if current_secret == all_members:
        print(f'Cache for {dataset} is up-to-date.')
        return  # Nothing left to do.

    response = secret_manager.add_secret_version(
        request={
            'parent': secret_manager.secret_path(PROJECT_ID, secret_name),
            'payload': {'data': all_members.encode('UTF-8')},
        }
    )

    print(f'Added secret version: {response.name}')


def access_group_cache(unused_data, unused_context):
    """Main entry point."""
    config = json.loads(_read_secret('server-config'))
    # Given that each group takes multiple seconds to process (!), we're running them
    # concurrently. We're using processes, as google-api-python-client is not thread-safe:
    # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
    with multiprocessing.Pool(20) as pool:
        pool.map(_process_dataset_group, config)
