"""
This scheduled job resolves the config_path of Analysis Runner entries in Metamist
belonging to Seqera workflows. The resolved configurations are uploaded to the config bucket
and the corresponding Analysis Runner entry in Metamist is also updated.
"""

import datetime
import logging
import os
import time

from cloudpathlib import AnyPath
from metamist_util import get_dataset
from seqera_api import (
    BATCH_URL_UNAVAILABLE,
    CONFIG_PENDING,
    SEQERA_STATUS_KEY,
    SeqeraApiClient,
)

from metamist.graphql import gql, query

CONFIG_PATH_PREFIX = os.getenv('CONFIG_PATH_PREFIX', 'gs://cpg-config')

JOB_DEADLINE_SECONDS = int(os.getenv('JOB_DEADLINE_SECONDS', '240'))
UNRESOLVED_AFTER = datetime.timedelta(hours=2)

# The Metamist meta key
CONFIG_UNRESOLVED = 'CONFIG_UNRESOLVED' # A custom statue
# https://docs.seqera.io/platform-cloud/monitoring/overview
SEQERA_FINISHED_STATUSES = ('SUCCEEDED', 'FAILED', 'CANCELLED')

MY_PROJECTS_QUERY = gql(
    """
    query MyProjects {
        myProjects {
            id
        }
    }
    """
)

# This query pulls all the unresolved entries belonging to the input projects
PENDING_ENTRIES_QUERY = gql(
    """
    query PendingAnalysisRunners($projectIds: [Int!]!, $meta: JSON!) {
        analysisRunners(
            project: {in_: $projectIds}, configPath: {isnull: true}, meta: $meta
        ) {
            arGuid
            accessLevel
            batchUrl
            meta
            timestamp
            project {
                name
            }
        }
    }
    """
)

UPDATE_ENTRY_MUTATION = gql(
    """
    mutation UpdateAnalysisRunnerEntry(
        $arGuid: String!, $update: AnalysisRunnerUpdateInput!
    ) {
        analysisRunner {
            updateAnalysisRunnerLog(arGuid: $arGuid, arUpdateInput: $update) {
                arGuid
                configPath
                batchUrl
            }
        }
    }
    """
)


def get_project_ids() -> list[int]:

    response = query(MY_PROJECTS_QUERY)
    return [project['id'] for project in response['myProjects']]


def get_pending_entries(project_ids: list[int]) -> list[dict]:

    response = query(
        PENDING_ENTRIES_QUERY,
        {
            'projectIds': project_ids,
            'meta': {SEQERA_STATUS_KEY: {'eq': CONFIG_PENDING}},
        },
    )
    return response['analysisRunners']


def write_config(ar_guid: str, config_text: str) -> str:

    config_path = AnyPath(CONFIG_PATH_PREFIX) / f'{ar_guid}.config'
    # The file could be written by a previous run whose Metamist update failed.
    if not config_path.exists():
        config_path.write_text(config_text)
    return str(config_path)


def get_missing_batch_url(seqera: SeqeraApiClient, entry: dict) -> str | None:

    if entry['batchUrl'] != BATCH_URL_UNAVAILABLE:
        return None
    return seqera.workflow_url(entry['meta']['workflow_id'])


def fetch_workflow(entry: dict, seqera: SeqeraApiClient) -> dict:
    workflow_id = entry['meta']['workflow_id']
    return seqera.describe_workflow(workflow_id)


def get_entry_age(entry: dict) -> datetime.timedelta:

    created = datetime.datetime.fromisoformat(entry['timestamp'])
    if created.tzinfo is None:
        # MariaDB Metamist stores the timestamp without a timezone, in UTC
        created = created.replace(tzinfo=datetime.timezone.utc)
    return datetime.datetime.now(datetime.timezone.utc) - created


def update_metamist_entry(
    ar_guid: str,
    config_path: str | None = None,
    current_status: str | None = None,
    batch_url: str | None = None,
    reason: str | None = None,
) -> None:

    update: dict = {}
    if config_path is not None:
        update['configPath'] = config_path
    if current_status is not None:
        update['meta'] = {SEQERA_STATUS_KEY: current_status}
    if batch_url is not None:
        update['batchUrl'] = batch_url
    if not update:
        return

    query(UPDATE_ENTRY_MUTATION, {'arGuid': ar_guid, 'update': update})
    if reason is not None:
        logging.error(f'Seqera entry unresolved {ar_guid}: {reason}')



def resolve_entry(entry: dict, clients: dict[tuple, SeqeraApiClient]) -> bool:
    """
    During a successful execution, the scheduler save the resolved config of a Seqera workflow and update its Metamist entry.
    Returns True on success, False otherwise.
    """
    access_level = entry['accessLevel']
    dataset = get_dataset(entry['project']['name'], access_level)

    client_key = (dataset, access_level)
    if client_key not in clients:
        clients[client_key] = SeqeraApiClient(dataset, access_level)
    seqera = clients[client_key]

    # Resolve the batch url, if it was not set during workflow launch
    batch_url = get_missing_batch_url(seqera, entry)
    workflow = fetch_workflow(entry, seqera)

    config_text = workflow.get('configText')
    workflow_status = workflow.get('status')

    if config_text:
        config_path = write_config(entry['arGuid'], config_text)

        update_metamist_entry(
            entry['arGuid'],
            config_path,
            current_status=workflow_status, # The status here does not reflect the final workflow status
            batch_url=batch_url,
        )
        return True

    if workflow_status in SEQERA_FINISHED_STATUSES:
        # Update the Metamist entry, if the config can not be resolved even after the workflow has finished.
        update_metamist_entry(
            entry['arGuid'],
            current_status=workflow_status,
            batch_url=batch_url,
            reason=f'Workflow finished with status: {workflow_status} without resolving config',
        )
        return False

    update_metamist_entry(entry['arGuid'], batch_url=batch_url)
    return False


def main() -> None:
    logging.info('Starting Analysis Runner Config Resolver')

    project_ids = get_project_ids()
    if not project_ids:
        raise RuntimeError('No Metamist project accessible for the Analysis Runner Config Resolver')

    pending_entries = get_pending_entries(project_ids)
    timeout_timestamp = time.monotonic() + JOB_DEADLINE_SECONDS
    seqera_clients: dict[tuple, SeqeraApiClient] = {}

    for entry in pending_entries:
        # Stops picking new entries once the deadline passes.
        if time.monotonic() > timeout_timestamp:
            break
        try:
            resolve_entry(entry, seqera_clients)
        except Exception as e:
            logging.error(f'Failed to resolve entry {entry.get('arGuid')}. Will be retried until timeout. {e!r}')
            if get_entry_age(entry) > UNRESOLVED_AFTER:
                update_metamist_entry(
                    entry['arGuid'],
                    current_status=CONFIG_UNRESOLVED,
                    reason=f'Skipping after timeout: {e!r}',
                )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
