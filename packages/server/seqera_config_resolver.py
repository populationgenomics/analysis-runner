"""
This scheduled job resolves the config_path of Analysis Runner entries in Metamist
belonging to Seqera workflows. The resolved configurations are uploaded to the config bucket
and the corresponding Analysis Runner entry in Metamist is also updated.
"""

import asyncio
import logging
import os
from http import HTTPStatus

import requests
from cloudpathlib import AnyPath
from metamist_util import get_dataset
from seqera_api import BATCH_URL_UNAVAILABLE, SEQERA_PENDING, SeqeraApiClient

from metamist.graphql import gql, query_async

CONFIG_PATH_PREFIX = os.getenv('CONFIG_PATH_PREFIX', 'gs://cpg-config')

# configPath of entries that will never get a resolved config, so they're no longer pending
SEQERA_NOT_FOUND = 'SEQERA-NOT-FOUND'
FINISHED_STATUSES = {'SUCCEEDED', 'FAILED', 'CANCELLED'}

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
    query PendingAnalysisRunners($projectIds: [Int!]!, $configPath: String!) {
        analysisRunners(project: {in_: $projectIds}, configPath: {eq: $configPath}) {
            arGuid
            accessLevel
            batchUrl
            meta
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


async def get_project_ids() -> list[int]:

    response = await query_async(MY_PROJECTS_QUERY)
    return [project['id'] for project in response['myProjects']]


async def get_pending_entries(project_ids: list[int]) -> list[dict]:

    response = await query_async(
        PENDING_ENTRIES_QUERY,
        {'projectIds': project_ids, 'configPath': SEQERA_PENDING},
    )
    return response['analysisRunners']


def write_config(ar_guid: str, config_text: str) -> str:

    # TODO the seqera configText is groovy like. Just dumping now.
    config_path = AnyPath(CONFIG_PATH_PREFIX) / f'{ar_guid}.config'
    config_path.write_text(config_text)
    return str(config_path)


def get_missing_batch_url(seqera: SeqeraApiClient, entry: dict) -> str | None:

    if entry['batchUrl'] != BATCH_URL_UNAVAILABLE:
        return None

    try:
        return seqera.workflow_url(entry['meta']['workflow_id'])
    except (requests.HTTPError, KeyError):
        return None


async def fetch_workflow(entry: dict, seqera: SeqeraApiClient) -> dict | None:

    ar_guid = entry['arGuid']
    workflow_id = entry['meta']['workflow_id']
    try:
        return seqera.describe_workflow(workflow_id)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == HTTPStatus.NOT_FOUND:
            update_input = {'arGuid': ar_guid, 'update': {'configPath': SEQERA_NOT_FOUND}}
            await query_async(UPDATE_ENTRY_MUTATION, update_input)
    return None


async def save_config_and_update_entry(
    entry: dict, seqera: SeqeraApiClient, workflow: dict
) -> bool:

    ar_guid = entry['arGuid']
    workflow_id = entry['meta']['workflow_id']

    config_path = None
    config_text = workflow.get('configText')
    status = workflow.get('status')
    if config_text:
        config_path = write_config(ar_guid, config_text)
    elif status in FINISHED_STATUSES:
        logging.warning(f'Seqera workflow of {ar_guid}:{workflow_id} finished ({status}) without a resolved config')
        # Update the Metamist entry, if the config can not be resolved even after the workflow has finished.
        # This avoids refetching failing Metamist analysis runner entries
        config_path = f'SEQERA-{status}'

    update = {}
    if config_path is not None:
        update['configPath'] = config_path

    # update the batch url, if it was not set during workflow launch
    batch_url = get_missing_batch_url(seqera, entry)
    if batch_url is not None:
        update['batchUrl'] = batch_url

    if not update:
        return False

    await query_async(UPDATE_ENTRY_MUTATION, {'arGuid': ar_guid, 'update': update})
    return bool(config_text)


async def resolve_entry(entry: dict, clients: dict[tuple, SeqeraApiClient]) -> bool:
    """
    During a successful execution, the scheduler save the resolved config of a Seqera workflow and update its Metamist entry.
    """
    access_level = entry['accessLevel']
    dataset = get_dataset(entry['project']['name'], access_level)

    client_key = (dataset, access_level)
    if client_key not in clients:
        clients[client_key] = SeqeraApiClient(dataset, access_level)
    seqera = clients[client_key]

    workflow = await fetch_workflow(entry, seqera)
    if workflow is None:
        return False
    return await save_config_and_update_entry(entry, seqera, workflow)


async def main() -> None:
    project_ids = await get_project_ids()
    if not project_ids:
        raise RuntimeError('No Metamist project accessible for the Scheduler')

    pending_entries = await get_pending_entries(project_ids)

    seqera_clients: dict[tuple, SeqeraApiClient] = {}
    resolved = 0
    failed = []
    for entry in pending_entries:
        try:
            if await resolve_entry(entry, seqera_clients):
                resolved += 1
        except Exception:
            failed.append(entry['arGuid'])

    logging.info(
        f'Resolved {resolved} of {len(pending_entries)} pending analysis runner Seqera entries'
    )
    if failed:
        raise RuntimeError(f'Failed to resolve {len(failed)} analysis runner Seqera entries: {failed}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
