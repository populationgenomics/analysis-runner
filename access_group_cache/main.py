"""Stores group membership information as secrets, for faster lookups."""

import asyncio
import json
import urllib
from typing import Dict, List, Optional
import aiohttp
from google.cloud import secretmanager
import google.api_core.exceptions

PROJECT_ID = 'analysis-runner'

secret_manager = secretmanager.SecretManagerServiceClient()


def _read_secret(name: str) -> Optional[str]:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    try:
        response = secret_manager.access_secret_version(
            request={
                'name': f'{secret_manager.secret_path(PROJECT_ID, name)}/versions/latest'
            }
        )
    except google.api_core.exceptions.ClientError as e:
        # Fail gracefully if there's no secret version yet.
        print(f'Problem accessing secret {name}: {e}')
        return None
    return response.payload.data.decode('UTF-8')


async def _groups_lookup(access_token: str, group_name: str) -> Optional[str]:
    async with aiohttp.ClientSession() as session:
        # https://cloud.google.com/identity/docs/reference/rest/v1/groups/lookup
        async with session.get(
            f'https://cloudidentity.googleapis.com/v1/groups:lookup?'
            f'groupKey.id={urllib.parse.quote(group_name)}',
            headers={'Authorization': f'Bearer {access_token}'},
        ) as resp:
            if resp.status != 200:
                return None

            content = await resp.text()
            return json.loads(content)['name']


async def _groups_memberships_list(access_token: str, group_parent: str) -> List[str]:
    result = []

    async with aiohttp.ClientSession() as session:
        page_token = None
        while True:
            # https://cloud.google.com/identity/docs/reference/rest/v1/groups/lookup
            async with session.get(
                f'https://cloudidentity.googleapis.com/v1/{group_parent}/memberships?'
                f'pageToken={page_token or ""}',
                headers={'Authorization': f'Bearer {access_token}'},
            ) as resp:
                content = await resp.text()
                decoded = json.loads(content)
                for member in decoded['memberships']:
                    result.append(member['preferredMemberKey']['id'])

                page_token = decoded.get('nextPageToken')
                if not page_token:
                    break

    return result


async def _transitive_group_members(access_token: str, group_name: str) -> List[str]:
    queue = [group_name]
    seen = set()
    result = []

    while queue:
        current = queue.pop()
        if current in seen:
            continue  # Break cycles
        seen.add(current)

        group_parent = await _groups_lookup(access_token, current)
        if not group_parent:
            # Group couldn't be resolved, which usually means it's an individual.
            result.append(current)
            continue

        # It's a group, so add its members for the next round.
        queue.extend(await _groups_memberships_list(access_token, group_parent))

    return result


async def _get_service_account_access_token() -> str:
    # https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances#applications
    async with aiohttp.ClientSession() as session:
        async with session.get(
            'http://metadata.google.internal/computeMetadata/v1/instance/'
            'service-accounts/default/token',
            headers={'Metadata-Flavor': 'Google'},
        ) as resp:
            content = await resp.text()
            return json.loads(content)['access_token']


async def _get_dataset_access_group_members(
    datasets: List[str],
) -> Dict[str, List[str]]:
    access_token = await _get_service_account_access_token()

    group_names = [
        f'{dataset}-access@populationgenomics.org.au' for dataset in datasets
    ]
    results = await asyncio.gather(
        _transitive_group_members(access_token, group_name)
        for group_name in group_names
    )

    return dict(zip(datasets, results))


def access_group_cache(unused_data, unused_context):
    """Cloud Function entry point."""

    config = json.loads(_read_secret('server-config'))

    # Google Groups API queries are ridiculously slow, on the order of a few hundred ms
    # per query. That's why we use async processing here to keep processing times low.
    group_members = asyncio.run(_get_dataset_access_group_members(config.keys()))

    for dataset in config:
        secret_value = ','.join(sorted(group_members[dataset]))

        # TODO
        print(f'{dataset}: {secret_value}')
        continue

        # Check whether the current secret version is up-to-date.
        secret_name = f'{dataset}-access-members-cache'
        current_secret = _read_secret(secret_name)

        if current_secret == secret_value:
            print(f'Cache for {dataset} is up-to-date.')
            continue  # Nothing left to do.

        response = secret_manager.add_secret_version(
            request={
                'parent': secret_manager.secret_path(PROJECT_ID, secret_name),
                'payload': {'data': secret_value.encode('UTF-8')},
            }
        )

        print(f'Added secret version: {response.name}')
