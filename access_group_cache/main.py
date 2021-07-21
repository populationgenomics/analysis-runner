"""Stores group membership information as secrets, for faster lookups."""

import asyncio
import json
import urllib
import os
from typing import List, Optional
import aiohttp
import cpg_utils.cloud
from flask import Flask

PROJECT_ID = 'analysis-runner'

app = Flask(__name__)


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
    groups = [group_name]
    seen = set()
    result = set()

    while groups:
        remaining_groups = []
        for group in groups:
            if group in seen:
                continue  # Break cycles.
            seen.add(group)
            remaining_groups.append(group)

        group_parents = await asyncio.gather(
            *(_groups_lookup(access_token, group) for group in remaining_groups)
        )

        memberships_aws = []
        for group, group_parent in zip(remaining_groups, group_parents):
            if group_parent:
                # It's a group, so add its members for the next round.
                memberships_aws.append(
                    _groups_memberships_list(access_token, group_parent)
                )
            else:
                # Group couldn't be resolved, which usually means it's an individual.
                result.add(group)

        memberships = await asyncio.gather(*memberships_aws)

        groups = []
        for members in memberships:
            groups.extend(members)

    return sorted(list(result))


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


async def _get_group_members(group_names: List[str]) -> List[List[str]]:
    access_token = await _get_service_account_access_token()

    return await asyncio.gather(
        *(
            _transitive_group_members(access_token, group_name)
            for group_name in group_names
        )
    )


@app.route('/', methods=['POST'])
def index():
    """Cloud Run entry point."""

    config = json.loads(cpg_utils.cloud.read_secret(PROJECT_ID, 'server-config'))

    groups = []
    for dataset in config:
        groups.append(f'{dataset}-access')
        groups.append(f'{dataset}-web-access')

    # Google Groups API queries are ridiculously slow, on the order of a few hundred ms
    # per query. That's why we use async processing here to keep processing times low.
    all_group_members = asyncio.run(
        _get_group_members([f'{group}@populationgenomics.org.au' for group in groups])
    )

    for group, group_members in zip(groups, all_group_members):
        secret_value = ','.join(group_members)

        # Check whether the current secret version is up-to-date.
        secret_name = f'{group}-members-cache'
        current_secret = cpg_utils.cloud.read_secret(PROJECT_ID, secret_name)

        if current_secret == secret_value:
            print(f'Secret {secret_name} is up-to-date')
        else:
            cpg_utils.cloud.write_secret(PROJECT_ID, secret_name, secret_value)
            print(f'Updated secret {secret_name}')

    return ('', 204)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
