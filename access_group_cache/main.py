"""Stores group membership information as secrets, for faster lookups."""

import asyncio
import json
import logging
import urllib
import os
from typing import List, Optional, Union, Tuple
import aiohttp
import cpg_utils.cloud
from flask import Flask

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

app = Flask(__name__)


async def _get_group_parent(access_token: str, group_name: str) -> Optional[str]:
    """
    Get the group parent (group ID)
    """
    if group_name.endswith('.iam.gserviceaccount.com'):
        # we know it's not a group, because it's a service account
        return None

    async with aiohttp.ClientSession() as session:
        # https://cloud.google.com/identity/docs/reference/rest/v1/groups/lookup
        async with session.get(
            f'https://cloudidentity.googleapis.com/v1/groups:lookup?'
            f'groupKey.id={urllib.parse.quote(group_name)}',
            headers={'Authorization': f'Bearer {access_token}'},
        ) as resp:
            if resp.status == 403:
                # This is the "email isn't actually a google group" case
                # Probably a 403 to stop any unauthorized info leakage
                return None

            resp.raise_for_status()

            content = await resp.text()
            return json.loads(content)['name']


async def _groups_memberships_list(
    access_token: str, group_parent: str
) -> Tuple[List[str], List[str]]:
    """Get a tuple of (group_emails, member_emails) in this group_parent"""
    members = []
    child_groups = []

    async with aiohttp.ClientSession() as session:
        page_token = None
        while True:
            # https://cloud.google.com/identity/docs/reference/rest/v1/groups/lookup
            async with session.get(
                f'https://cloudidentity.googleapis.com/v1/{group_parent}/memberships?'
                f'view=FULL&pageToken={page_token or ""}',
                headers={'Authorization': f'Bearer {access_token}'},
            ) as resp:
                resp.raise_for_status()

                content = await resp.text()
                decoded = json.loads(content)

                for member in decoded['memberships']:
                    member_id = member['preferredMemberKey']['id']
                    if member['type'] == 'GROUP':
                        child_groups.append(member_id)
                    else:
                        members.append(member_id)

                page_token = decoded.get('nextPageToken')
                if not page_token:
                    break

    return child_groups, members


async def _transitive_group_members(
    access_token: str, group_name: str
) -> Union[List[str], Exception]:
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
            *(_get_group_parent(access_token, group) for group in remaining_groups),
            return_exceptions=True,
        )

        memberships_aws = []
        for group, group_parent in zip(remaining_groups, group_parents):
            if isinstance(group_parent, Exception):
                return group_parent

            if group_parent is None:
                # Group couldn't be resolved, which usually means it's an individual.
                result.add(group)
            else:
                # It's a group, so add its members for the next round.
                memberships_aws.append(
                    _groups_memberships_list(access_token, group_parent)
                )

        memberships = await asyncio.gather(*memberships_aws, return_exceptions=True)

        groups = []
        for membership_result in memberships:
            if isinstance(membership_result, Exception):
                # if any membership checked, fail the whole group
                return membership_result

            child_groups, members = membership_result

            result.update(members)
            groups.extend(child_groups)

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


async def _get_group_members(
    group_names: List[str],
) -> List[Union[List[str], Exception]]:
    access_token = await _get_service_account_access_token()

    return await asyncio.gather(
        *(
            _transitive_group_members(access_token, group_name)
            for group_name in group_names
        ),
        return_exceptions=True,
    )


@app.route('/', methods=['POST'])
def index():
    """Cloud Run entry point."""

    config = json.loads(
        cpg_utils.cloud.read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    )

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

    groups = []
    dataset_by_group = {}
    for dataset in config:
        for group_type in group_types:
            group = f'{dataset}-{group_type}'
            groups.append(group)
            dataset_by_group[group] = dataset

    # Google Groups API queries are ridiculously slow, on the order of a few hundred ms
    # per query. That's why we use async processing here to keep processing times low.
    all_group_members = asyncio.run(
        _get_group_members([f'{group}@populationgenomics.org.au' for group in groups])
    )

    for group, group_members in zip(groups, all_group_members):
        if isinstance(group_members, Exception):
            logging.warning(
                f'Skipping update for "{group}" due to exception {group_members}'
            )
            continue
        secret_value = ','.join(group_members)

        dataset = dataset_by_group[group]
        project_id = config[dataset]['projectId']

        # Check whether the current secret version is up-to-date.
        secret_name = f'{group}-members-cache'
        current_secret = cpg_utils.cloud.read_secret(project_id, secret_name)

        if current_secret == secret_value:
            print(f'Secret {secret_name} is up-to-date')
        else:
            cpg_utils.cloud.write_secret(project_id, secret_name, secret_value)
            print(f'Updated secret {secret_name}')

    return ('', 204)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
