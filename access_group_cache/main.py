"""Stores group membership information as secrets, for faster lookups."""

from typing import List, Optional, Tuple, Dict, Set
import asyncio
from datetime import datetime
import subprocess
from collections import defaultdict
from graphlib import TopologicalSorter
import json
import logging
import urllib

import os
import aiohttp
import cpg_utils.cloud
import cpg_utils.permissions
from flask import Flask
from cloudpathlib import AnyPath

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
logging.basicConfig(level=logging.INFO)

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
                # Probably a 403 to stop any unauthorized info leakage
                # This covers two unfortunate cases:
                #   1. The "email isn't actually a google group"
                #   2. The analysis-runner-cache SA is not in the group to resolve it
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
            # use view=FULL so we get the membership 'type' (GROUP or other)
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


def _write_group_membership_list(group: str, members: List[str]):

    versions = ['latest', datetime.now().isoformat().split('.')[0]]

    for version in versions:
        filename = cpg_utils.permissions._group_name_to_filename(group, version)
        f = AnyPath(filename).open('w+')
        f.write(','.join(members))
        f.close()


async def _get_service_account_access_token() -> str:
    # https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances#applications
    return (
        subprocess.check_output(['gcloud', 'auth', 'print-access-token'])
        .decode()
        .strip()
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(
            'http://metadata.google.internal/computeMetadata/v1/instance/'
            'service-accounts/default/token',
            headers={'Metadata-Flavor': 'Google'},
        ) as resp:
            content = await resp.text()
            return json.loads(content)['access_token']


@app.route('/', methods=['POST'])
def index():
    """Cloud Run entry point."""
    main()


def main():
    """Entry point, more convenient to test"""

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
        get_group_members([f'{group}@populationgenomics.org.au' for group in groups])
    )

    for group, group_members in all_group_members.items():

        # Check whether the current secret version is up-to-date.
        try:
            current_grp = cpg_utils.permissions.get_group_members(group)
        except FileNotFoundError:
            # doesn't exist, so we need to create it
            current_grp = []

        shorted_group_name = group.split('@')[0]
        if current_grp == group_members:
            print(f'Group cache {shorted_group_name} is up-to-date')
        else:
            _write_group_membership_list(group, group_members)
            print(f'Updated group cache {shorted_group_name}')

    return ('', 204)


async def get_group_members(
    groups: List[str], filter_to_requested=True
) -> Dict[str, List[str]]:
    """
    This function (iteratively) gets group members
    and avoids hitting the Google Groups API too many times by
    caching subgroups.
    """

    group_parents_map: Dict[str, Optional[str]] = {}
    group_members_list: Dict[str, Set[str]] = defaultdict(set)
    group_dependencies: Dict[str, Set[str]] = defaultdict(set)

    access_token = await _get_service_account_access_token()

    rounds = 0
    start = datetime.now()

    remaining_groups = groups
    while remaining_groups:
        rounds += 1
        # group_parents are group IDs
        group_parents = await asyncio.gather(
            *(_get_group_parent(access_token, group) for group in remaining_groups),
            return_exceptions=True,
        )

        membership_coroutine_map = {}
        for group, group_parent in zip(remaining_groups, group_parents):
            if isinstance(group_parent, Exception):
                # this feels bad
                # return group_parent
                raise Exception(f'Could not resolve group "{group}": {group_parent}')

            group_parents_map[group] = group_parent
            if group_parent is None:
                # Group couldn't be resolved, which usually means it's an individual.
                logging.warning(f'Warning: group is unresolvable: {group}')
                if group not in group_members_list:
                    group_members_list[group] = set()

            else:
                # It's a group, so add its members for the next round.
                membership_coroutine_map[group] = _groups_memberships_list(
                    access_token, group_parent
                )

        _next_groups = set()
        if membership_coroutine_map:

            membership_group_names, membership_coroutines = list(
                zip(*membership_coroutine_map.items())
            )
            memberships = await asyncio.gather(
                *membership_coroutines, return_exceptions=True
            )

            for group, result in zip(membership_group_names, memberships):
                if isinstance(result, Exception):
                    # if any membership checked, fail the whole group
                    raise Exception(f'Could not resolve group {group}')

                child_groups, members = result
                # group_dependencies help us resolve groups later
                group_dependencies[group] = child_groups
                group_members_list[group].update(members)
                _next_groups.update(child_groups)

        remaining_groups = [grp for grp in _next_groups if grp not in group_parents_map]

    # resolve
    ordered_groups = TopologicalSorter(group_dependencies)

    resolved_members = {}

    for group in ordered_groups.static_order():
        resolved = list(group_members_list.get(group, []))
        for dep in group_dependencies.get(group, []):
            if dep in group_members_list:
                resolved.extend(group_members_list[dep])

        resolved_members[group] = sorted(resolved)

    if filter_to_requested:
        requested_groups = set(groups)
        resolved_members = {
            group: members
            for group, members in resolved_members.items()
            if group in requested_groups
        }

            cpg_utils.cloud.write_secret(project_id, secret_name, secret_value)
            print(f'Updated secret {secret_name}')

    return resolved_members


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
