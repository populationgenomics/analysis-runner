"""Convenience functions for using the GCP Cloud Identity API."""

import logging
import googleapiclient.discovery

SERVICE_NAME = 'cloudidentity.googleapis.com'
API_VERSION = 'v1'
DISCOVERY_URL = f'https://{SERVICE_NAME}/$discovery/rest?version={API_VERSION}'

service = googleapiclient.discovery.build(
    SERVICE_NAME, API_VERSION, discoveryServiceUrl=DISCOVERY_URL
)


def check_group_membership(user: str, group: str) -> bool:
    """Returns whether the user is a member of the group.

    Both user and group are specified as email addresses.

    Note:
    - This does *not* look up transitive memberships, i.e. nested groups.
    - The service account performing the lookup must be a member of the group itself,
      in order to have visiblity of all members.
    """

    try:
        # See https://bit.ly/37WcB1d for the API calls.
        # Pylint can't resolve the methods in Resource objects.
        # pylint: disable=E1101
        parent = service.groups().lookup(groupKey_id=group).execute()['name']

        _ = (
            service.groups()
            .memberships()
            .lookup(parent=parent, memberKey_id=user)
            .execute()['name']
        )

        return True
    except googleapiclient.errors.HttpError as e:  # Failed lookups result in a 403.
        logging.warning(e)
        return False
