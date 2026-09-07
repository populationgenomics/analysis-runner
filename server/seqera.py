import os

import requests
from aiohttp import web

seqera_api = os.environ['TOWER_API_ENDPOINT']
seqera_token = os.environ['TOWER_ACCESS_TOKEN']
seqera_workspace = os.environ['TOWER_WORKSPACE_ID']


def find_workspace_id(workspace_name: str) -> int:
    if '/' not in workspace_name:
        return int(workspace_name)

    resp = requests.get(f'{seqera_api}/user-info', headers=auth_header)
    resp.raise_for_status()
    user_id = resp.json()['user']['id']

    resp = requests.get(f'{seqera_api}/user/{user_id}/workspaces', headers=get_headers)
    resp.raise_for_status()

    org_name, base_workspace_name = workspace_name.split('/')
    for workspace in resp.json()['orgsAndWorkspaces']:
        if workspace['orgName'] == org_name and workspace['workspaceName'] == base_workspace_name:
            return workspace['workspaceId']

    raise ValueError(f"Can't find workspace id for {workspace_name!r}")


auth_header = {'Authorization': f'Bearer {seqera_token}'}
get_headers = {**auth_header, 'Accept': 'application/json'}
post_headers = {**auth_header, 'Content-Type': 'application/json'}

workspace_param = {'workspaceId': find_workspace_id(seqera_workspace)}


def user_info():
    resp = requests.get(f'{seqera_api}/user-info', headers=auth_header)
    print(resp)
    print(resp.json())


def describe_workflow(workflow_id):
    resp = requests.get(f'{seqera_api}/workflow/{workflow_id}', headers=get_headers, params=workspace_param)
    resp.raise_for_status()
    resolved_config = resp.json()['workflow']['configText']
    print(resolved_config)


def launch_workflow():
    launch = {
        'launch': {
            'computeEnvId': '7W9fZGejIKeSOOSUXhKbYm',
            'pipeline': 'https://github.com/jmarshall/test',
            'revision': '21cd3b269ef07aab1f44e6e97755c2e52efac9c7',
            'workDir': '/tmp',
        },
        'stubRun': True,
    }

    resp = requests.post(f'{seqera_api}/workflow/launch', headers=post_headers, params=workspace_param, json=launch)
    print(resp)
    print(resp.content)
    resp.raise_for_status()


def add_seqera_routes(routes: web.RouteTableDef):
    """Add Seqera route to 'routes' flask API."""

    @routes.post('/seqera')
    async def seqera(request: web.Request) -> web.Response:
        email = get_email_from_request(request)


# For easy testing
if __name__ == '__main__':
    import sys

    if sys.argv[1] == 'user':
        user_info()
    elif sys.argv[1] == 'launch':
        launch_workflow()
    elif sys.argv[1] == 'describe':
        describe_workflow(sys.argv[2])
