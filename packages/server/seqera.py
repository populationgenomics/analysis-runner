import requests
from aiohttp import web
from cachetools.func import lru_cache
from util import get_seqera_config, read_ar_secret


class SeqeraApiClient:
    def __init__(self, dataset: str, access_level: str):
        seqera_config = get_seqera_config()
        self.org_id = seqera_config['org_id']
        self.api_url = seqera_config['api_url']
        self.dataset_config = seqera_config['datasets'][dataset][access_level]

        self.token = read_ar_secret(self.dataset_config['launch_token_secret_name'])

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json',
        }
        url = f'{self.api_url}/{endpoint}'
        response = requests.get(url, headers=headers, params=params)

        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, body: dict, params: dict | None = None) -> dict:
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
        }
        url = f'{self.api_url}/{endpoint}'
        response = requests.post(url, headers=headers, params=params, json=body)

        if not response.ok and response.text:
            reason = f'{response.status_code} {response.reason}: {response.text}'
            raise web.HTTPBadRequest(reason=reason)
        response.raise_for_status()
        return response.json()

    @property
    def server_url(self) -> str:
        return self.api_url.replace('://api.', '://', 1).removesuffix('/api')

    @property
    @lru_cache
    def org_name(self) -> str:
        return self.get(f'orgs/{self.org_id}')['organization']['name']

    @property
    @lru_cache
    def workspace_name(self) -> str:
        workspace_id = self.dataset_config['workspace_id']
        response = self.get(f'orgs/{self.org_id}/workspaces/{workspace_id}')
        return response['workspace']['name']

    def launch_workflow(self, params: dict) -> str:
        url_params = {'workspaceId': self.dataset_config['workspace_id']}

        launch = {
            'launch': {
                'computeEnvId': self.dataset_config['compute_env_id'],
                'pipeline': params['pipeline'],
                'revision': params['commit'],
                'workDir': '/tmp',
            },
        }
        return self.post('workflow/launch', launch, url_params)['workflowId']


def add_seqera_routes(routes: web.RouteTableDef):
    """Add Seqera route to 'routes' flask API."""

    @routes.post('/seqera')
    async def seqera(request: web.Request) -> web.Response:
        """Main seqera submission entry point."""

        params = await request.json()

        seqera = SeqeraApiClient(params['dataset'], params['accessLevel'])
        workflow_id = seqera.launch_workflow(params)

        try:
            where = f' at [{seqera.org_name} / {seqera.workspace_name}] workspace'
            url = f'{seqera.server_url}/orgs/{seqera.org_name}/workspaces/{seqera.workspace_name}/watch/{workflow_id}'
        except (requests.HTTPError, KeyError):
            where = ''
            url = '[URL unavailable]'

        return web.Response(text=f'Workflow {workflow_id} submitted{where}.\n{url}\n')
