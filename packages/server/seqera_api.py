import requests
from aiohttp import web
from cachetools.func import lru_cache
from util import get_seqera_config, read_ar_secret

SEQERA_KEYS = {
    'commit_id': 'commitId',
    'config_text': 'configText',
    'main_script': 'mainScript',
    'params': 'paramsText',
    'repository': 'pipeline',
    'revision': 'revision',
}


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
        response = requests.get(url, headers=headers, params=params, timeout=60)

        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, body: dict, params: dict | None = None) -> dict:
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
        }
        url = f'{self.api_url}/{endpoint}'
        response = requests.post(
            url, headers=headers, params=params, json=body, timeout=60
        )

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

    @property
    def workspace_param(self) -> dict:
        return {'workspaceId': self.dataset_config['workspace_id']}

    def compute_environment(self, cenv_id: int) -> dict:
        response = self.get(f'compute-envs/{cenv_id}', params=self.workspace_param)
        return response['computeEnv']

    def describe_workflow(self, workflow_id: str) -> dict:
        response = self.get(f'workflow/{workflow_id}', params=self.workspace_param)
        return response['workflow']

    def launch_workflow(self, params: dict) -> str:
        compute_env = self.compute_environment(self.dataset_config['compute_env_id'])

        launch = {
            'launch': {
                'computeEnvId': compute_env['id'],
                'workDir': compute_env['config']['workDir'],
            }
        }

        for ar_key, seqera_key in SEQERA_KEYS.items():
            if ar_key in params:
                launch['launch'][seqera_key] = params[ar_key]

        return self.post('workflow/launch', launch, self.workspace_param)['workflowId']
