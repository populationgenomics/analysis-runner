# Also used by the Seqera config resolver job, which runs without Hail: don't import util.py.
import json
import os

import requests
from aiohttp import web
from cachetools.func import lru_cache, ttl_cache

from cpg_utils.cloud import read_secret

ANALYSIS_RUNNER_PROJECT_ID = os.getenv('ANALYSIS_RUNNER_PROJECT_ID', 'analysis-runner')

# configPath of a Seqera run's metamist log until the config resolver saves its resolved config
SEQERA_PENDING = 'SEQERA-PENDING'

# batch_url of a Seqera run's metamist log if its URL couldn't be built at launch
BATCH_URL_UNAVAILABLE = 'URL UNAVAILABLE'

SEQERA_KEYS = {
    'commit_id': 'commitId',
    'config_text': 'configText',
    'main_script': 'mainScript',
    'repository': 'pipeline',
    'revision': 'revision',
    'run_name': 'runName',
}


# cache the result for 600 seconds, so we can call this function multiple times
@ttl_cache(maxsize=1, ttl=600)
def get_seqera_config() -> dict:
    config = os.getenv('SEQERA_PLATFORM_CONFIG')
    if config is None:
        config = read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'seqera-platform-config')
    if config is None:
        raise web.HTTPInternalServerError(
            reason='Failed to read seqera-platform-config secret'
        )

    return json.loads(config)


def read_ar_secret(secret_name: str) -> str:
    """Read a secret stored in the analysis-runner project."""
    value = read_secret(ANALYSIS_RUNNER_PROJECT_ID, secret_name)
    if value is None:
        raise web.HTTPInternalServerError(
            reason='Failed to read analysis-runner secret'
        )
    return value


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

    def workflow_url(self, workflow_id: str) -> str:
        return (
            f'{self.server_url}/orgs/{self.org_name}'
            f'/workspaces/{self.workspace_name}/watch/{workflow_id}'
        )

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

        if 'params' in params:
            launch['launch']['paramsText'] = json.dumps(params['params'])

        return self.post('workflow/launch', launch, self.workspace_param)['workflowId']
