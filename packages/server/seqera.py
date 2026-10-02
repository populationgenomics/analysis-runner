import urllib.parse

import requests
from aiohttp import web
from seqera_api import SeqeraApiClient
from util import (
    check_allowed_repos,
    check_dataset_and_group,
    get_and_check_cloud_environment,
    get_email_from_request,
    get_server_config,
)


def validate_config_url(url: str, is_test: bool):
    # TODO Check repo/branch/etc permissions
    pass


def parse_github_url(url_str: str) -> tuple[str, str, str | None, str | None]:
    url = urllib.parse.urlsplit(url_str)
    if url.netloc not in ('github.com', 'raw.githubusercontent.com'):
        raise ValueError('bad host')

    if url.path.count('/') == 2:  # noqa: PLR2004
        owner, repo = url.path.removeprefix('/').split('/')
        return (owner, repo, None, None)

    owner, repo, ref_path = url.path.removeprefix('/').split('/', 2)
    ref_path = ref_path.removeprefix('blob/').removeprefix('refs/heads/')

    return (owner, repo, ref_path, None)


def add_seqera_routes(routes: web.RouteTableDef):
    """Add Seqera route to 'routes' flask API."""

    @routes.post('/seqera')
    async def seqera(request: web.Request) -> web.Response:
        """Main seqera submission entry point."""
        params = await request.json()

        dataset = params['dataset']
        dataset_config = check_dataset_and_group(
            server_config=get_server_config(),
            environment=get_and_check_cloud_environment(params),
            dataset=dataset,
            email=get_email_from_request(request),
        )

        _, repo, _, path = parse_github_url(params['repository'])
        if path is not None:
            raise ValueError('GitHub repository URL without file path required')

        check_allowed_repos(dataset_config=dataset_config, repo=repo, dataset=dataset)

        seqera = SeqeraApiClient(dataset, params['access_level'])
        workflow_id = seqera.launch_workflow(params)

        try:
            where = f' at [{seqera.org_name} / {seqera.workspace_name}] workspace'
            url = f'{seqera.server_url}/orgs/{seqera.org_name}/workspaces/{seqera.workspace_name}/watch/{workflow_id}'
        except (requests.HTTPError, KeyError):
            where = ''
            url = '[URL unavailable]'

        return web.Response(text=f'Workflow {workflow_id} submitted{where}.\n{url}\n')
