import urllib.parse

import requests
from aiohttp import web
from seqera_api import SeqeraApiClient
from util import (
    check_allowed_repos,
    check_branch_contains_commit,
    check_dataset_and_group,
    generate_ar_guid,
    get_and_check_cloud_environment,
    get_email_from_request,
    get_server_config,
    log_submission_to_metamist,
)

from cpg_utils.config import AR_GUID_NAME


def parse_github_url(url_str: str) -> tuple[str, str, str | None, str | None]:
    url = urllib.parse.urlsplit(url_str)
    if url.netloc not in ('github.com', 'raw.githubusercontent.com'):
        raise ValueError('bad host')

    if url.path.count('/') == 2:  # noqa: PLR2004
        owner, repo = url.path.removeprefix('/').split('/')
        return (owner, repo, None, None)

    owner, repo, ref_path = url.path.removeprefix('/').split('/', 2)
    ref_path = ref_path.removeprefix('blob/').removeprefix('refs/heads/')

    # TODO separate ref and path
    return (owner, repo, ref_path, None)


def get_and_check_seqera_commit(params: dict) -> str:
    if 'commit_id' in params and 'revision' in params:
        raise web.HTTPBadRequest(reason='Both commit-id and revision specified')

    commit = params.get('commit_id') or params.get('revision')
    if commit is None:
        raise web.HTTPBadRequest(reason='Either commit-id or revision is required')
    if commit == 'HEAD':
        raise web.HTTPBadRequest(reason='Invalid commit parameter')

    return commit


def sanitise_run_name(name: str) -> str:
    """Ensure the run name conforms to Seqera's rules."""
    if not name[0].isalpha():
        name = f'X{name}'

    name = ''.join(c if c.isalnum() or c in '-_' else '_' for c in name)
    return name[:80]


def add_seqera_routes(routes: web.RouteTableDef):
    """Add Seqera route to 'routes' flask API."""

    @routes.post('/seqera')
    async def seqera(request: web.Request) -> web.Response:
        """Main seqera submission entry point."""
        email = get_email_from_request(request)
        params = await request.json()

        ar_guid = generate_ar_guid()
        cloud_environment = get_and_check_cloud_environment(params)

        dataset = params['dataset']
        access_level = params['access_level']
        is_test = access_level == 'test'

        dataset_config = check_dataset_and_group(
            server_config=get_server_config(),
            environment=cloud_environment,
            dataset=dataset,
            email=email,
        )

        owner, repo, _, path = parse_github_url(params['repository'])
        if path is not None:
            raise ValueError('GitHub repository URL without file path required')

        check_allowed_repos(dataset_config=dataset_config, repo=repo, dataset=dataset)

        commit = get_and_check_seqera_commit(params)
        if not is_test:
            on_main = await check_branch_contains_commit(repo, 'main', commit, owner)
            if not on_main:
                raise web.HTTPBadRequest(reason='Commit is not present on main branch')

        user_name, _ = email.split('@', 1)
        main_script = params.get('main_script')
        params['run_name'] = sanitise_run_name(f'{user_name}-{commit}-{main_script}')

        if 'params' in params:
            params['params'][AR_GUID_NAME] = ar_guid
        else:
            params['params'] = {AR_GUID_NAME: ar_guid}

        seqera = SeqeraApiClient(dataset, access_level)
        workflow_id = seqera.launch_workflow(params)

        metadata = {
            AR_GUID_NAME: ar_guid,
            'name': params['run_name'],
            'dataset': dataset,
            'user': email,
            'accessLevel': access_level,
            'repo': repo,
            'commit': commit,
            'script': main_script,
            'description': params.get('description'),
            'environment': cloud_environment,
            'meta': {'workflow_id': workflow_id},
        }

        try:
            where = f' at [{seqera.org_name} / {seqera.workspace_name}] workspace'
            url = f'{seqera.server_url}/orgs/{seqera.org_name}/workspaces/{seqera.workspace_name}/watch/{workflow_id}'
            metadata['batch_url'] = url
        except (requests.HTTPError, KeyError):
            where = ''
            placeholder = 'URL unavailable'
            url = f'[{placeholder}]'
            metadata['batch_url'] = placeholder.upper()

        await log_submission_to_metamist(metadata)

        return web.Response(text=f'Workflow {workflow_id} submitted{where}.\n{url}\n')
