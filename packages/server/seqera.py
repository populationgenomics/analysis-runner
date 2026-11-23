import json
import logging
import re
import urllib.parse
from typing import Any

import requests
from aiohttp import web
from seqera_api import CONFIG_PENDING, SEQERA_STATUS_KEY, SeqeraApiClient
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
from metamist.graphql import gql, query_async


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


SYNTAX_PARSERS = ('v1', 'v2')
DEFAULT_SYNTAX_PARSER = 'v2'


def apply_seqera_launch_options(params: dict) -> None:
    """Default the syntax parser to v2, then validate it."""
    # Platform would otherwise run the v1 parser, whatever the Nextflow version.
    params.setdefault('syntax_parser', DEFAULT_SYNTAX_PARSER)

    if params['syntax_parser'] not in SYNTAX_PARSERS:
        raise web.HTTPBadRequest(reason='syntax_parser must be one of v1, v2')


def sanitise_run_name(name: str) -> str:
    """Ensure the run name conforms to Seqera's rules."""
    if not name[0].isalpha():
        name = f'X{name}'

    name = ''.join(c if c.isalnum() or c in '-_' else '_' for c in name)
    return name[:80]


async def read_seqera_request(request: web.Request) -> tuple[str, dict, str, dict]:
    email = get_email_from_request(request)
    params = await request.json()

    cloud_environment = get_and_check_cloud_environment(params)
    dataset_config = check_dataset_and_group(
        server_config=get_server_config(),
        environment=cloud_environment,
        dataset=params['dataset'],
        email=email,
    )
    return email, params, cloud_environment, dataset_config


async def check_commit_on_main(owner: str, repo: str, commit: str, is_test: bool):
    """Unless at test, the commit must be on the main branch."""
    if not is_test:
        on_main = await check_branch_contains_commit(repo, 'main', commit, owner)
        if not on_main:
            raise web.HTTPBadRequest(reason='Commit is not present on main branch')


URL_UNAVAILABLE = 'URL unavailable'


def workflow_location(
    seqera: SeqeraApiClient, workflow_id: str
) -> tuple[str, str | None]:
    try:
        where = f' at [{seqera.org_name} / {seqera.workspace_name}] workspace'
        return where, seqera.workflow_url(workflow_id)
    except (requests.HTTPError, KeyError):
        return '', None


async def log_submission(
    *,
    ar_guid: str,
    run_name: str,
    params: dict,
    email: str,
    environment: str,
    repo: str,
    commit: str,
    script: str | None,
    meta: dict,
    url: str | None,
) -> None:
    """Record the submission in metamist."""
    metadata = {
        AR_GUID_NAME: ar_guid,
        'name': run_name,
        'dataset': params['dataset'],
        'user': email,
        'accessLevel': params['access_level'],
        'repo': repo,
        'commit': commit,
        'script': script,
        'description': params.get('description'),
        'environment': environment,
        'meta': meta,
    }
    metadata['batch_url'] = url or URL_UNAVAILABLE.upper()

    await log_submission_to_metamist(metadata)


SOURCE_RUN_STATUSES = ('FAILED', 'CANCELLED')


def get_source_ar_guid(source_launch: dict) -> str:
    """The ar_guid the analysis-runner injected into the source run's params."""
    try:
        source_params = json.loads(source_launch.get('paramsText') or '{}')
    except json.JSONDecodeError:
        source_params = None

    ar_guid = (
        source_params.get(AR_GUID_NAME) if isinstance(source_params, dict) else None
    )
    if not ar_guid:
        raise web.HTTPBadRequest(
            reason='The source run was not launched by the analysis-runner'
        )
    return ar_guid


def get_source_launch(
    seqera: SeqeraApiClient, params: dict, dataset_config: dict
) -> tuple[dict, str]:

    source_id = params['workflow_id']
    source_launch = seqera.describe_workflow_launch(source_id)

    # paramsText may contain dataset and access_level fields but this is not
    # guaranteed. Therefore, computeEnv is compared.
    source_compute_env = source_launch.get('computeEnv') or {}
    if str(source_compute_env.get('id')) != str(dataset_config['compute_env_id']):
        raise web.HTTPBadRequest(
            reason=f'Run {source_id} does not belong to dataset {params["dataset"]} '
            f'at {params["access_level"]} access level'
        )

    return source_launch, get_source_ar_guid(source_launch)


async def check_source_pipeline(
    pipeline: str, commit: str, params: dict, dataset_config: dict
) -> str:
    """
    Revalidate the original run's pipeline
    """
    owner, repo, _, _ = parse_github_url(pipeline)
    check_allowed_repos(
        dataset_config=dataset_config, repo=repo, dataset=params['dataset']
    )
    await check_commit_on_main(owner, repo, commit, params['access_level'] == 'test')
    return repo


def submit_from_source_launch(
    seqera: SeqeraApiClient,
    source_launch: dict,
    params: dict,
    resume: bool,
    ar_guid: str,
    commit: str,
    work_dir: str,
    run_name: str,
) -> str:
    # A new params file replaces the source params as a whole.
    if 'params' in params:
        run_params = params['params']
    else:
        run_params = json.loads(source_launch.get('paramsText') or '{}')
    run_params[AR_GUID_NAME] = ar_guid

    launch = {
        **source_launch,
        'workDir': work_dir,
        'revision': commit,
        'resume': resume,
        'runName': run_name,
        'paramsText': json.dumps(run_params),
        'configText': params.get('config_text', source_launch.get('configText')),
    }
    if not resume:
        # A relaunch starts a new session.
        launch.pop('sessionId', None)

    return seqera.submit_launch(
        {key: value for key, value in launch.items() if value is not None}
    )


async def resume_run(
    params: dict, seqera: SeqeraApiClient, dataset_config: dict
) -> dict[str, Any]:
    """
    Resume the original run in its own session, under the same ar_guid and work
    dir, so cached tasks are reused.
    """
    source_id = params['workflow_id']
    source_launch, ar_guid = get_source_launch(seqera, params, dataset_config)

    # The original run is one of its session's runs, so it is taken from that list
    session_id = source_launch['sessionId']
    workflows_of_session_id = seqera.list_workflows(f'sessionId:{session_id}')
    source_workflow = next(
        (w for w in workflows_of_session_id if w['id'] == source_id), None
    ) or seqera.describe_workflow(source_id)

    if source_workflow.get('status') not in SOURCE_RUN_STATUSES:
        raise web.HTTPBadRequest(
            reason=f'Only runs with status {", ".join(SOURCE_RUN_STATUSES)} '
            f'can be resumed.',
        )

    # resumeCommitId makes the new run use the same commit as the original run.
    # If it is empty, the original run possibly failed before the
    # previous workflow started.
    commit = source_launch.get('resumeCommitId')
    if not commit:
        raise web.HTTPBadRequest(
            reason=f'Run {source_id} has nothing to resume, relaunch or resubmit it instead.'
        )

    await check_source_pipeline(source_launch['pipeline'], commit, params, dataset_config)

    run_name = source_workflow['runName']
    if source_workflow.get('resume'):
        # The original run was itself a resume: drop its version.
        # Based on {user_name}-{commit}-{main_script} format
        run_name = re.sub(r'-\d+$', '', run_name)
    run_name = f'{run_name}-{len(workflows_of_session_id) + 1}'

    workflow_id = submit_from_source_launch(
        seqera,
        source_launch,
        params,
        resume=True,
        ar_guid=ar_guid,
        commit=commit,
        work_dir=source_launch['resumeDir'],
        run_name=run_name,
    )

    # The runs sharing this run's session, this one last.
    session_workflow_ids = [workflow['id'] for workflow in workflows_of_session_id]
    session_workflow_ids.append(workflow_id)

    return {
        'workflow_id': workflow_id,
        AR_GUID_NAME: ar_guid,
        'session_workflow_ids': session_workflow_ids,
    }


async def relaunch_run(
    params: dict, seqera: SeqeraApiClient, dataset_config: dict, email: str
) -> dict[str, Any]:
    """
    Relaunch the original run from scratch as an independent run, under a new
    ar_guid, work dir and session.
    """
    source_launch, _ = get_source_launch(seqera, params, dataset_config)

    # The original commit, or fallback to revision (a branch, tag, or commit).
    commit = source_launch.get('resumeCommitId') or source_launch['revision']
    repo = await check_source_pipeline(source_launch, commit, params, dataset_config)

    ar_guid = generate_ar_guid()

    # Named like a new launch.
    user_name, _ = email.split('@', 1)
    main_script = source_launch.get('mainScript') or 'main.nf'
    run_name = sanitise_run_name(f'{user_name}-{commit}-{main_script}')

    workflow_id = submit_from_source_launch(
        seqera,
        source_launch,
        params,
        resume=False,
        ar_guid=ar_guid,
        commit=commit,
        work_dir=seqera.run_work_dir(ar_guid),
        run_name=run_name,
    )

    return {
        'workflow_id': workflow_id,
        AR_GUID_NAME: ar_guid,
        'run_name': run_name,
        'repo': repo,
        'commit': commit,
        'main_script': main_script,
    }


UPDATE_SUBMISSION_MUTATION = gql(
    """
    mutation UpdateAnalysisRunnerEntry(
        $arGuid: String!, $update: AnalysisRunnerUpdateInput!
    ) {
        analysisRunner {
            updateAnalysisRunnerLog(arGuid: $arGuid, arUpdateInput: $update) {
                arGuid
            }
        }
    }
    """
)


async def log_resume_to_metamist(
    ar_guid: str,
    workflow_id: str,
    session_workflow_ids: list[str],
    email: str,
    url: str | None,
) -> None:
    update: dict[str, Any] = {
        # The scheduled job will set the config path
        'clearConfigPath': True,
        'batchUrl': url or URL_UNAVAILABLE.upper(),
        'submittingUser': email,
        'meta': {
            'workflow_id': workflow_id,
            SEQERA_STATUS_KEY: CONFIG_PENDING,
            # Other the workflows sharing the same session-id (in other words resumed under
            # same arguid)
            'session_workflow_ids': session_workflow_ids,
        },
    }

    try:
        await query_async(
            UPDATE_SUBMISSION_MUTATION, {'arGuid': ar_guid, 'update': update}
        )
    except Exception:
        # The run is already launched, so don't fail the request.
        logging.exception(
            f'Failed to update the metamist entry for {ar_guid} with resumed '
            f'workflow {workflow_id}',
        )
        return

    logging.info(f'Updated the metamist entry for {ar_guid}: resumed as {workflow_id}')


def add_seqera_routes(routes: web.RouteTableDef):
    """Add Seqera routes to 'routes' flask API."""

    @routes.post('/seqera')
    async def seqera(request: web.Request) -> web.Response:
        """Main seqera submission entry point."""
        email, params, cloud_environment, dataset_config = await read_seqera_request(
            request
        )
        ar_guid = generate_ar_guid()

        dataset = params['dataset']
        access_level = params['access_level']
        is_test = access_level == 'test'

        owner, repo, _, path = parse_github_url(params['repository'])
        if path is not None:
            raise ValueError('GitHub repository URL without file path required')

        check_allowed_repos(dataset_config=dataset_config, repo=repo, dataset=dataset)

        commit = get_and_check_seqera_commit(params)
        apply_seqera_launch_options(params)
        await check_commit_on_main(owner, repo, commit, is_test)

        user_name, _ = email.split('@', 1)
        main_script = params.get('main_script')
        params['run_name'] = sanitise_run_name(f'{user_name}-{commit}-{main_script}')

        if 'params' in params:
            params['params'][AR_GUID_NAME] = ar_guid
        else:
            params['params'] = {AR_GUID_NAME: ar_guid}

        seqera = SeqeraApiClient(dataset, access_level)
        workflow_id = seqera.launch_workflow(params, ar_guid)

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

        where, url = workflow_location(seqera, workflow_id)
        if url:
            metadata['batch_url'] = url
        else:
            placeholder = 'URL unavailable'
            url = f'[{placeholder}]'
            metadata['batch_url'] = placeholder.upper()

        await log_submission_to_metamist(metadata)

        return web.Response(text=f'Workflow {workflow_id} submitted{where}.\n{url}\n')

    @routes.post('/seqera/resume')
    async def seqera_resume(request: web.Request) -> web.Response:
        """Resume an original run."""
        email, params, _, dataset_config = await read_seqera_request(request)

        seqera = SeqeraApiClient(params['dataset'], params['access_level'])

        submission_info = await resume_run(params, seqera, dataset_config)
        workflow_id = submission_info['workflow_id']
        where, url = workflow_location(seqera, workflow_id)

        if url is None:
            placeholder = 'URL unavailable'
            url = f'[{placeholder}]'

        # The ar_guid is kept, so update its existing metamist entry.
        await log_resume_to_metamist(
            ar_guid=submission_info[AR_GUID_NAME],
            workflow_id=workflow_id,
            session_workflow_ids=submission_info['session_workflow_ids'],
            email=email,
            url=url,
        )

        return web.Response(text=f'Workflow {workflow_id} resumed{where}.\n{url}\n')

    @routes.post('/seqera/relaunch')
    async def seqera_relaunch(request: web.Request) -> web.Response:
        """Relaunch an original run from scratch under a new ar_guid."""
        email, params, cloud_environment, dataset_config = await read_seqera_request(
            request
        )

        seqera = SeqeraApiClient(params['dataset'], params['access_level'])
        submission_info = await relaunch_run(params, seqera, dataset_config, email)
        workflow_id = submission_info['workflow_id']

        metadata = {
            AR_GUID_NAME: submission_info[AR_GUID_NAME],
            'name': submission_info['run_name'],
            'dataset': params['dataset'],
            'user': email,
            'accessLevel': params['access_level'],
            'repo': submission_info['repo'],
            'commit': submission_info['commit'],
            'script': submission_info['main_script'],
            'description': params.get('description'),
            'environment': cloud_environment,
            'meta': {'workflow_id': workflow_id, SEQERA_STATUS_KEY: CONFIG_PENDING},
        }

        where, url = workflow_location(seqera, workflow_id)
        if url:
            metadata['batch_url'] = url
        else:
            placeholder = 'URL unavailable'
            url = f'[{placeholder}]'
            metadata['batch_url'] = placeholder.upper()

        await log_submission_to_metamist(metadata)

        return web.Response(text=f'Workflow {workflow_id} relaunched{where}.\n{url}\n')
