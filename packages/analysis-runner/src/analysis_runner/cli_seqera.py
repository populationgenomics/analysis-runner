"""
CLI options for launching Nextflow workflows on the Seqera platform
"""

import argparse
import sys
from typing import Any

import requests
import yaml

from analysis_runner.util import (
    SERVER_ENDPOINT,
    _perform_version_check,
    confirm_choice,
    get_server_endpoint,
    logger,
)
from cpg_utils.cloud import get_google_identity_token


def add_seqera_args(
    parser: argparse.ArgumentParser | None = None,
) -> argparse.ArgumentParser:
    """
    Add CLI arguments for launching Nextflow workflows on Seqera.

    Flag names more or less mirror the Seqera Platform CLI (``tw launch``)
    """
    if not parser:
        parser = argparse.ArgumentParser('seqera analysis-runner')

    parser.add_argument(
        '--dataset',
        required=True,
        type=str,
        help='The dataset name, determines what data the run should have access to.',
    )
    parser.add_argument(
        '--access-level',
        choices=(['test', 'standard', 'full']),
        default='test',
        help='Which permissions level to grant when running the job.',
    )

    parser.add_argument(
        '--repository',
        '--repo',
        required=False,
        help='The name of the repository where the pipeline to run resides. '
        'Required unless --resume or --relaunch is given.',
    )

    parser.add_argument(
        '--revision',
        required=False,
        help='The git branch or tag to use. Defaults to "main" unless --commit-id '
        'is given.',
    )
    parser.add_argument(
        '--commit-id',
        required=False,
        help='Optionally pin the pipeline execution to a specific Git commit hash.',
    )

    parser.add_argument(
        '--main-script',
        required=False,
        help='The Nextflow entry script to run. Defaults to "main.nf".',
    )

    parser.add_argument(
        '--params-file',
        required=False,
        help='Path to a params file (YAML or JSON), forwarded to seqera as `paramsText`',
    )

    parser.add_argument(
        '--config',
        required=False,
        help=(
            'A full Nextflow config file to apply to the run, given as a github URL '
            '(github.com/... or raw.githubusercontent.com/...). For standard / full '
            'access the URL must point to a config on the main branch of an '
            'allow-listed repository. Test access is less restricted: the URL may '
            'reference any branch, or a local file path may be supplied instead.'
            'Specifying a config will override pipeline config files.'
        ),
    )

    parser.add_argument(
        '--syntax-parser',
        choices=['v1', 'v2'],
        required=False,
        help=(
            'The Nextflow syntax parser to run the pipeline with. Defaults to v2; '
            'use v1 for pipelines that still rely on the legacy syntax.'
        ),
    )

    source_run = parser.add_mutually_exclusive_group()
    source_run.add_argument(
        '--resume',
        metavar='WORKFLOW_ID',
        required=False,
        help=(
            'Resume an earlier run, given its Seqera workflow id. The run keeps its '
            'AR GUID and work directory. Cached tasks whose inputs did not change will be reused. '
            '--repository, --revision, --commit-id, '
            '--main-script and --syntax-parser come from the original run; only --params-file and '
            '--config can be changed.'
        ),
    )
    source_run.add_argument(
        '--relaunch',
        metavar='WORKFLOW_ID',
        required=False,
        help=(
            'Relaunch an earlier run from scratch, given its Seqera workflow id. It '
            'gets a new AR GUID and work directory and reuses no cached tasks. '
            '--repository, --revision, --commit-id, --main-script and --syntax-parser '
            'come from the original run; only --params-file and --config can be '
            'changed.'
        ),
    )

    parser.add_argument(
        '--use-test-server',
        action='store_true',
        help='Use the test analysis-runner server, this overrides --server-url',
    )
    parser.add_argument(
        '--server-url',
        required=False,
        default=SERVER_ENDPOINT,
        help='Supply a server URL to use"',
    )

    return parser


def run_seqera_from_args(args: argparse.ArgumentParser):
    """Run seqera nextflow submission from argparse.parse_arguments"""
    return run_seqera(**vars(args))


def _read_params(params: str) -> dict:
    """Read a params file (YAML or JSON; "-" for stdin) into a dict."""
    if params == '-':
        content = sys.stdin.read()
    else:
        with open(params) as f:
            content = f.read()

    # JSON is a subset of YAML, so safe_load handles both formats.
    parsed = yaml.safe_load(content)
    if not isinstance(parsed, dict):
        raise ValueError('The params file must contain a top-level mapping')
    return parsed


def _resolve_configs(config: str, access_level: str) -> dict[str, str]:
    if config.startswith(('http://', 'https://')):
        # A github URL, this is further validated on the server side to ensure
        # the file is in an appropriate repo on an appropriate branch
        return {'config_url': config}
    if access_level == 'test':
        # Test access level can use a local config file
        with open(config) as f:
            return {'config_text': f.read()}
    raise SystemExit(
        'For standard/full access, --config must be a github URL to a config '
        'file on the main branch of an allow-listed repository.',
    )


def _post_to_server(
    path: str,
    server_args: dict[str, Any],
    server_url: str | None,
    use_test_server: bool,
) -> None:
    """POST the request to the analysis-runner server and log the outcome."""
    server_endpoint = get_server_endpoint(
        server_url=server_url, is_test=use_test_server
    )
    endpoint = f'{server_endpoint.rstrip("/")}/{path}'
    _token = get_google_identity_token(server_endpoint)

    response = requests.post(
        endpoint,
        json=server_args,
        headers={'Authorization': f'Bearer {_token}'},
        timeout=60,
    )
    try:
        response.raise_for_status()
        logger.info(f'Request submitted successfully: {response.text}')
    except requests.HTTPError as e:
        logger.critical(
            f'Request failed with status {response.status_code}: {e!s}\n'
            f'Full response: {response.text}',
        )


def run_seqera(
    dataset: str,
    access_level: str,
    repository: str | None = None,
    revision: str | None = None,
    commit_id: str | None = None,
    main_script: str | None = None,
    params_file: str | None = None,
    config: str | None = None,
    syntax_parser: str | None = None,
    resume: str | None = None,
    relaunch: str | None = None,
    use_test_server: bool = False,
    server_url: str | None = None,
) -> None:
    """
    Prepare parameters and submit a Nextflow workflow to the analysis-runner.
    """
    _perform_version_check()

    if access_level == 'full' and not confirm_choice(
        'Full access increases the risk of accidental data loss. Continue?',
    ):
        raise SystemExit

    if resume or relaunch:
        if any([repository, revision, commit_id, main_script, syntax_parser]):
            raise SystemExit(
                'With --resume or --relaunch, only --params-file and --config can be provided.',
            )
        run_from_source_run(
            resume=bool(resume),
            dataset=dataset,
            access_level=access_level,
            workflow_id=resume or relaunch,
            params_file=params_file,
            config=config,
            use_test_server=use_test_server,
            server_url=server_url,
        )
        return

    if not repository:
        raise SystemExit('--repository is required unless --resume or --relaunch')

    # Default to "main" if no revision specified and no exact commit is pinned
    if not revision and not commit_id:
        revision = 'main'

    server_args: dict[str, Any] = {
        'dataset': dataset,
        'access_level': access_level,
        'main_script': main_script or 'main.nf',
        'repository': repository,
    }

    # Only options that were given: the server leaves absent ones to Seqera's defaults.
    optional_args = {
        'revision': revision,
        'commit_id': commit_id,
        'syntax_parser': syntax_parser,
    }
    server_args.update({key: value for key, value in optional_args.items() if value})

    if params_file:
        server_args['params'] = _read_params(params_file)

    if config:
        server_args.update(_resolve_configs(config, access_level))

    logger.info(
        f'Submitting Nextflow workflow {repository}@{commit_id or revision} '
        f'for dataset "{dataset}"',
    )

    _post_to_server('seqera', server_args, server_url, use_test_server)


# Resume / relaunch an earlier Seqera run: `seqera --resume|--relaunch <workflow_id>`

RESUME_WARNING = (
    'Resuming keeps the AR GUID and work directory of the original run. During a '
    'resumed workflow, cached tasks whose inputs did not change will be reused. Continue?'
)
RELAUNCH_WARNING = (
    'Relaunching runs the pipeline from scratch under a new AR GUID and work '
    'directory. No cached tasks are reused. Continue?'
)


def run_from_source_run(
    resume: bool,
    dataset: str,
    access_level: str,
    workflow_id: str,
    params_file: str | None = None,
    config: str | None = None,
    use_test_server: bool = False,
    server_url: str | None = None,
) -> None:
    """Resume or relaunch an earlier Seqera run."""

    if not confirm_choice(RESUME_WARNING if resume else RELAUNCH_WARNING):
        raise SystemExit

    server_args: dict[str, Any] = {
        'dataset': dataset,
        'access_level': access_level,
        'workflow_id': workflow_id,
    }

    if params_file:
        server_args['params'] = _read_params(params_file)

    if config:
        server_args.update(_resolve_configs(config, access_level))

    action = 'resume' if resume else 'relaunch'
    logger.info(f'Requesting {action} of Seqera workflow {workflow_id} for "{dataset}"')

    _post_to_server(f'seqera/{action}', server_args, server_url, use_test_server)
