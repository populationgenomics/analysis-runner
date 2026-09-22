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
        required=True,
        help='The name of the repository where the pipeline to run resides.',
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
        default='main.nf',
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
        '--use-test-server',
        action='store_true',
        help='Use the test analysis-runner server',
    )
    parser.add_argument(
        '--server-url',
        required=False,
        default=SERVER_ENDPOINT,
        help='Supply a server URL to use, this will override the "--use-test-server"',
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


def run_seqera(
    dataset: str,
    access_level: str,
    repository: str,
    revision: str | None = None,
    commit_id: str | None = None,
    main_script: str = 'main.nf',
    params_file: str | None = None,
    config: str | None = None,
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

    # Default to "main" if no revision specified and no exact commit is pinned
    if not revision and not commit_id:
        revision = 'main'

    server_args: dict[str, Any] = {
        'dataset': dataset,
        'access_level': access_level,
        'main_script': main_script,
        'repository': repository,
    }

    if revision:
        server_args['revision'] = revision
    if commit_id:
        server_args['commit_id'] = commit_id

    if params_file:
        server_args['params'] = _read_params(params_file)

    if config:
        if config.startswith(('http://', 'https://')):
            # A github URL, this is further validated on the server side to ensure
            # the file is in an appropriate repo on an appropriate branch
            server_args['config_url'] = config
        elif access_level == 'test':
            # Test access level can use a local config file
            with open(config) as f:
                server_args['config_text'] = f.read()
        else:
            raise SystemExit(
                'For standard/full access, --config must be a github URL to a config '
                'file on the main branch of an allow-listed repository.',
            )

    logger.info(
        f'Submitting Nextflow workflow {repository}@{commit_id or revision} '
        f'for dataset "{dataset}"',
    )

    server_endpoint = get_server_endpoint(
        server_url=server_url, is_test=use_test_server
    )
    endpoint = server_endpoint.rstrip('/') + '/seqera'
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
