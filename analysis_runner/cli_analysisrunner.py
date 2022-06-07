"""
CLI options for standard analysis-runner
"""

import os
import argparse
from shutil import which
from typing import List

import requests
from cpg_utils.config import read_configs
from analysis_runner.constants import get_server_endpoint
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_path_from_git_root,
    check_if_commit_is_on_remote,
)
from analysis_runner.util import (
    add_general_args,
    _perform_version_check,
    confirm_choice,
    logger,
    get_google_identity_token,
)


def add_analysis_runner_args(parser=None) -> argparse.ArgumentParser:
    """
    Add CLI arguments for standard analysis-runner
    """
    if not parser:
        parser = argparse.ArgumentParser('analysis-runner subparser')

    add_general_args(parser)

    parser.add_argument(
        '--image',
        help=(
            'Image name, if using standard / full access levels, this must start with '
            'australia-southeast1-docker.pkg.dev/cpg-common/'
        ),
    )
    parser.add_argument(
        '--cpu',
        help=(
            'Number of CPUs to request. This follows the hail batch convention: '
            'https://hail.is/docs/batch/api/batch/hailtop.batch.job.Job.html#hailtop.batch.job.Job.cpu'
        ),
    )
    parser.add_argument(
        '--memory',
        help=(
            'Amount of memory to request in GB (eg: 4G). This follows the hail batch convention: '
            'https://hail.is/docs/batch/api/batch/hailtop.batch.job.Job.html#hailtop.batch.job.Job.memory'
        ),
    )

    parser.add_argument(
        '-e',
        '--env',
        required=False,
        help='Environment variables e.g. -e SM_ENVIRONMENT=production -e OTHERVAR=value',
        action='append',
    )

    parser.add_argument(
        '--config',
        required=False,
        help=(
            'Paths to a configurations in TOML format, which will be merged from left '
            'to right order (cloudpathlib.AnyPath-compatible paths are supported). '
            'The analysis-runner will add the default environment-related options to '
            'this dictionary and make it available to the batch.'
        ),
        action='append',
    )

    parser.add_argument('script', nargs=argparse.REMAINDER, default=[])

    return parser


def run_analysis_runner_from_args(args):
    """Run analysis runner from argparse.parse_arguments"""
    return run_analysis_runner(**vars(args))


def run_analysis_runner(  # pylint: disable=too-many-arguments
    dataset,
    output_dir,
    script,
    description,
    access_level,
    commit=None,
    repository=None,
    cwd=None,
    image=None,
    cpu=None,
    memory=None,
    config: List[str] = None,
    env: List[str] = None,
    use_test_server=False,
):
    """
    Main function that drives the CLI.
    """

    if repository is not None and commit is None:
        raise Exception(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'"
        )

    _perform_version_check()

    if access_level == 'full':
        if not confirm_choice(
            'Full access increases the risk of accidental data loss. Continue?',
        ):
            raise SystemExit()

    _repository = repository
    _commit_ref = commit
    _script = list(script)
    _cwd = cwd

    # false-y value catches empty list / tuple as well
    if not _script:
        _script = ['main.py']

    executable_path = os.path.join(_cwd or '', _script[0])

    # we can find the script, and it's a relative path (not absolute)
    if os.path.exists(executable_path) and not executable_path.startswith('/'):
        _perform_shebang_check(executable_path)
        # if it's just the path name, eg: you call
        #   analysis-runner my_file.py
        # need to pre-pend "./" to execute
        if os.path.basename(_script[0]) == _script[0]:
            _script[0] = './' + _script[0]
    elif not (which(_script[0]) or which(executable_path)):
        # the first el of _script is not executable
        # (at least on this computer)
        if not confirm_choice(
            f"The program '{executable_path}' was not executable \n"
            f'(or a script could not be found) on this computer. \n'
            f'Please confirm to continue.'
        ):
            raise SystemExit()

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        if _cwd is None:
            _cwd = get_relative_path_from_git_root()

        if not check_if_commit_is_on_remote(_commit_ref):
            if not confirm_choice(
                f'The commit "{_commit_ref}" was not found on GitHub '
                '(Did you forget to push your latest commit?) \n'
                'Please confirm if you want to proceed anyway.'
            ):
                raise SystemExit()

    if _cwd == '.':
        _cwd = None

    _env = None
    if env:
        _env = {}
        for env_var_pair in env:
            try:
                pair = env_var_pair.split('=', maxsplit=1)
                _env[pair[0]] = pair[1]
            except IndexError as e:
                raise IndexError(
                    env_var_pair + ' does not conform to key=value format.'
                ) from e

    _config = None
    if config:
        _config = read_configs(config)

    _token = get_google_identity_token()

    logger.info(f'Submitting {_repository}@{_commit_ref} for dataset "{dataset}"')

    response = requests.post(
        get_server_endpoint(is_test=use_test_server),
        json={
            'dataset': dataset,
            'output': output_dir,
            'repo': _repository,
            'accessLevel': access_level,
            'commit': _commit_ref,
            'script': _script,
            'description': description,
            'cwd': _cwd,
            'image': image,
            'cpu': cpu,
            'memory': memory,
            'environmentVariables': _env,
            'config': _config,
        },
        headers={'Authorization': f'Bearer {_token}'},
    )
    try:
        response.raise_for_status()
        logger.info(f'Request submitted successfully: {response.text}')
    except requests.HTTPError as e:
        logger.critical(
            f'Request failed with status {response.status_code}: {str(e)}\n'
            f'Full response: {response.text}',
        )


def _perform_shebang_check(script):
    """
    Returns None if script has shebang, otherwise raises Exception
    """
    with open(script, encoding='utf-8') as f:
        potential_shebang = f.readline()
        if potential_shebang.startswith('#!'):
            return

        suggestion_shebang = ''
        if script.endswith('.py'):
            suggestion_shebang = '#!/usr/bin/env python3'
        elif script.endswith('.sh'):
            suggestion_shebang = '#!/usr/bin/env bash'
        elif script.lower().endswith('.r') or script.lower().endswith('.rscript'):
            suggestion_shebang = '#!/usr/bin/env Rscript'

        message = f'Couldn\'t find shebang at start of "{script}"'
        if suggestion_shebang:
            message += (
                f', consider inserting "{suggestion_shebang}" at the top of this file'
            )
        raise Exception(message)
