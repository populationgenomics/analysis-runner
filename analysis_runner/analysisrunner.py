"""
CLI options for standard analysis-runner
"""

import os
import json
import argparse
from shutil import which

import requests

from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_path_from_git_root,
)

from analysis_runner.util import (
    add_general_args,
    _perform_version_check,
    confirm_choice,
    SERVER_ENDPOINT,
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
        '--environment-variables',
        required=False,
        help='A dictionary of environment variables',
    )

    parser.add_argument('script', nargs=argparse.REMAINDER, default=[])

    return parser


def run_analysis_runner_from_args(args):
    """Run analysis runner from argparse.parse_arguments"""
    return run_analysis_runner(**vars(args))


def run_analysis_runner(
    dataset,
    output_dir,
    script,
    description,
    access_level,
    commit=None,
    repository=None,
    cwd=None,
    environment_variables=None,
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

    if _cwd == '.':
        _cwd = None

    _environment_variables = None
    if environment_variables:
        _environment_variables = json.loads(environment_variables)

    _token = get_google_identity_token()

    logger.info(f'Submitting {_repository}@{_commit_ref} for dataset "{dataset}"')

    response = requests.post(
        SERVER_ENDPOINT,
        json={
            'dataset': dataset,
            'output': output_dir,
            'repo': _repository,
            'accessLevel': access_level,
            'commit': _commit_ref,
            'script': _script,
            'description': description,
            'cwd': _cwd,
            'environmentVariables': _environment_variables,
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
