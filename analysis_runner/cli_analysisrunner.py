"""
CLI options for standard analysis-runner
"""

import argparse
import dataclasses
import os
from shutil import which
from typing import Any

import requests

from analysis_runner.util import (
    _perform_version_check,
    add_general_args,
    confirm_choice,
    get_server_endpoint,
    logger,
)
from cpg_utils.cloud import get_google_identity_token
from cpg_utils.config import read_configs
from cpg_utils.git import (
    check_if_commit_is_on_remote,
    get_git_branch_name,
    get_git_commit_ref_of_current_repository,
    get_git_default_remote,
    get_relative_path_from_git_root,
    get_repo_name_from_remote,
)


def add_analysis_runner_args(
    parser: argparse.ArgumentParser | None = None,
) -> argparse.ArgumentParser:
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
        '--storage',
        help=(
            'Amount of storage to request in GB (eg: 4G). This follows the hail batch convention: '
            'https://hail.is/docs/batch/api/batch/hailtop.batch.job.Job.html#hailtop.batch.job.Job.storage'
        ),
    )

    parser.add_argument(
        '--preemptible',
        required=False,
        action=argparse.BooleanOptionalAction,
        default=True,
        help='Whether to use a preemptible machine or not.',
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

    parser.add_argument(
        '--skip-repo-checkout',
        required=False,
        action='store_true',
    )

    parser.add_argument('script', nargs=argparse.REMAINDER, default=[])

    return parser


def run_analysis_runner_from_args(args: argparse.ArgumentParser):
    """Run analysis runner from argparse.parse_arguments"""
    return run_analysis_runner(**vars(args))


@dataclasses.dataclass
class RepositorySpecificInformation:
    repository: str
    commit: str
    cwd: str | None
    branch: str | None
    script: list[str]


def run_analysis_runner(
    dataset: str,
    output_dir: str,
    script: list[str],
    description: str,
    access_level: str,
    commit: str | None = None,
    repository: str | None = None,
    cwd: str | None = None,
    image: str | None = None,
    cpu: str | None = None,
    memory: str | None = None,
    storage: str | None = None,
    preemptible: str | None = None,
    branch: str | None = None,
    config: list[str] | None = None,
    env: list[str] | None = None,
    use_test_server: bool = False,
    server_url: str | None = None,
    skip_repo_checkout: bool = False,
):
    """
    Main function that drives the CLI.
    """
    if not script:
        raise ValueError('No script provided')

    _perform_version_check()

    if access_level == 'full' and not confirm_choice(
        'Full access increases the risk of accidental data loss. Continue?',
    ):
        raise SystemExit

    _script = list(script)
    server_args: dict[str, Any] = {
        'dataset': dataset,
        'output': output_dir,
        'accessLevel': access_level,
        'script': _script,
        'description': description,
        'image': image,
        'cpu': cpu,
        'memory': memory,
        'storage': storage,
        'preemptible': preemptible,
    }

    if not skip_repo_checkout:
        repo_info = get_repository_specific_information(
            repository=repository,
            commit=commit,
            branch=branch,
            cwd=cwd,
            script=_script,
        )
        server_args['repo'] = repo_info.repository
        server_args['commit'] = repo_info.commit
        server_args['branch'] = repo_info.branch
        server_args['cwd'] = repo_info.cwd
        server_args['script'] = repo_info.script

        logger.info(
            f'Submitting {repo_info.repository}@{repo_info.commit} on {repo_info.branch} for dataset "{dataset}"',
        )
    else:
        logger.info(f'Submitting analysis for dataset "{dataset}"')

    if env:
        _env: dict = {}
        for env_var_pair in env:
            try:
                pair = env_var_pair.split('=', maxsplit=1)
                _env[pair[0]] = pair[1]
            except IndexError as e:
                raise IndexError(
                    env_var_pair + ' does not conform to key=value format.',
                ) from e
        server_args['environmentVariables'] = _env

    if config:
        _config = dict(read_configs(config))
        server_args['config'] = _config

    server_endpoint = get_server_endpoint(
        server_url=server_url,
        is_test=use_test_server,
    )
    _token = get_google_identity_token(server_endpoint)

    response = requests.post(
        server_endpoint,
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


def _perform_shebang_check(script: str) -> None:
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
        raise AssertionError(message)


def get_repository_specific_information(  # noqa: C901
    repository: str | None,
    commit: str | None,
    branch: str | None,
    cwd: str | None,
    script: list[str],
) -> RepositorySpecificInformation:
    """
    Do all the repository specific stuff (like fetching repo, commit, cwd).
    The values here are the _user_ provided values (from the CLI).
    """

    if repository is not None and commit is None:
        raise ValueError(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'",
        )

    _repository = repository
    _commit_ref = commit
    _branch = branch or get_git_branch_name()
    _script = list(script)
    _cwd = cwd

    # os.path.exists is only case-sensitive if the local file system is
    # https://stackoverflow.com/questions/6710511/case-sensitive-path-comparison-in-python
    # string in list of strings is exact
    executable_path = os.path.join(_cwd or '', _script[0])

    # we can find the script, and it's a relative path (not absolute)
    if (
        os.path.basename(executable_path)
        in os.listdir(os.path.dirname(executable_path) or '.')
    ) and not executable_path.startswith('/'):
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
            f'Please confirm to continue.',
        ):
            raise SystemExit

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        if _cwd is None:
            _cwd = get_relative_path_from_git_root()

        if not check_if_commit_is_on_remote(_commit_ref) and not confirm_choice(
            f'The commit "{_commit_ref}" was not found on GitHub '
            '(Did you forget to push your latest commit?) \n'
            'Please confirm if you want to proceed anyway.',
        ):
            raise SystemExit

    if _cwd == '.':
        _cwd = None

    if not _repository or not _commit_ref:
        raise SystemExit(
            'Could not determine repository, commit, or cwd. '
            'Please supply these parameters explicitly.',
        )

    return RepositorySpecificInformation(
        repository=_repository,
        commit=_commit_ref,
        cwd=_cwd,
        branch=_branch,
        script=_script,
    )
