#!/usr/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""
import os
import argparse
import logging
import requests
import google.auth
import google.auth.transport.requests
from analysis_runner import _version
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_path_from_git_root,
)

logger = logging.getLogger('analysis_runner')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


BRANCH = 'main'

SERVER_ENDPOINT = 'https://server-a2pko7ameq-ts.a.run.app'


def main_from_args(args=None):
    """
    Parse args (if args is None, argparse automatically uses sys.argv) and run main
    """
    args = parse_args(args=args)
    return main(**vars(args))


def main(
    dataset,
    output_dir,
    script,
    description,
    access_level,
    commit=None,
    repository=None,
):
    """
    Main function that drives the CLI.
    """

    if repository is not None and commit is None:
        raise Exception(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'"
        )

    if access_level == 'full':
        if not confirm_choice(
            'Full access increases the risk of accidental data loss. Continue?',
        ):
            raise SystemExit()

    _repository = repository
    _commit_ref = commit
    _script = list(script)
    _cwd = None

    if ' ' in _script[0]:
        _script = _script[0].split() + _script[1:]

    # false-y value catches empty list / tuple as well
    if not _script:
        _script = ['main.py']

    if _script[0]:
        if os.path.exists(_script[0]):
            _perform_shebang_check(_script[0])

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        _cwd = get_relative_path_from_git_root()
        if _cwd == ".":
            _cwd = None

    _token = _get_google_auth_token()

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
            'cwd': _cwd
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


def confirm_choice(choice: str):
    """
    Confirm 'choice' with user input: y/n
    """
    choice += ' (y/n): '
    while True:
        confirmation = str(input(choice)).lower()
        if confirmation in ('yes', 'y'):
            return True
        if confirmation in ('no', 'n'):
            return False

        print('Unrecognised option, please try again.')


def _get_google_auth_token() -> str:
    # https://stackoverflow.com/a/55804230
    # command = ['gcloud', 'auth', 'print-identity-token']

    creds, _ = google.auth.default()

    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.id_token


def parse_args(args=None):
    """
    Parse args using argparse
    (if args is None, argparse automatically uses `sys.argv`)
    """
    parser = argparse.ArgumentParser()
    # https://docs.python.org/dev/library/argparse.html#action
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version=f'analysis-runner v{_version.__version__}',
    )
    parser.add_argument(
        '--dataset',
        required=True,
        type=str,
        help='The dataset name, which determines which analysis-runner '
        'server to send the request to.',
    )
    parser.add_argument(
        '-o',
        '--output-dir',
        required=True,
        type=str,
        help='The output directory of the run, MUST start with "gs://".',
    )
    parser.add_argument(
        '--repository',
        '--repo',
        help='The URI of the repository to run, must be approved by the appropriate '
        'server. Default behavior is to find the repository of the current working '
        'directory with `git remote get-url origin`.',
    )
    parser.add_argument(
        '--commit',
        help='The commit HASH or TAG of a commit to run, the default behavior is to '
        'use the current commit of the local repository, however the literal value '
        '"HEAD" is not allowed.',
    )

    parser.add_argument(
        '--description',
        required=True,
        help='Human-readable description of the job, '
        'logged together with the output data.',
    )

    parser.add_argument(
        '--access-level',
        choices=(['test', 'standard', 'full']),
        default='test',
        help='Which permissions to grant when running the job.',
    )

    parser.add_argument('script', nargs=argparse.REMAINDER, default=[])

    return parser.parse_args(args)


if __name__ == '__main__':
    main_from_args()
