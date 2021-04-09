#!/usr/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""
import logging
import click
import requests
import google.auth
import google.auth.transport.requests
from analysis_runner import _version
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_script_path_from_git_root,
)

logger = logging.getLogger('analysis_runner')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


BRANCH = 'main'

SERVER_ENDPOINT = 'https://server-a2pko7ameq-ts.a.run.app'


@click.version_option(_version.__version__)
@click.command(
    help='CLI for the analysis runner - a CPG service for running analysis from '
    'some GitHub repository (at a specific commit). The parameters are used to form a '
    'POST request, sent to a server based on the --dataset parameter you provide.'
)
@click.option(
    '--dataset',
    required=True,
    help='The dataset name, which determines which analysis-runner server to send the '
    ' request to.',
)
@click.option(
    '--output-dir',
    '-o',
    required=True,
    help='The output directory of the run, MUST start with "gs://".',
)
@click.option(
    '--repository',
    '--repo',
    help='The URI of the repository to run, must be approved by the appropriate server.'
    ' Default behavior is to find the repository of the current working'
    ' directory with `git remote get-url origin`.',
)
@click.option(
    '--commit',
    help='The commit HASH or TAG of a commit to run, the default behavior is to '
    'use the current commit of the local repository, however the literal value '
    '"HEAD" is not allowed.',
)
@click.option(
    '--description',
    required=True,
    help='Human-readable description of the job, logged together with the output data.',
)
@click.option(
    '--access-level',
    type=click.Choice(['test', 'standard', 'full']),
    default='test',
    help='Which permissions to grant when running the job.',
)
@click.argument('script', nargs=-1)
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
    The parameters are provided automatically by @click.
    """

    if repository is not None and commit is None:
        raise Exception(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'"
        )

    if access_level == 'full':
        click.confirm(
            'Full access increases the risk of accidental data loss. Continue?',
            abort=True,
        )

    _repository = repository
    _commit_ref = commit
    _script = list(script)
    if ' ' in _script[0]:
        _script = _script[0].split() + _script[1:]

    # false-y value catches empty list / tuple as well
    if not _script:
        _script = ['main.py']

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        # Make the first argument (the script name) relative
        # to the git root and current directory
        _script[0] = get_relative_script_path_from_git_root(_script[0])

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


def _get_google_auth_token() -> str:
    # https://stackoverflow.com/a/55804230
    # command = ['gcloud', 'auth', 'print-identity-token']

    creds, _ = google.auth.default()

    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.id_token


if __name__ == '__main__':
    # Disable pylint because click decorates the function in a specific way
    # pylint: disable=no-value-for-parameter
    main()
