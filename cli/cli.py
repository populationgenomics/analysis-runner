#!/usr/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""
import os
import re
import logging
import subprocess
import click
import requests
import google.auth
import google.auth.transport.requests
from cli import _version

logging.basicConfig(level='INFO')

BRANCH = 'main'
DEFAULT_SERVER_LOOKUP = (
    f'https://raw.githubusercontent.com/'
    f'populationgenomics/analysis-runner/{BRANCH}/cli/servermap.json'
)
# Make this None if you want to support all organizations
SUPPORTED_ORGANIZATIONS = {'populationgenomics'}


@click.version_option(_version.__version__)
@click.command(
    help='CLI for the analysis runner - a CPG service for running analysis from '
    'some GitHub repository (at a specific commit). The parameters are used to form a '
    'POST request, sent to a server based on the --dataset parameter you provide.'
)
@click.option(
    '--dataset',
    required=True,
    help='The dataset name, which determines which '
    'analysis-runner server to send the request to',
)
@click.option(
    '--output-dir',
    '-o',
    required=True,
    help='The output directory of the run, MUST start with gs://',
)
@click.option(
    '--repository',
    '--repo',
    help='The URI of the repository to run, must be approved by the appropriate server.'
    ' Default behavior is to find the repository of the current working'
    ' directory with `git remote get-url origin`',
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
    help='Description of job, otherwise defaults to: "$USER FROM LOCAL: $REPO@$COMMIT"',
)
@click.argument('script', nargs=-1)
def main(dataset, output_dir, script, description, commit=None, repository=None):
    """
    Main function that drives the CLI.
    The parameters are provided automatically by @click.
    """

    if repository is not None and commit is None:
        raise Exception(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'"
        )

    _repository = repository
    _commit_ref = commit
    _script = list(script)

    # false-y value catches empty list / tuple as well
    if not _script:
        _script = ['main.py']

    if repository is None:
        _repository = _get_git_default_remote()
        if _commit_ref is None:
            _commit_ref = _get_git_commit_ref_of_current_repository()

        # Make the first argument (the script name) relative
        # to the git root and current directory
        _script[0] = _get_relative_script_path_from_git_root(_script[0])

    _url = _get_url_from_dataset(dataset)
    _token = _get_google_auth_token()

    logging.info(f'Submitting {_repository}@{_commit_ref} for dataset "{dataset}"')

    response = requests.post(
        _url,
        json={
            'output': output_dir,
            'repo': _repository,
            'commit': _commit_ref,
            'script': _script,
            'description': description,
        },
        headers={'Authorization': f'Bearer {_token}'},
    )
    try:
        response.raise_for_status()
        logging.info(f'Request submitted successfully: {response.text}')
    except requests.HTTPError as e:
        logging.critical(
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


def _get_output_of_command(command, description: str) -> str:
    try:
        return subprocess.check_output(command).decode().strip()
    # Handle and rethrow KeyboardInterrupt error to stop global exception catch
    # pylint: disable=try-except-raise
    except KeyboardInterrupt:
        raise
    except subprocess.CalledProcessError as e:
        raise Exception(
            f"Couldn't call {description} by calling '{' '.join(command)}', {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"Couldn't process {description} through calling '{' '.join(command)}', {e}"
        ) from e


def _get_relative_script_path_from_git_root(script_name: str) -> str:
    """
    If we're in a subdirectory, get the relative path from the git root
    to the current directory. For example, the relative path to this
    script directly is:

        cli/cli.py
    """
    root = _get_git_repo_root()
    base = os.path.relpath(os.getcwd(), root)
    return os.path.join(base, script_name)


def _get_git_default_remote() -> str:
    command = ['git', 'remote', 'get-url', 'origin']
    full_remote = _get_output_of_command(command, 'get default Git repository')
    return _get_repo_name_from_remote(full_remote)


def _get_git_repo_root() -> str:
    command = ['git', 'rev-parse', '--show-toplevel']
    repo_root = _get_output_of_command(command, 'get Git repo directory')
    return repo_root


def _get_git_commit_ref_of_current_repository() -> str:
    command = ['git', 'rev-parse', 'HEAD']
    return _get_output_of_command(command, 'get latest Git commit')


def _get_repo_name_from_remote(remote_name: str) -> str:
    """
    Turn the remote name received from 'git remote get-url origin' into the

    >>> _get_repo_name_from_remote(\
        'git@github.com:populationgenomics/analysis-runner.git'\
    )
    'analysis-runner'
    >>> _get_repo_name_from_remote(\
        'https://github.com/populationgenomics/analysis-runner.git'\
    )
    'analysis-runner'
    """

    organization = None
    repo = None
    if remote_name.startswith('http'):
        match = re.match(r'https:\/\/[A-z0-9\.]+?\/(.+?)\/(.+)$', remote_name)
        organization, repo = match.groups()
    elif remote_name.startswith('git@'):
        match = re.match(r'git@[A-z0-9\.]+?:(.+?)\/(.+)$', remote_name)
        organization, repo = match.groups()

    if SUPPORTED_ORGANIZATIONS and organization not in SUPPORTED_ORGANIZATIONS:
        raise Exception(f'Unsupported GitHub organization "{organization}"')

    if not repo:
        raise Exception(f'Unsupported remote format: "{remote_name}"')
    if repo.endswith('.git'):
        repo = repo[:-4]

    return repo


def _get_url_from_dataset(dataset: str) -> str:
    resource = requests.get(DEFAULT_SERVER_LOOKUP)
    if not resource.ok:
        resource.raise_for_status()

    d = resource.json()

    url = d.get(dataset)
    if url:
        return url

    raise Exception(f"Couldn't get URL for '{dataset}', expected one of {d.keys()}")


if __name__ == '__main__':
    # Disable pylint because click decorates the function in a specific way
    # pylint: disable=no-value-for-parameter
    main()
