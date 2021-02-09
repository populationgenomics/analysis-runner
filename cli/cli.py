#!/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""

import subprocess
import click
import requests
import google.auth
import google.auth.transport.requests

BRANCH = 'add-cli'  # 'master'
DEFAULT_SERVER_LOOKUP = (
    f'https://raw.githubusercontent.com/'
    f'populationgenomics/analysis-runner/{BRANCH}/cli/servermap.json'
)


@click.command()
@click.option(
    '--dataset',
    required=True,
    help='The dataset name, which determines which '
    'analysis-runner server to send the request to',
)
@click.option(
    '--output-dir',
    required=True,
    help='The output directory of the run, MUST start with gs://',
)
@click.option(
    '--repository',
    '--repo',
    help='The URI of the repository to run, must be approved by the appropriate server.'
    ' Default behaviour is to find the repository of the current working'
    ' directory with `git remote get-url origin`',
)
@click.option(
    '--commit-ref',
    '--hash',
    help='The hash of commit to run, default behaviour is to ' 'use the current commit',
)
@click.option(
    '--description',
    required=True,
    help='Description of job, otherwise defaults to: "$USER FROM LOCAL: $REPO@$COMMIT"',
)
@click.option('script', nargs=-1, default=['main.py'])
def main(dataset, output_dir, script, description, commit_ref=None, repository=None):
    """
    Main function that drives the CLI.
    The parameters are provided automatically by @click.
    """
    _repository = repository or _get_default_remote()
    _commit_ref = commit_ref or _get_default_commit_ref(
        used_custom_repository=repository is not None
    )
    _url = _get_url_from_dataset(dataset)
    _token = _get_google_auth_token()

    print(dataset, output_dir, script, _repository, _commit_ref, _token)

    requests.post(
        _url,
        json={
            'output': output_dir,
            'repo': _repository,
            'commit': _commit_ref,
            'script': list(script),
            'description': description,
        },
        headers={'Authorization': f'Bearer {_token}'},
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


def _get_default_remote() -> str:
    command = ['git', 'remote', 'get-url', 'origin']
    return _get_output_of_command(command, 'get GIT repository')


def _get_default_commit_ref(used_custom_repository) -> str:
    if used_custom_repository:
        raise Exception(
            "The analysis-runner CLI can't get the commit ref"
            ' when using a custom repository'
        )

    command = ['git', 'rev-parse', 'HEAD']
    return _get_output_of_command(command, 'get latest GIT commit')


def _get_url_from_dataset(dataset: str) -> str:

    resource = requests.get(DEFAULT_SERVER_LOOKUP)
    d = resource.json()

    url = d.get(dataset)
    if url:
        return url

    raise Exception(f"Couldn't get URL for '{dataset}', expected one of {d.keys()}")


if __name__ == '__main__':
    # Disable pylint because click decorates the function in a specific way
    # pylint: disable=no-value-for-parameter
    main()
