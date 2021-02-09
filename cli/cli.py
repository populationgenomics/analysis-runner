"""
CLI for interacting with the REMOTE analysis-runner
    Motivation: https://github.com/populationgenomics/analysis-runner/issues/8

Mostly for interacting with the server/main.py

Called with:

    analysis-runner run \
        --output-dir gs://some-path/ \
        --commit-hash <current-hash> \
        --repo <current-repo>  \
        --script <default: main.py>
Process:

    1. Fill in the mising info (repo, commit hash, etc)
    2. Get the gcloud auth token $(gcloud auth print-identity-token)
    3. Get the submit URL from the batch parameter
    4. Form a POST request with params:
        * output
        * repo
        * commit
        * script
        * description
    5. Collect and print response
"""
from getpass import getuser

import subprocess
import click
import requests
import google.auth
import google.auth.transport.requests

BRANCH = 'add-cli'  # 'master'
DEFAULT_STACK_LOOKUP = (
    f'https://raw.githubusercontent.com/'
    f'populationgenomics/analysis-runner/{BRANCH}/cli/stacklookup.json'
)


@click.command()
@click.option(
    '--stack',
    required=True,
    help='The stack name, which determines which analysis-runner server to send to',
)
@click.option(
    '--output-dir',
    required=True,
    help='The output directory of the run, MUST start with gs://',
)
@click.option(
    '--repository',
    help='The URI of the repository to run, must be approved by the appropriate server.'
    ' Default behaviour is to find the repository of the current working'
    ' directory with `git remote get-url origin`',
)
@click.option(
    '--commit-ref',
    '--hash',
    help='The hash of repository to run, default behaviour is to '
    'find the latest hash of the current repository',
)
@click.option(
    '--description',
    help='Description of job, otherwise defaults to: "$USER FROM LOCAL: $REPO@$COMMIT"',
)
@click.option('--script', multiple=True, default=['main.py'])
def main(stack, output_dir, script, commit_ref=None, repository=None, description=None):
    """
    Main function drives the CLI
    """
    _repository = repository or _get_default_remote()
    _commit_ref = commit_ref or _get_default_commit_ref(
        used_custom_repository=repository is not None
    )
    _description = description or f'{getuser()} FROM LOCAL: {_repository}@{_commit_ref}'
    _token = _get_google_auth_token()
    _url = _get_url_from_stack(stack)

    print(stack, output_dir, script, _repository, _commit_ref, _token)

    requests.post(
        _url,
        json={
            'output': output_dir,
            'repo': _repository,
            'commit': _commit_ref,
            'script': list(script),
            'description': _description,
        },
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


def _get_url_from_stack(stack: str) -> str:

    resource = requests.get(DEFAULT_STACK_LOOKUP)
    d = resource.json()

    url = d.get(stack)
    if url:
        return url

    raise Exception(f"Couldn't get URL for '{stack}', expected one of {d.keys()}")


if __name__ == '__main__':
    # Disable pylint because click decorates the function in a specific way
    # pylint: disable=no-value-for-parameter
    main()
