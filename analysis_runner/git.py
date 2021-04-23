"""Helper functions for working with Git repositories."""

import os
import re
import subprocess
from typing import List

SUPPORTED_ORGANIZATIONS = {'populationgenomics'}


def get_output_of_command(command: List[str], description: str) -> str:
    """subprocess.check_output wrapper that returns string output and raises detailed
    exceptions on error."""
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


def get_relative_script_path_from_git_root(script_name: str) -> str:
    """
    If we're in a subdirectory, get the relative path from the git root
    to the current directory, and append the script path.
    For example, the relative path to this script (from git root) is:

        analysis_runner/git.py
    """
    base = get_relative_path_from_git_root()
    return os.path.join(base, script_name)


def get_relative_path_from_git_root() -> str:
    """
    If we're in a subdirectory, get the relative path from the git root
    to the current directory. Relpath returns "." if cwd is a git root.
    """
    root = get_git_repo_root()
    base = os.path.relpath(os.getcwd(), root)
    return base


def get_git_default_remote() -> str:
    """Returns the default git remote,
    e.g. https://github.com/populationgenomics/analysis-runner
    """
    command = ['git', 'remote', 'get-url', 'origin']
    return get_output_of_command(command, 'get default Git remote')


def get_git_repo_root() -> str:
    """Returns the git repository directory root,
    e.g. /Users/foo/repos/analysis-runner
    """
    command = ['git', 'rev-parse', '--show-toplevel']
    repo_root = get_output_of_command(command, 'get Git repo directory')
    return repo_root


def get_git_commit_ref_of_current_repository() -> str:
    """Returns the commit SHA at the current HEAD."""
    command = ['git', 'rev-parse', 'HEAD']
    return get_output_of_command(command, 'get latest Git commit')


def get_repo_name_from_remote(remote_name: str) -> str:
    """
    Get the name of a GitHub repo from a supported organization
    based on its remote URL e.g.:

    >>> get_repo_name_from_remote(\
        'git@github.com:populationgenomics/analysis-runner.git'\
    )
    'analysis-runner'
    >>> get_repo_name_from_remote(\
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

    if organization not in SUPPORTED_ORGANIZATIONS:
        raise Exception(f'Unsupported GitHub organization "{organization}"')

    if not repo:
        raise Exception(f'Unsupported remote format: "{remote_name}"')
    if repo.endswith('.git'):
        repo = repo[:-4]

    return repo
