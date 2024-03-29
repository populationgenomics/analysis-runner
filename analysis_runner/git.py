"""Helper functions for working with Git repositories."""

import os
import re
import subprocess
from shlex import quote
from typing import Any, List, Optional

GITHUB_ORG = 'populationgenomics'
SUPPORTED_ORGANIZATIONS = {GITHUB_ORG}


def get_output_of_command(command: List[str], description: str) -> str:
    """subprocess.check_output wrapper that returns string output and raises detailed
    exceptions on error."""
    try:
        return subprocess.check_output(command).decode().strip()  # noqa: S603
    # Handle and rethrow KeyboardInterrupt error to stop global exception catch

    except KeyboardInterrupt:
        raise
    except subprocess.CalledProcessError as e:
        raise OSError(
            f"Couldn't call {description} by calling '{' '.join(command)}', {e}",
        ) from e
    except Exception as e:  # noqa: BLE001
        raise type(e)(
            f"Couldn't process {description} through calling '{' '.join(command)}', {e}",
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
    return os.path.relpath(os.getcwd(), root)


def get_git_default_remote() -> str:
    """Returns the git remote of 'origin',
    e.g. https://github.com/populationgenomics/analysis-runner
    """
    command = ['git', 'remote', 'get-url', 'origin']
    return get_output_of_command(command, 'get Git remote of origin')


def get_git_repo_root() -> str:
    """Returns the git repository directory root,
    e.g. /Users/foo/repos/analysis-runner
    """
    command = ['git', 'rev-parse', '--show-toplevel']
    return get_output_of_command(command, 'get Git repo directory')


def get_git_commit_ref_of_current_repository() -> str:
    """Returns the commit SHA at the current HEAD."""
    command = ['git', 'rev-parse', 'HEAD']
    return get_output_of_command(command, 'get latest Git commit')


def get_git_branch_name() -> Optional[str]:
    """Returns the current branch name."""
    command = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    try:
        value = subprocess.check_output(command).decode().strip()  # noqa: S603
        if value:
            return value
    except Exception:  # noqa: BLE001
        return None

    return None


def get_repo_name_from_current_directory() -> str:
    """Gets the repo name from the default remote"""
    return get_repo_name_from_remote(get_git_default_remote())


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
        if match:
            organization, repo = match.groups()
    elif remote_name.startswith('git@'):
        match = re.match(r'git@[A-z0-9\.]+?:(.+?)\/(.+)$', remote_name)
        if match:
            organization, repo = match.groups()

    if organization not in SUPPORTED_ORGANIZATIONS:
        raise ValueError(f'Unsupported GitHub organization "{organization}"')
    if not repo:
        raise ValueError(f'Unsupported remote format: "{remote_name}"')

    if repo.endswith('.git'):
        repo = repo[:-4]

    return repo


def check_if_commit_is_on_remote(commit: str) -> bool:
    """
    Returns 'True' if the commit is available on a remote branch.
    This relies on the current environment to be up-to-date.
    It asks if the local environment knows a remote branch with the commit.
    """
    command = ['git', 'branch', '-r', '--contains', commit]
    try:
        ret = subprocess.check_output(command)  # noqa: S603
        return bool(ret)
    except subprocess.CalledProcessError:
        return False


def guess_script_name_from_script_argument(script: List[str]) -> Optional[str]:
    """
    Guess the script name from the first argument of the script.
    If the first argument is an executable, try the second param

    >>> guess_script_name_from_script_argument(['python', 'main.py'])
    'main.py'

    >>> guess_script_name_from_script_argument(['./main.sh'])
    'main.sh'

    >>> guess_script_name_from_script_argument(['main.sh'])
    'main.sh'

    >>> guess_script_name_from_script_argument(['./test/path/main.sh', 'arg1', 'arg2'])
    'test/path/main.sh'

    >>> guess_script_name_from_script_argument(['gcloud', 'cp' 'test'])
    None

    """
    executables = {'python', 'python3', 'bash', 'sh', 'rscript'}
    _script = script[0]
    if _script.lower() in executables:
        _script = script[1]

    if _script.startswith('./'):
        return _script[2:]

    # a very bad check if it follows format "file.ext"
    if '.' in _script:
        return _script

    return None


def guess_script_github_url_from(
    *,
    repo: Optional[str],
    commit: Optional[str],
    cwd: Optional[str],
    script: List[str],
) -> Optional[str]:
    """
    Guess the GitHub URL of the script from the given arguments.
    """
    guessed_script_name = guess_script_name_from_script_argument(script)
    if not guessed_script_name:
        return None

    url = f'https://github.com/{GITHUB_ORG}/{repo}/tree/{commit}'

    if cwd == '.' or cwd is None:
        return f'{url}/{guessed_script_name}'

    return os.path.join(url, cwd, guessed_script_name)


def prepare_git_job(
    job: Any,  # don't specify the type to avoid an extra import
    repo_name: str,
    commit: str,
    is_test: bool = True,
    print_all_statements: bool = True,
    get_deploy_token: bool = True,
):
    """
    Takes a hail batch job, and:
        * Activates the google service account
        * Clones the repository, and
            * if access_level != "test": check the desired commit is on 'main'
            * check out the specific commit

    :param get_deploy_token: If True, get the deploy token from secret manager.
        This requires cpg-utils, which you might want to disable if you're running
        this method outside of a CPG project.
    """

    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')

    # Use "set -x" to print the commands for easier debugging.
    if print_all_statements:
        job.command('set -x')

    # activate the google service account
    job.command(
        'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS',
    )

    # Note: for private GitHub repos we'd need to use a token to clone.
    #   - store the token on secret manager
    #   - The git_credentials_secret_{name,project} values are set by cpg-infrastructure
    #   - check at runtime whether we can get the token
    #   - if so, set up the git credentials store with that value
    if get_deploy_token:
        job.command(
            """
# get secret names from config if they exist
secret_name=$(python3 -c '
try:
    from cpg_utils.config import get_config
    print(get_config(print_config=False).get("infrastructure", {}).get("git_credentials_secret_name", ""))
except:
    pass
' || echo '')

secret_project=$(python3 -c '
try:
    from cpg_utils.config import get_config
    print(get_config(print_config=False).get("infrastructure", {}).get("git_credentials_secret_project", ""))
except:
    pass
' || echo '')

if [ ! -z "$secret_name" ] && [ ! -z "$secret_project" ]; then
    # configure git credentials store if credentials are set
    gcloud --project $secret_project secrets versions access --secret $secret_name latest > ~/.git-credentials
    git config --global credential.helper "store"
else
    echo 'No git credentials secret found, unable to check out private repositories.'
fi
        """,
        )

    # Any job commands here are evaluated in a bash shell, so user arguments should
    # be escaped to avoid command injection.
    repo_path = f'https://github.com/{GITHUB_ORG}/{repo_name}.git'
    job.command(f'git clone --recurse-submodules {quote(repo_path)}')
    job.command(f'cd {quote(repo_name)}')
    # Except for the "test" access level, we check whether commits have been
    # reviewed by verifying that the given commit is in the main branch.
    if not is_test:
        job.command('git checkout main')
        job.command(
            f'git merge-base --is-ancestor {quote(commit)} HEAD || '
            '{ echo "error: commit not merged into main branch"; exit 1; }',
        )
    job.command(f'git checkout {quote(commit)}')
    job.command('git submodule update')

    return job
