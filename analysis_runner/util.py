"""Utility function"""
# pylint: disable=import-outside-toplevel

import re
import logging
import requests

from analysis_runner._version import __version__

BRANCH = 'main'
SERVER_ENDPOINT = 'https://server-a2pko7ameq-ts.a.run.app'

logger = logging.getLogger('analysis_runner')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def add_general_args(parser):
    """
    Add CLI arguments that are relevant for most
    analysis-runner submission modes (standard / cromwell)
    """
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
        help='The output directory within the bucket. This should not contain a prefix like "gs://cpg-fewgenomes-main/".',
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

    parser.add_argument(
        '--cwd',
        required=False,
        help='Supply the (relative) working directory to use, the analysis-runner will '
        '"cd <cwd>" before running any execution. If the "--cwd" and "--repository" '
        'arguments are not supplied, the relative path to the git root will be determined',
    )

    parser.add_argument(
        '--environment-variables',
        required=False,
        help='A dictionary of environment variables e.g. {"SM_ENVIRONMENT":"development"} ',
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


def get_google_identity_token() -> str:
    """
    Get google identity token, equivalent of calling:
        ['gcloud', 'auth', 'print-identity-token']
    """
    import google.auth
    import google.auth.transport.requests

    # https://stackoverflow.com/a/55804230
    creds, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.id_token


def _perform_version_check():

    current_version = __version__

    # with this URL, we're looking for a line with format:
    #   __version__ = '<version>'
    # match it with regex: r"__version__ = '(.+)'$"
    version_url = (
        'https://raw.githubusercontent.com/populationgenomics/'
        'analysis-runner/main/analysis_runner/_version.py'
    )
    try:
        resp = requests.get(version_url)
        resp.raise_for_status()
        data = resp.text
    except requests.HTTPError as e:
        logger.debug(
            f'An error occurred when fetching version '
            f'information about the analysis-runner: {e}'
        )
        return
    for line in data.splitlines(keepends=False):
        if not line.startswith('__version__ = '):
            continue

        latest_version = re.match(f"__version__ = '(.+)'$", line).groups()[0]
        if current_version != latest_version:
            message = (
                f'Your version of analysis-runner is out of date: '
                f'{current_version} != {latest_version} (current vs latest).\n'
                f'Your analysis will still be submitted, but may not work as expected.'
                f' You can update the analysis-runner by running '
                f'"conda install -c cpg analysis-runner={latest_version}".'
            )
            logger.warning(message)
        return
