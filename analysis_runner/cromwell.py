"""
Cromwell CLI
"""
# pylint: disable=too-many-arguments,too-many-return-statements

import argparse
from typing import List, Dict, Optional

import requests

from analysis_runner.util import (
    logger,
    add_general_args,
    _perform_version_check,
    confirm_choice,
    _get_google_identity_token,
    SERVER_ENDPOINT,
)
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_path_from_git_root,
)


def add_cromwell_args(parser=None) -> argparse.ArgumentParser:
    """Create / add arguments for cromwell argparser"""
    if not parser:
        parser = argparse.ArgumentParser('cromwell subparser')

    add_general_args(parser)

    parser.add_argument(
        '-i', '--inputs', help='Relative path to input JSON', required=False
    )
    # matches the cromwell param
    parser.add_argument(
        '-p',
        '--imports',
        nargs='+',
        required=False,
        help='A list of directories which are used to search for workflow imports',
    )
    parser.add_argument(
        '--workflow-input-prefix',
        type=str,
        required=False,
        help='Prefix to apply to all inputs AFTER the workflow argument, usually the workflow name',
    )

    # workflow WDL
    parser.add_argument('workflow', help='WDL file to submit to cromwell')

    # other inputs
    parser.add_argument(
        'dynamic_inputs',
        nargs=argparse.REMAINDER,
        help='--{key} {value} that get automatically parsed into an inputs json',
    )

    return parser


def run_cromwell(
    dataset,
    output_dir,
    description,
    access_level,
    workflow: str,
    inputs: List[str],
    imports: List[str] = None,
    workflow_input_prefix: str = None,
    dynamic_inputs: List[str] = None,
    commit=None,
    repository=None,
):
    """
    Prepare parameters for cromwell analysis-runner job
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
    _cwd = None

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        _cwd = get_relative_path_from_git_root()
        if _cwd == '.':
            _cwd = None

    _inputs_dict = None
    if dynamic_inputs:
        _inputs_dict = parse_additional_args(dynamic_inputs)
        if workflow_input_prefix:
            _inputs_dict = {
                workflow_input_prefix + k: v for k, v in _inputs_dict.items()
            }

    response = requests.post(
        SERVER_ENDPOINT + '/cromwell',
        json={
            'dataset': dataset,
            'output': output_dir,
            'repo': _repository,
            'accessLevel': access_level,
            'commit': _commit_ref,
            'description': description,
            'cwd': _cwd,
            'workflow': workflow,
            'inputs_dict': _inputs_dict,
            'input_json_paths': inputs or [],
            'dependencies': imports or [],
        },
        headers={'Authorization': f'Bearer {_get_google_identity_token()}'},
    )
    try:
        response.raise_for_status()
        logger.info(f'Request submitted successfully: {response.text}')
    except requests.HTTPError as e:
        logger.critical(
            f'Request failed with status {response.status_code}: {str(e)}\n'
            f'Full response: {response.text}',
        )


def try_parse_value(value: Optional[str]):
    """Try parse value from command line string"""
    if value is None or value == 'None' or value == 'null':
        return value

    if isinstance(value, list):
        return list(map(try_parse_value, value))

    if not isinstance(value, str):
        # maybe the CLI did some parsing
        return value

    if value == 'true':
        return True
    if value == 'false':
        return False

    # EAFP
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass

    return value


def parse_keyword(keyword: str):
    """Parse CLI keyword"""
    return keyword[2:].replace('-', '_')


def parse_additional_args(args: List[str]) -> Dict[str, any]:
    """
    Parse a list of strings to an inputs json

    The theory is to look for any args that start with '--',
    strip those two leading dashes, replace '-' with '_'.
    1. If the keyword is followed by another keyword, then make
          the first param a boolean (with True)
    2. If the keyword is followed by a single non-keyword value, set the value
          parse the type and
    3. If the keyword is followed by multiple arguments, make it an array

    Examples:
    >>> parse_additional_args(['--keyword1', '--keyword2'])
    {'keyword1': True, 'keyword2': True}

    >>> parse_additional_args(['--keyword', 'single value'])
    {'keyword': 'single value'}

    >>> parse_additional_args(['--keyword', 'val1', 'val2'])
    {'keyword': ['val1', 'val2']}

    >>> parse_additional_args(['--keyword', 'false'])
    {'keyword': False}


    Special cases:
        * Keyword with single value, followed by the same keyword with no params, turn the original list into an array, eg:

    >>> parse_additional_args(['--keyword', 'value', '--keyword'])
    {'keyword': ['value']}

        * Keyword with multiple values, followed by same keyword with multiple values results in nested lists

    >>> parse_additional_args(['--keyword', 'val1_a', 'val1_b', '--keyword', 'val2_a', 'val2_b'])
    {'keyword': [['val1_a', 'val1_b'], ['val2_a', 'val2_b']]}
    """

    keywords = {}

    def add_keyword_value_to_keywords(keyword, value):
        if keyword in keywords:
            if value is None:
                value = [keywords.get(keyword)]
            else:
                value = [keywords.get(keyword), try_parse_value(new_value)]
        elif value is None:
            # flag
            value = True
        else:
            value = try_parse_value(value)

        keywords[keyword] = value

    current_keyword = None
    new_value: any = None

    for arg in args:
        if not arg.startswith('--'):

            if new_value is None:
                # if it's the first value we're seeing, set it
                new_value = arg
            elif not isinstance(new_value, list):
                # if it's the second value we're seeing
                new_value = [new_value, arg]
            else:
                # 3rd or more, just keep adding it to an array
                new_value.append(arg)

            continue

        # found a new keyword
        if current_keyword is None:
            # first case, do nothing
            current_keyword = parse_keyword(arg)
            continue

        add_keyword_value_to_keywords(current_keyword, new_value)
        current_keyword = parse_keyword(arg)
        new_value = None

    add_keyword_value_to_keywords(current_keyword, new_value)

    return keywords
