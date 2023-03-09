"""
CLI options for standard analysis-runner
"""

import os
import argparse
import sys
from typing import List

import requests
import toml
from cpg_utils.config import read_configs
from cpg_utils.cloud import get_google_identity_token
from analysis_runner.constants import get_server_endpoint
from analysis_runner.util import _perform_version_check, logger


def add_config_args(parser=None) -> argparse.ArgumentParser:
    """
    Add CLI arguments for standard analysis-runner
    """
    if not parser:
        parser = argparse.ArgumentParser('config subparser')

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
        '--access-level',
        choices=(['test', 'standard', 'full']),
        default='test',
        help='Which permissions to grant when running the job.',
    )

    parser.add_argument(
        '--image',
        help=(
            'Image name, if using standard / full access levels, this must start with '
            'australia-southeast1-docker.pkg.dev/cpg-common/'
        ),
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
        '--config-output',
        required=False,
        help='Output path to write the generated config to (in YAML)',
    )

    return parser


def run_config_from_args(args):
    """Run analysis runner from argparse.parse_arguments"""
    return run_config(**vars(args))


def run_config(  # pylint: disable=too-many-arguments
    dataset,
    output_dir,
    access_level,
    image=None,
    config: List[str] = None,
    config_output=None,
    use_test_server=False,
):
    """
    Main function that drives the CLI.
    """
    _perform_version_check()

    _config = None
    if config:
        _config = dict(read_configs(config))

    server_endpoint = os.path.join(
        get_server_endpoint(is_test=use_test_server), 'config'
    )
    _token = get_google_identity_token(server_endpoint)

    response = requests.post(
        server_endpoint,
        json={
            'dataset': dataset,
            'output': output_dir,
            'accessLevel': access_level,
            'image': image,
            'config': _config,
        },
        headers={'Authorization': f'Bearer {_token}'},
        timeout=60,
    )
    try:
        response.raise_for_status()
        if config_output:
            if not config_output.endswith('.toml'):
                logger.warning(
                    'The config is written as a .toml file, but the extension on the '
                    'file you have provided is not .toml'
                )
            with open(config_output, 'w+', encoding='utf-8') as f:
                toml.dump(response.json(), f)
                logger.info(f'Wrote config to {config_output}')
        else:
            toml.dump(response.json(), sys.stdout)

    except requests.HTTPError as e:
        logger.critical(
            f'Request failed with status {response.status_code}: {str(e)}\n'
            f'Full response: {response.text}',
        )
