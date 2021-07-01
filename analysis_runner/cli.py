#!/usr/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""
from typing import Dict, Tuple, Callable

import sys
import argparse

from analysis_runner._version import __version__
from analysis_runner.analysisrunner import add_analysis_runner_args, run_analysis_runner
from analysis_runner.cromwell import add_cromwell_args, run_cromwell


def main_from_args(args=None):
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
        version=f'analysis-runner v{__version__}',
    )

    default_mode = 'analysis-runner'
    modes: Dict[str, Tuple[Callable[[], argparse.ArgumentParser], Callable]] = {
        'analysis-runner': (add_analysis_runner_args, run_analysis_runner),
        'cromwell': (add_cromwell_args, run_cromwell),
    }

    # sub-argparser don't work with a default mode, so add a
    parser.add_argument(
        'mode',
        choices=list(modes.keys()),
        default=default_mode,
        nargs='?',
        help='Which mode of the analysis-runner to use',
    )

    args = args or sys.argv[1:]

    mode = default_mode
    if len(args) > 0 and args[0] in modes:
        mode = args.pop(0)

    mode_argparser_f, run_mode = modes[mode]
    run_mode(**vars(mode_argparser_f().parse_args(args)))


if __name__ == '__main__':
    main_from_args()
