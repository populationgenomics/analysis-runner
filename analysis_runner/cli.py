#!/usr/bin/env python

"""
CLI for interfacing with deployed analysis runner.
See README.md for more information.
"""
from typing import Dict, Tuple, Callable

import sys
import argparse

from analysis_runner._version import __version__
from analysis_runner.cli_analysisrunner import (
    add_analysis_runner_args,
    run_analysis_runner_from_args,
)
from analysis_runner.cli_cromwell import add_cromwell_args, run_cromwell_from_args


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
        'analysis-runner': (add_analysis_runner_args, run_analysis_runner_from_args),
        'cromwell': (add_cromwell_args, run_cromwell_from_args),
    }

    args = args or sys.argv[1:]

    if len(args) == 0:
        args = ['--help']

    mode = args[0]

    if mode in ('-h', '--help', 'help'):
        # display help text
        cs_modekeys = ','.join(modes)
        print(
            f"""
usage: analysis-runner [-h] [-v] [{{{cs_modekeys}] ...

optional positional arguments:
  [{{{cs_modekeys},version,help}}]
    DEFAULT = analysis-runner

optional arguments:
  -h, --help       show this help message and exit
  -v, --version    display the version and exit
"""
        )
        return
    if mode in ('-v', '--version', 'version'):
        print(f'analysis-runner v{__version__}')
        return

    if mode not in modes:
        mode = default_mode
    else:
        args = args[1:]

    mode_argparser_f, run_mode = modes[mode]
    run_mode(mode_argparser_f().parse_args(args))


if __name__ == '__main__':
    main_from_args()
