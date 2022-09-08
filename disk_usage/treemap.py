#!/usr/bin/env python3

"""Produces a treemap visualization from disk usage summary stats."""

import argparse
import gzip
import json
import logging
from cloudpathlib import AnyPath
import plotly.express as px


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The path to the gzipped input JSON; supports cloud paths and can be specified multiple times',
        required=True,
        action='append',
    )
    parser.add_argument(
        '--output',
        help='The path to the output HTML report; supports cloud paths',
        required=True,
    )
    parser.add_argument(
        '--max-depth',
        help='Maximum folder depth to display',
        default=5,
        type=int,
    )
    args = parser.parse_args()

    names, parents, values = [], [], []
    for input_path in args.input:
        logging.info(f'Processing {input_path}')
        with AnyPath(input_path).open('rb') as f:
            with gzip.open(f, 'rt') as gfz:
                for name, vals in json.load(gfz).items():
                    depth = name.count('/') - 1  # Don't account for gs:// scheme.
                    if depth > args.max_depth:
                        continue
                    names.append(name)
                    slash_index = name.rfind('/')
                    # Strip one folder for the parent name. Map `gs://` to the empty
                    # string, i.e. the treemap root.
                    parents.append(name[:slash_index] if slash_index > 3 else '')
                    values.append(vals['size'])

    fig = px.treemap(names=names, parents=parents, values=values)
    fig.write_html(args.output)


if __name__ == '__main__':
    main()
