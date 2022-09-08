#!/usr/bin/env python3

import argparse
import plotly.express as px
import gzip
import json
from cloudpathlib import AnyPath


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The gzipped input JSON; can be specified multiple times',
        action='append',
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
        with AnyPath(input_path).open('rb') as f:
            with gzip.open(f, 'rt') as gfz:
                for k, v in json.load(gfz).items():
                    if not k:
                        continue  # Skip the root itself
                    depth = k.count('/') - 1  # Don't account for gs:// scheme.
                    if depth > args.max_depth:
                        continue
                    names.append(k)
                    slash_index = k.find('/')
                    parent = k[:slash_index] if slash_index != -1 else ''


fig = px.treemap(
    names=["Eve", "Cain", "Seth", "Enos", "Noam", "Abel", "Awan", "Enoch", "Azura"],
    parents=["", "Eve", "Eve", "Seth", "Seth", "Eve", "Eve", "Awan", "Eve"],
)
fig.update_traces(root_color="lightgrey")
fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
fig.show()

if __name__ == '__main__':
    main()
