#!/usr/bin/env python3

"""Computes aggregate bucket disk usage stats."""

from collections import defaultdict
import logging
from cloudpathlib import AnyPath
from cpg_utils.hail_batch import get_config, output_path
from google.cloud import storage


# It's important not to list the `archive` bucket here, as Class B operations are very
# expensive for that storage class.
BUCKET_SUFFIXES = [
    # 'main',
    # 'main-analysis',
    # 'main-tmp',
    # 'main-upload',
    # 'main-web',
    'test',
    'test-analysis',
    'test-tmp',
    'test-upload',
    'test-web',
]


def aggregate_level(name: str) -> str:
    """Returns a prefix for the given blob name at the aggregation level."""
    ht_index = name.find('.ht/')
    if ht_index != -1:
        return name[: ht_index + 3]
    mt_index = name.find('.mt/')
    if mt_index != -1:
        return name[: mt_index + 3]
    slash_index = name.find('/')
    if slash_index != -1:
        next_slash = name.find('/', slash_index + 1)
        if next_slash != -1:
            slash_index = next_slash
    return name[:slash_index]


def main():
    """Main entrypoint."""
    # Don't print DEBUG logs from urllib3.connectionpool
    logging.getLogger().setLevel(logging.INFO)

    storage_client = storage.Client()
    dataset = get_config()['workflow']['dataset']

    aggregate_size = defaultdict(int)
    for bucket_suffix in BUCKET_SUFFIXES:
        bucket_name = f'cpg-{dataset}-{bucket_suffix}'
        logging.info(f'Listing blobs in {bucket_name}...')
        blobs = storage_client.list_blobs(bucket_name)
        for index, blob in enumerate(blobs):
            if (index + 1) % 10**6 == 0:
                print(f'{(index + 1) / 10**6} M blobs...')
            name = f'gs://{bucket_name}/{aggregate_level(blob.name)}'
            aggregate_size[name] += blob.size

    sorted_entries = list(aggregate_size.items())
    sorted_entries.sort(key=lambda e: e[1], reverse=True)

    output = output_path('disk_usage.csv')
    logging.info(f'Writing results to {output}...')
    with AnyPath(output).open('wt') as f:
        print('\n'.join(f'{e[0]},{e[1]}' for e in sorted_entries), file=f)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
