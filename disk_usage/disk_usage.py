#!/usr/bin/env python3

"""Computes aggregate bucket disk usage stats."""

from collections import defaultdict
import json
import logging
from cloudpathlib import AnyPath
from cpg_utils.hail_batch import get_config, output_path
from google.cloud import storage


# It's important not to list the `archive` bucket here, as Class B operations are very
# expensive for that storage class.
BUCKET_SUFFIXES = [
    'main',
    'main-analysis',
    'main-tmp',
    'main-upload',
    'main-web',
    'test',
    'test-analysis',
    'test-tmp',
    'test-upload',
    'test-web',
]


def aggregate_level(name: str) -> str:
    """Returns a prefix for the given blob name at the folder or Hail table level."""
    ht_index = name.find('.ht/')
    if ht_index != -1:
        return name[: ht_index + 3]
    mt_index = name.find('.mt/')
    if mt_index != -1:
        return name[: mt_index + 3]
    slash_index = name.rfind('/')
    if slash_index == -1:
        return ''  # Root level
    return name[:slash_index]


def main():
    """Main entrypoint."""
    # Don't print DEBUG logs from urllib3.connectionpool.
    logging.getLogger().setLevel(logging.INFO)

    storage_client = storage.Client()
    dataset = get_config()['workflow']['dataset']
    access_level = get_config()['workflow']['access_level']

    aggregate_stats = defaultdict(lambda: defaultdict(int))
    for bucket_suffix in BUCKET_SUFFIXES:
        if access_level == 'test' and not bucket_suffix.startswith('test'):
            continue  # Skip main buckets when testing.

        bucket_name = f'cpg-{dataset}-{bucket_suffix}'
        logging.info(f'Listing blobs in {bucket_name}...')
        blobs = storage_client.list_blobs(bucket_name)
        count = 0
        for blob in blobs:
            count += 1
            if count % 10**6 == 0:
                logging.info(f'{count // 10**6} M blobs...')
            folder = aggregate_level(blob.name)
            last_index = 0
            while True:
                index = folder.find('/', last_index)
                substr = folder[:index] if index != -1 else folder
                path = f'gs://{bucket_name}/{substr}'
                stats = aggregate_stats[path]
                stats['size'] += blob.size
                stats['num_blobs'] += 1

                if index == -1:
                    break

                last_index = index + 1

        logging.info(f'{bucket_name} contains {count} blobs.')

    output = output_path('disk_usage.json')
    logging.info(f'Writing results to {output}...')
    with AnyPath(output).open('wt') as f:
        json.dump(aggregate_stats, f)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
