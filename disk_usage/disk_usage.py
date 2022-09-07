#!/usr/bin/env python3

"""Creates an aggregate report for disk usage for all buckets of a dataset, by
explicitly listing all blobs. In contrast to `gsutil du`, we aggregate at a 2-level
folder depth or at any `.ht` or `.mt` level."""

from collections import defaultdict
from cloudpathlib import AnyPath
from cpg_utils.hail_batch import get_config, output_path
from google.cloud import storage


BUCKET_SUFFIXES = [
    # 'archive',
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
    index = name.find('/')
    if index != -1:
        index = name.find('/', index + 1)
    return name[:index]


def main():
    """Main entrypoint."""
    storage_client = storage.Client()
    dataset = get_config()['workflow']['dataset']

    aggregate_size = defaultdict(int)
    for bucket_suffix in BUCKET_SUFFIXES:
        bucket_name = f'{dataset}-{bucket_suffix}'
        print(f'Listing blobs in {bucket_name}...')
        blobs = storage_client.list_blobs(bucket_name)
        for index, blob in enumerate(blobs):
            if ((index + 1) & (1 << 20)) == 0:
                print(f'{(index + 1) >> 20} Mi blobs...')
            name = f'gs://{bucket_name}/{aggregate_level(blob.name)}'
            aggregate_size[name] += blob.size

    sorted_entries = list(aggregate_size.items())
    sorted_entries.sort(key=lambda e: e[1], reverse=True)

    with AnyPath(output_path('disk_usage.csv', 'wt')).open() as f:
        print('\n'.join(f'{e[0]},{e[1]}' for e in sorted_entries), file=f)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
