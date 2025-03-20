#!/usr/bin/env python3

"""
Create zip archives of the files in the specified GCS tree and
write them to the specified output GCS location.

Typical usage:

analysis-runner --dataset DATASET --description 'Zip some trees' \
    --access-level standard --output-dir unused --storage 10G \
    python3 scripts/zip_gcs_trees.py --output-base gs://BUCKET/PATH \
        --basedir gs://BUCKET/PATH ABC DEF GHI

The script will need storage space for one zip archive at a time,
so storage should be set to the expected size of the largest archive.
"""

import os
import re
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import click
from google.cloud import storage

storage_client = storage.Client()


def zip_tree(
    zip_fname: str,
    bucket: str,
    prefix: str,
    subset: str,
    regex: re.Pattern,
    maxfiles: int | None,
):
    """
    Create a zip archive containing the files within the directory subtree.
    """
    print(f'Creating {zip_fname} from gs://{bucket}/{prefix}/{subset}/**')
    with ZipFile(zip_fname, mode='w') as zipf:
        nfiles = 0
        for blob in storage_client.list_blobs(bucket, prefix=f'{prefix}/{subset}/'):
            subname = blob.name.removeprefix(prefix).removeprefix('/')
            if not regex.fullmatch(subname):
                continue

            if nfiles <= 100:  # noqa: PLR2004  # Literal 100 is clear enough here!
                print(f'Adding {subname} to archive')
            elif nfiles % 100 == 0:
                print(f'Adding a hundred files through {subname} to archive')

            contents = blob.download_as_bytes()
            info = ZipInfo(filename=subname, date_time=blob.updated.utctimetuple())
            zipf.writestr(info, contents, compress_type=ZIP_DEFLATED, compresslevel=9)

            nfiles += 1
            if maxfiles is not None and nfiles >= maxfiles:
                break

    print(f'Added {nfiles} files to {zip_fname}')


def gcs_copy(gcs_path: str, local_path: str):
    blob = storage.blob.Blob.from_uri(gcs_path, client=storage_client)

    print(f'Writing {local_path} to {gcs_path}')
    blob.upload_from_filename(local_path)
    print(f'Wrote {os.path.getsize(local_path)} bytes')


@click.command(no_args_is_help=True)
@click.option(
    '--basedir',
    required=True,
    help='Base of the GCS directory tree to zip',
)
@click.option(
    '--output-base',
    required=True,
    help='GCS location where zip archives should be written',
)
@click.option(
    '--pattern',
    default='.*',
    help='Add only files with SUBSET/BASENAME matching the pattern',
)
@click.option(
    '--limit',
    default=-1,
    help='Maximum number of files to add [for testing]',
)
@click.argument(
    'subsets',
    nargs=-1,
)
def main(
    basedir: str,
    output_base: str,
    pattern: str,
    limit: int,
    subsets: tuple[str],
):
    """
    Each SUBSET specifies a subdirectory of BASEDIR whose contents will be individually zipped.
    These zip archives are then written to OUTPUT-BASE.
    """
    regex = re.compile(pattern)

    (bucket, prefix) = basedir.removeprefix('gs://').split('/', maxsplit=1)
    for subset in subsets:
        zip_fname = f'{subset}.zip'
        zip_tree(zip_fname, bucket, prefix, subset, regex, limit if limit > 0 else None)

        gcs_copy(f'{output_base}/{zip_fname}', zip_fname)
        os.remove(zip_fname)
        print()


if __name__ == '__main__':
    main()
