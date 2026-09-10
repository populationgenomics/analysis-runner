#!/usr/bin/env python3

"""
Upload files (usually zip archives) from GCS paths to the specified
(already created, unpublished) Zenodo deposit.

Typical usage:

analysis-runner --dataset DATASET --description 'Upload to Zenodo' \
    --access-level standard --output-dir unused --storage SIZE \
    --env ZENODO_TOKEN=TOKEN \
    python3 scripts/upload_to_zenodo.py --deposit ID GCSFILE...

Each GCSFILE may optionally be suffixed with #NEWNAME to give the file
a different name when it is attached to the Zenodo deposit.

The script will need storage space for one zip archive at a time,
so storage should be set sufficient for the size of the largest archive.

Uploads use the InvenioRDM multipart transfer (Zenodo runs on InvenioRDM):
the file is registered once with a declared part count, each part is PUT
independently (in parallel, with per-part retries), then committed. A dropped
connection only costs the part in flight rather than the whole archive.
"""

import hashlib
import math
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import click
import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter, Retry

storage_client = storage.Client()

RETRYABLE_STATUSES = (429, 500, 502, 503, 504)
RETRYABLE_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
MD5_CHUNK = 8 * 1024 * 1024
MIB = 1024 * 1024

# Zenodo may compute the checksum of a committed multipart file asynchronously.
CHECKSUM_POLL_INTERVAL = 10
CHECKSUM_POLL_LIMIT = 600


class ZenodoClient:
    """Thin wrapper over the InvenioRDM draft-files API for one record."""

    def __init__(self, host: str, record_id: str, token: str, timeout: float) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {token}'
        retry = Retry(
            total=5,
            connect=5,
            read=3,
            status=5,
            backoff_factor=2,
            status_forcelist=RETRYABLE_STATUSES,
            allowed_methods=frozenset({'GET', 'DELETE', 'POST'}),
            raise_on_status=False,
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

        draft_url = f'https://{host}/api/records/{record_id}/draft'
        response = self.session.get(draft_url, timeout=timeout)
        response.raise_for_status()
        self.files_url = response.json()['links']['files']

    def _json(self, method: str, url: str, **kwargs: Any) -> dict:
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if not response.ok:
            raise RuntimeError(
                f'{method} {url} -> HTTP {response.status_code}: {response.text[:500]}'
            )
        return response.json() if response.content else {}

    def existing_keys(self) -> set[str]:
        body = self._json('GET', self.files_url)
        return {entry['key'] for entry in body.get('entries', [])}

    def delete_file(self, key: str):
        print(f'Removing existing draft file {key}', flush=True)
        self._json('DELETE', f'{self.files_url}/{key}')

    def init_multipart(self, key: str, size: int, parts: int, part_size: int) -> dict:
        """Register the file; returns the file entry (with links.parts and links.commit)."""
        payload = [
            {
                'key': key,
                'size': size,
                'transfer': {'type': 'M', 'parts': parts, 'part_size': part_size},
            },
        ]
        body = self._json('POST', self.files_url, json=payload)
        entry = next(e for e in body['entries'] if e['key'] == key)
        links = entry['links']
        if 'parts' not in links:
            raise RuntimeError(
                f'Server did not return multipart part links for {key}: {links}'
            )
        return entry

    def put_part(self, url: str, data: bytes):
        """Single attempt at one part. Bare request so retries are controlled by the caller."""
        response = requests.put(
            url,
            data=data,
            headers={
                'Authorization': self.session.headers['Authorization'],
                'Content-Type': 'application/octet-stream',
                'Content-Length': str(len(data)),
            },
            timeout=self.timeout,
        )
        if response.status_code in RETRYABLE_STATUSES:
            raise requests.ConnectionError(
                f'HTTP {response.status_code}: {response.text[:200]}'
            )
        response.raise_for_status()

    def commit(self, entry: dict) -> dict:
        return self._json('POST', entry['links']['commit'])

    def file_entry(self, key: str) -> dict:
        return self._json('GET', f'{self.files_url}/{key}')


def md5_of(path: str) -> str:
    """Hex MD5 of a file, read in chunks."""
    digest = hashlib.md5(usedforsecurity=False)
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(MD5_CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def download_from_gcs(local_path: str, gcs_path: str):
    (bucket_name, path) = gcs_path.removeprefix('gs://').split('/', maxsplit=1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)
    print(f'Downloading {gcs_path} into {local_path}', flush=True)
    blob.download_to_filename(local_path)


def read_part(local_path: str, part_no: int, part_size: int) -> bytes:
    """Read the bytes for 1-indexed part_no."""
    with open(local_path, 'rb') as fh:
        fh.seek((part_no - 1) * part_size)
        return fh.read(part_size)


def upload_part_with_retries(
    client: ZenodoClient,
    local_path: str,
    part_no: int,
    part_size: int,
    url: str,
    attempts: int,
):
    data = read_part(local_path, part_no, part_size)
    for attempt in range(1, attempts + 1):
        try:
            client.put_part(url, data)
            return part_no
        except RETRYABLE_ERRORS as exc:
            problem = str(exc)
        if attempt == attempts:
            raise RuntimeError(
                f'Part {part_no} failed after {attempts} attempts; last: {problem}'
            )
        delay = 5 * 2**attempt
        print(
            f'Part {part_no} attempt {attempt} failed ({problem}); retrying in {delay}s',
            flush=True,
        )
        time.sleep(delay)
    raise AssertionError('unreachable')  # pragma: no cover


def wait_for_checksum(client: ZenodoClient, key: str, committed: dict) -> str:
    """Return the md5 hex Zenodo reports, polling if it is computed asynchronously."""
    deadline = time.monotonic() + CHECKSUM_POLL_LIMIT
    entry = committed
    while True:
        checksum = (entry.get('checksum') or '').removeprefix('md5:')
        if checksum:
            return checksum
        if time.monotonic() > deadline:
            raise RuntimeError(
                f'No checksum reported for {key} after {CHECKSUM_POLL_LIMIT}s'
            )
        print('Waiting for Zenodo to compute checksum', flush=True)
        time.sleep(CHECKSUM_POLL_INTERVAL)
        entry = client.file_entry(key)


def multipart_upload(
    client: ZenodoClient,
    local_path: str,
    key: str,
    part_size: int,
    workers: int,
    attempts: int,
) -> int:
    """Upload local_path as key using the multipart transfer. Returns the verified size."""
    print(f'Computing MD5 of {local_path}', flush=True)
    local_md5 = md5_of(local_path)

    size = os.path.getsize(local_path)
    parts = max(1, math.ceil(size / part_size))
    print(f'{size} bytes in {parts} part(s) of {part_size} bytes', flush=True)

    if key in client.existing_keys():
        client.delete_file(key)

    entry = client.init_multipart(key, size, parts, part_size)
    part_urls = {p['part']: p['url'] for p in entry['links']['parts']}
    missing = set(range(1, parts + 1)) - part_urls.keys()
    if missing:
        raise RuntimeError(f'Server returned no upload URL for parts {sorted(missing)}')

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                upload_part_with_retries,
                client,
                local_path,
                n,
                part_size,
                part_urls[n],
                attempts,
            )
            for n in range(1, parts + 1)
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            future.result()  # re-raise the first failure; remaining futures finish or fail on their own
            if done % 10 == 0 or done == parts:
                print(f'{done}/{parts} parts uploaded', flush=True)

    print('Committing', flush=True)
    committed = client.commit(entry)
    remote_md5 = wait_for_checksum(client, key, committed)
    if remote_md5 != local_md5:
        raise RuntimeError(
            f'Checksum mismatch for {key} (local {local_md5}, Zenodo {remote_md5})'
        )
    print(f'Checksum verified ({local_md5})', flush=True)
    return int(committed.get('size') or size)


@click.command(no_args_is_help=True)
@click.option(
    '--deposit',
    required=True,
    help='Deposit (record) ID to which files will be uploaded',
)
@click.option(
    '--sandbox',
    is_flag=True,
    help='Upload to sandbox instead of real Zenodo',
)
@click.option(
    '--timeout',
    default=600.0,
    help='Request timeout (in seconds), applied per part',
)
@click.option(
    '--attempts',
    default=5,
    help='Maximum upload attempts per part',
)
@click.option(
    '--part-size-mib',
    default=100,
    help='Multipart part size in MiB',
)
@click.option(
    '--workers',
    default=4,
    help='Parallel part uploads (memory use is roughly workers * part size)',
)
@click.option(
    '--token',
    envvar='ZENODO_TOKEN',
    help='Authentication token for Zenodo',
)
@click.argument(
    'files',
    nargs=-1,
)
def main(
    deposit: str,
    sandbox: bool,
    timeout: float,
    attempts: int,
    part_size_mib: int,
    workers: int,
    token: str,
    files: tuple[str],
):
    """
    Each zip archive listed in FILES is uploaded to the specified Zenodo deposit.
    The authentication token can also be specified via the ZENODO_TOKEN environment variable.
    """
    zenodo_host = 'sandbox.zenodo.org' if sandbox else 'zenodo.org'
    client = ZenodoClient(zenodo_host, deposit, token, timeout)
    part_size = part_size_mib * MIB

    tmpdir = os.environ.get('BATCH_TMPDIR') or tempfile.gettempdir()

    for file_newname in files:
        file, _, newname = file_newname.partition('#')
        basename = file.rsplit('/', maxsplit=1)[-1]
        tmp_filename = os.path.join(tmpdir, basename)

        download_from_gcs(tmp_filename, file)

        key = newname or basename
        print(f'Uploading {basename} to {zenodo_host} as {key}', flush=True)
        size = multipart_upload(client, tmp_filename, key, part_size, workers, attempts)
        print(f'Uploaded {size} bytes', flush=True)
        print(flush=True)

        os.remove(tmp_filename)


if __name__ == '__main__':
    main()
