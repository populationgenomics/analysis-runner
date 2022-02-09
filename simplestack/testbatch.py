"""This script should be """
import subprocess

import hailtop.batch as hb
import click
import requests

CLOUD_RUN_URL = "https://<CLOUD RUN URL HERE>"


@click.argument("--environment", choices=['azure', 'gcp'])
def main(environment):

    sb = hb.ServiceBackend()
    b = hb.Batch(backend=sb)
    j = b.new_python_job('read_file')

    # get file from some server that requires authentication
    file_to_read = get_filename_from_server(environment)

    if environment == 'gcp':
        j.call(read_file_from_gcs, file_to_read)
    elif environment == 'azure':
        j.call(read_file_from_azure_blob_storage, file_to_read)

    b.run(wait=False)
    print(f"Batch ID: {b.id}")


def get_token(environment) -> str:
    """Get a GCP / Azure token based on environment"""

    if environment == 'gcp':
        return (
            subprocess.check_output(['gcloud', 'auth', 'print-identity-token'])
            .decode()
            .strip()
        )
    elif environment == 'az':
        raise NotImplementedError


def get_filename_from_server(environment) -> str:
    """Get a filename to READ from the server"""

    token = get_token(environment)
    res = requests.get(CLOUD_RUN_URL, headers={'Authorization': f'Bearer {token}'})
    res.raise_for_status()

    rjson = res.json()
    whoami = rjson.get('whoami')
    print(f"Server thinks I'm {whoami}")

    return rjson[environment]


def read_file_from_azure_blob_storage(filename):
    """Read a file from Azure Blob Storage"""
    raise NotImplementedError


def read_file_from_gcs(filename):
    """Read a file from Google Cloud Storage"""
    raise NotImplementedError


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
