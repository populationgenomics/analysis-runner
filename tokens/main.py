"""Copies Hail tokens from Kubernetes to the Google Secret Manager."""

from typing import List, NamedTuple
import kubernetes.client
import kubernetes.config
from google.cloud import secretmanager


class DatasetConfig(NamedTuple):
    """The configuration for a particular dataset:
    gcp_project_id: The GCP project ID associated with the dataset.
    allowed_repositories: The repositories allowed for running analyses.
    """

    gcp_project_id: str
    allowed_repositories: List[str]


# Per-dataset configuration.
CONFIG = {
    'tob-wgs': DatasetConfig('tob-wgs', ['tob-wgs']),
}

kubernetes.config.load_kube_config()
kube_client = kubernetes.client.CoreV1Api()

secret_manager = secretmanager.SecretManagerServiceClient()


def add_secret(gcp_project_id: str, name: str, value: str) -> None:
    """Adds the given secret to the Secret Manager as a new version."""
    payload = value.encode('UTF-8')
    secret_path = secret_manager.secret_path(gcp_project_id, name)
    response = secret_manager.add_secret_version(
        request={'parent': secret_path, 'payload': {'data': payload}}
    )
    print(response.name)


for dataset_name, dataset_config in CONFIG.items():
    kube_secret_name = f'{dataset_name}-tokens'
    kube_secret = kube_client.read_namespaced_secret(kube_secret_name, 'default')
    hail_token = kube_secret.data['tokens.json']

    add_secret(dataset_config.gcp_project_id, 'hail-token', hail_token)

    add_secret(
        dataset_config.gcp_project_id,
        'allowed-repositories',
        ','.join(dataset_config.allowed_repositories),
    )
