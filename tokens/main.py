import json
from kubernetes import client, config
from google.cloud import secretmanager

PROJECTS = [
    'tob-wgs',
]

GCP_PROJECT = 'analysis-runner'
SECRET_MANAGER_SECRET_NAME = 'hail-tokens'

config.load_kube_config()
kube_client = client.CoreV1Api()

tokens = {}
for project in PROJECTS:
    secret_name = f'{project}-tokens'
    secret = kube_client.read_namespaced_secret(secret_name, 'default')
    tokens[project] = secret.data['tokens.json']

secret_manager = secretmanager.SecretManagerServiceClient()
payload = json.dumps(tokens).encode('UTF-8')
parent = secret_manager.secret_path(GCP_PROJECT, SECRET_MANAGER_SECRET_NAME)
response = secret_manager.add_secret_version(
    request={"parent": parent, "payload": {"data": payload}}
)

print(f'Added secret version: {response.name}')