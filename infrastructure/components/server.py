"""
Analysis-runner server
"""

from dataclasses import dataclass

import pulumi
import pulumi_gcp as gcp

from util.naming_utils import member_resource_name

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()
CPG_CONFIG_BUCKET = config.require('cpg_config_bucket')
ORG_ID = config.require('org_id')

SERVER_IMAGE_TAG = config.get('server_image_tag')

if not SERVER_IMAGE_TAG:
    raise ValueError('Missing server_image_tag config')

# Custom org role: read objects, and create new ones without overwriting.
STORAGE_VIEWER_AND_CREATOR = f'organizations/{ORG_ID}/roles/StorageViewerAndCreator'


@dataclass(frozen=True)
class ServerSpec:
    name: str
    cpu: str
    memory: str
    timeout: str
    service_max_instances: int | None = None

    env: dict[str, str] | None = None


AR_SERVERS = [ServerSpec(**spec) for spec in config.require_object('server_services')]
SERVER_INVOKERS: list[str] = config.get_object('server_invokers') or []
MEMBERS_CACHE_BUCKET = config.require('members_cache_bucket')


SERVER_SECRETS: list[str] = config.require_object('server_secrets')
SERVER_CONFIG_SECRET = 'server-config'  # noqa: S105 - secret name, not a value


# TODO remove this after recreating prod secrets
PIN_SECRETS_TO_REGION = config.get_bool('pin_secrets_to_region') or False


def create_server_resources() -> dict[str, pulumi.Resource]:

    server_sa = gcp.serviceaccount.Account(
        'analysis-runner-server-sa',
        account_id='analysis-runner-server',
        project=PROJECT,
        display_name='analysis-runner-server',
        description='Runs the analysis-runner server',
        opts=pulumi.ResourceOptions(protect=True),
    )
    server_member = server_sa.email.apply(lambda e: f'serviceAccount:{e}')

    gcp.projects.IAMMember(
        'analysis-runner-server-log-writer',
        project=PROJECT,
        role='roles/logging.logWriter',
        member=server_member,
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Config templates and run configs from submissions reside here.
    cpg_config_bucket = gcp.storage.Bucket(
        'cpg-config-bucket',
        name=CPG_CONFIG_BUCKET,
        location=REGION.upper(),
        storage_class='STANDARD',
        uniform_bucket_level_access=True,
        public_access_prevention='enforced',
        versioning=gcp.storage.BucketVersioningArgs(enabled=True),
        opts=pulumi.ResourceOptions(protect=True),
    )
    gcp.storage.BucketIAMMember(
        'cpg-config-server-viewer-and-creator',
        bucket=cpg_config_bucket.name,
        role=STORAGE_VIEWER_AND_CREATOR,
        member=server_member,
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Grant dataset test groups, read access to dev run config. In production setup, this is already handled in cpg-infra
    # Read access for run config jobs
    cpg_config_readers: list[str] = config.get_object('cpg_config_readers') or []
    for member in cpg_config_readers:
        gcp.storage.BucketIAMMember(
            f'cpg-config-reader-{member_resource_name(member)}',
            bucket=cpg_config_bucket.name,
            role='roles/storage.objectViewer',
            member=member,
        )

    # Create secrets and add their accessors.
    # Secret values are not managed via IaC.
    # Cromwell secrets are managed in cpg-infra
    secrets: dict[str, gcp.secretmanager.Secret] = {}
    secret_accessors: list[gcp.secretmanager.SecretIamMember] = []
    for secret_name in SERVER_SECRETS:
        secret = gcp.secretmanager.Secret(
            f'{secret_name}-secret',
            secret_id=secret_name,
            project=PROJECT,
            replication=(
                gcp.secretmanager.SecretReplicationArgs(
                    user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                        replicas=[
                            gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                                location=REGION
                            ),
                        ],
                    ),
                )
                # TODO remove this after updating production
                if PIN_SECRETS_TO_REGION
                else gcp.secretmanager.SecretReplicationArgs(
                    auto=gcp.secretmanager.SecretReplicationAutoArgs(),
                )
            ),
            opts=pulumi.ResourceOptions(protect=True),
        )
        secrets[secret_name] = secret
        secret_accessors.append(
            gcp.secretmanager.SecretIamMember(
                f'{secret_name}-server-accessor',
                project=PROJECT,
                secret_id=secret.secret_id,
                role='roles/secretmanager.secretAccessor',
                member=server_member,
                opts=pulumi.ResourceOptions(protect=True),
            )
        )

    # create cloud run services
    services = {
        spec.name: _create_cloud_run_server(
            spec,
            server_sa,
            server_config_secret=secrets[SERVER_CONFIG_SECRET],
            depends_on=secret_accessors,
        )
        for spec in AR_SERVERS
    }

    # Grant invoker roles for cloud run services
    for service_name, service in services.items():
        for member in SERVER_INVOKERS:
            gcp.cloudrunv2.ServiceIamMember(
                f'{service_name}-invoker-{member_resource_name(member)}',
                project=PROJECT,
                location=REGION,
                name=service.name,
                role='roles/run.invoker',
                member=member,
            )

    # created In the common stack, the server's read access is granted here.
    if config.get_bool('grant_members_cache_read'):
        gcp.storage.BucketIAMMember(
            'members-cache-server-viewer',
            bucket=MEMBERS_CACHE_BUCKET,
            role='roles/storage.objectViewer',
            member=server_member,
            opts=pulumi.ResourceOptions(protect=True),
        )

    return {
        'server_sa': server_sa,
        'cpg_config_bucket': cpg_config_bucket,
        **secrets,
        **services,
    }


def _create_cloud_run_server(
    spec: ServerSpec,
    server_sa: gcp.serviceaccount.Account,
    server_config_secret: gcp.secretmanager.Secret,
    depends_on: list[pulumi.Resource],
) -> gcp.cloudrunv2.Service:
    return gcp.cloudrunv2.Service(
        spec.name,
        name=spec.name,
        location=REGION,
        project=PROJECT,
        ingress='INGRESS_TRAFFIC_ALL',
        deletion_protection=True,
        scaling=(
            gcp.cloudrunv2.ServiceScalingArgs(
                max_instance_count=spec.service_max_instances
            )
            if spec.service_max_instances
            else None
        ),
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            service_account=server_sa.email,
            timeout=spec.timeout,
            max_instance_request_concurrency=80,
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(max_instance_count=100),
            containers=[
                gcp.cloudrunv2.ServiceTemplateContainerArgs(
                    name='server-1',
                    image=f'{REGION}-docker.pkg.dev/{PROJECT}/images/server:{SERVER_IMAGE_TAG}',
                    envs=[
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='DRIVER_IMAGE',
                            value=f'{REGION}-docker.pkg.dev/{PROJECT}/images/driver:{SERVER_IMAGE_TAG}',
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='MEMBERS_CACHE_LOCATION',
                            value=f'gs://{MEMBERS_CACHE_BUCKET}',
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='SERVER_CONFIG',
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=server_config_secret.secret_id,
                                    version='latest',
                                ),
                            ),
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='CONFIG_PATH_PREFIX',
                            value=f'gs://{CPG_CONFIG_BUCKET}',
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='ANALYSIS_RUNNER_PROJECT_ID',
                            value=PROJECT,
                        ),
                        *(
                            gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                                name=name, value=value
                            )
                            for name, value in (spec.env or {}).items()
                        ),
                    ],
                    ports=gcp.cloudrunv2.ServiceTemplateContainerPortsArgs(
                        container_port=8080, name='http1'
                    ),
                    resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                        limits={'cpu': spec.cpu, 'memory': spec.memory},
                        cpu_idle=True,
                        startup_cpu_boost=True,
                    ),
                    startup_probe=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeArgs(
                        failure_threshold=1,
                        period_seconds=240,
                        timeout_seconds=240,
                        tcp_socket=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeTcpSocketArgs(
                            port=8080
                        ),
                    ),
                ),
            ],
        ),
        traffics=[
            gcp.cloudrunv2.ServiceTrafficArgs(
                type='TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST', percent=100
            ),
        ],
        opts=pulumi.ResourceOptions(
            protect=True,
            depends_on=depends_on,
            ignore_changes=[
                # Written by gcloud on every deploy #TODO remove these
                'client',
                'clientVersion',
            ],
        ),
    )
