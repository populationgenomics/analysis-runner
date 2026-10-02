"""
Analysis-runner server
"""

from dataclasses import dataclass

import pulumi
import pulumi_gcp as gcp

from components.common import protected

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()
SERVER_SA_ID = config.require('server_sa_id')
CPG_CONFIG_BUCKET = config.require('cpg_config_bucket')
MEMBERS_CACHE_LOCATION = config.require('members_cache_location')
ORG_ID = config.require('org_id')

# Custom org role: read objects, and create new ones without overwriting.
STORAGE_VIEWER_AND_CREATOR = (
    f'organizations/{ORG_ID}/roles/StorageViewerAndCreator'  # TODO check
)


@dataclass(frozen=True)
class ServerSpec:
    name: str
    cpu: str
    memory: str
    timeout: str
    service_max_instances: int | None = None


AR_SERVERS = [ServerSpec(**spec) for spec in config.require_object('server_services')]


def create_server_resources() -> dict[str, pulumi.Resource]:
    server_sa = gcp.serviceaccount.Account(
        'analysis-runner-server-sa',
        account_id=SERVER_SA_ID,
        project=PROJECT,
        display_name=SERVER_SA_ID,
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

    # Submission metadata which the metamist consumer subscribes on to.
    submissions_topic = gcp.pubsub.Topic(
        'submissions-topic',
        name='submissions',
        project=PROJECT,
        opts=pulumi.ResourceOptions(protect=True),
    )
    gcp.pubsub.TopicIAMMember(
        'submissions-topic-server-publisher',
        project=PROJECT,
        topic=submissions_topic.id,
        role='roles/pubsub.publisher',
        member=server_member,
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Server, driver, web and dataproc images.
    images_repo = gcp.artifactregistry.Repository(
        'images-repository',
        repository_id='images',
        location=REGION,
        project=PROJECT,
        format='DOCKER',
        cleanup_policy_dry_run=True,
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

    # create cloud run services
    services = {
        spec.name: _create_cloud_run_server(spec, server_sa) for spec in AR_SERVERS
    }

    return {
        'server_sa': server_sa,
        'submissions_topic': submissions_topic,
        'images_repo': images_repo,
        'cpg_config_bucket': cpg_config_bucket,
        **services,
    }


def _create_cloud_run_server(
    spec: ServerSpec, server_sa: gcp.serviceaccount.Account
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
                    # TODO check this
                    image=f'{REGION}-docker.pkg.dev/{PROJECT}/images/server:latest',
                    envs=[
                        # TODO check this
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='DRIVER_IMAGE',
                            value=f'{REGION}-docker.pkg.dev/{PROJECT}/images/driver:latest',
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='MEMBERS_CACHE_LOCATION',
                            value=MEMBERS_CACHE_LOCATION,
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='SERVER_CONFIG',
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret='server-config',
                                    version='latest',
                                ),
                            ),
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
        # TODO check here
        opts=protected(
            ignore_changes=[
                # Owned by the deploy workflow in the analysis-runner repo
                'template.containers[0].image',
                'template.containers[0].envs[0].value',  # DRIVER_IMAGE
                # Written by gcloud on every deploy
                'client',
                'clientVersion',
                'template.labels',
                'template.annotations',
            ],
        ),
    )
