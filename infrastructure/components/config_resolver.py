"""
Cloud Run Job to resolve Seqera workflow configs in Metamist
"""

import pulumi
import pulumi_gcp as gcp

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()

ORG_ID = config.require('org_id')
# server_image_tag is injected in deploy GHA,
SERVER_IMAGE_TAG = config.require('server_image_tag')
SEQERA_CONFIG_RESOLVER = 'seqera-config-resolver'

SEQERA_CONFIG_RESOLVER_TIMEOUT = '300s'
SEQERA_CONFIG_RESOLVER_SCHEDULE = '*/5 * * * *'
SEQERA_CONFIG_RESOLVER_ENV: dict[str, str] = (
    config.get_object('seqera_config_resolver_env') or {}
)


def create_seqera_config_resolver_resources(
    cpg_config_bucket: gcp.storage.Bucket,
    secrets: dict[str, gcp.secretmanager.Secret],
) -> dict[str, pulumi.Resource]:

    job_sa = gcp.serviceaccount.Account(
        f'{SEQERA_CONFIG_RESOLVER}-sa',
        account_id=SEQERA_CONFIG_RESOLVER,
        project=PROJECT,
        display_name=SEQERA_CONFIG_RESOLVER,
        description='Cloud run job to resolve Seqera workflow configs',
    )
    job_member = job_sa.email.apply(lambda e: f'serviceAccount:{e}')

    gcp.projects.IAMMember(
        f'{SEQERA_CONFIG_RESOLVER}-log-writer',
        project=PROJECT,
        role='roles/logging.logWriter',
        member=job_member,
    )

    # Resolved configs are written to the config bucket
    gcp.storage.BucketIAMMember(
        f'cpg-config-{SEQERA_CONFIG_RESOLVER}-viewer-and-creator',
        bucket=cpg_config_bucket.name,
        role=f'organizations/{ORG_ID}/roles/StorageViewerAndCreator',
        member=job_member,
    )

    # Seqera platform config and launch tokens managed in this stack.
    # In production, the seqera-platform-config accessor is managed in cpg-infra.
    secret_accessors = [
        gcp.secretmanager.SecretIamMember(
            f'{secret_name}-{SEQERA_CONFIG_RESOLVER}-accessor',
            project=PROJECT,
            secret_id=secret.secret_id,
            role='roles/secretmanager.secretAccessor',
            member=job_member,
        )
        for secret_name, secret in secrets.items()
        if secret_name.startswith('seqera-')
    ]

    job = gcp.cloudrunv2.Job(
        SEQERA_CONFIG_RESOLVER,
        name=SEQERA_CONFIG_RESOLVER,
        location=REGION,
        project=PROJECT,
        deletion_protection=False,
        template=gcp.cloudrunv2.JobTemplateArgs(
            task_count=1,
            template=gcp.cloudrunv2.JobTemplateTemplateArgs(
                service_account=job_sa.email,
                timeout=SEQERA_CONFIG_RESOLVER_TIMEOUT,
                # Unresolved entries are retried by the next scheduled run
                max_retries=0,
                containers=[
                    gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                        # The server image also contains the job
                        image=f'{REGION}-docker.pkg.dev/{PROJECT}/images/server:{SERVER_IMAGE_TAG}',
                        commands=['python', 'seqera_config_resolver.py'],
                        envs=[
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='CONFIG_PATH_PREFIX',
                                value=cpg_config_bucket.name.apply(
                                    lambda name: f'gs://{name}'
                                ),
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='ANALYSIS_RUNNER_PROJECT_ID',
                                value=PROJECT,
                            ),
                            *(
                                gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                    name=name, value=value
                                )
                                for name, value in SEQERA_CONFIG_RESOLVER_ENV.items()
                            ),
                        ],
                        resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                            limits={'cpu': '1', 'memory': '1Gi'},
                        ),
                    ),
                ],
            ),
        ),
        opts=pulumi.ResourceOptions(depends_on=secret_accessors),
    )

    scheduler_sa = gcp.serviceaccount.Account(
        f'{SEQERA_CONFIG_RESOLVER}-scheduler-sa',
        account_id=f'{SEQERA_CONFIG_RESOLVER}-scheduler',
        project=PROJECT,
        display_name=f'{SEQERA_CONFIG_RESOLVER}-scheduler',
        description='Runs the Seqera config resolver job on schedule',
    )

    gcp.cloudrunv2.JobIamMember(
        f'{SEQERA_CONFIG_RESOLVER}-scheduler-executor',
        project=PROJECT,
        location=REGION,
        name=job.name,
        role='roles/run.jobsExecutor',
        member=scheduler_sa.email.apply(lambda e: f'serviceAccount:{e}'),
    )

    scheduler = gcp.cloudscheduler.Job(
        f'{SEQERA_CONFIG_RESOLVER}-scheduler',
        name=SEQERA_CONFIG_RESOLVER,
        project=PROJECT,
        region=REGION,
        description='Runs the Seqera config resolver job',
        schedule=SEQERA_CONFIG_RESOLVER_SCHEDULE,
        time_zone='UTC',
        # A missed run is picked up by the next scheduled run
        retry_config=gcp.cloudscheduler.JobRetryConfigArgs(retry_count=0),
        http_target=gcp.cloudscheduler.JobHttpTargetArgs(
            http_method='POST',
            uri=job.name.apply(
                lambda name: (
                    f'https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/jobs/{name}:run'
                )
            ),
            oauth_token=gcp.cloudscheduler.JobHttpTargetOauthTokenArgs(
                service_account_email=scheduler_sa.email,
            ),
        ),
    )

    return {
        f'{SEQERA_CONFIG_RESOLVER}_sa': job_sa,
        f'{SEQERA_CONFIG_RESOLVER}_job': job,
        f'{SEQERA_CONFIG_RESOLVER}_scheduler': scheduler,
    }
