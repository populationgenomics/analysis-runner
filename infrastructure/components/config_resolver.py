"""
Cloud Run Job to resolve Seqera workflow configs in Metamist.
Deployed as its own stack, after the server stack, which owns the config bucket and
the secrets.
"""

import pulumi
import pulumi_gcp as gcp

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()

ORG_ID = config.require('org_id')
CPG_CONFIG_BUCKET = config.require('cpg_config_bucket')
# server_image_tag is injected in deploy GHA,
SERVER_IMAGE_TAG = config.require('server_image_tag')
CONFIG_RESOLVER = 'config-resolver'

CONFIG_RESOLVER_SCHEDULE = '*/5 * * * *'
CONFIG_RESOLVER_SECRETS: list[str] = config.require_object('config_resolver_secrets')
CONFIG_RESOLVER_ENV: dict[str, str] = config.get_object('config_resolver_env') or {}


def create_config_resolver_resources() -> dict[str, pulumi.Resource]:

    job_sa = gcp.serviceaccount.Account(
        f'{CONFIG_RESOLVER}-sa',
        account_id=CONFIG_RESOLVER,
        project=PROJECT,
        display_name=CONFIG_RESOLVER,
        description='Cloud run job to resolve Analysis Runner configs',
    )
    job_member = job_sa.email.apply(lambda e: f'serviceAccount:{e}')

    # Resolved configs are written to the config bucket
    gcp.storage.BucketIAMMember(
        f'cpg-config-{CONFIG_RESOLVER}-viewer-and-creator',
        bucket=CPG_CONFIG_BUCKET,
        role=f'organizations/{ORG_ID}/roles/StorageViewerAndCreator',
        member=job_member,
    )

    # In Development, seqera launch tokens -> created in the server stack.
    # In production, the seqera-platform-config managed in cpg-infra.
    secret_accessors = [
        gcp.secretmanager.SecretIamMember(
            f'{secret_name}-{CONFIG_RESOLVER}-accessor',
            project=PROJECT,
            secret_id=secret_name,
            role='roles/secretmanager.secretAccessor',
            member=job_member,
        )
        for secret_name in CONFIG_RESOLVER_SECRETS
    ]

    job = gcp.cloudrunv2.Job(
        CONFIG_RESOLVER,
        name=CONFIG_RESOLVER,
        location=REGION,
        project=PROJECT,
        deletion_protection=False,
        template=gcp.cloudrunv2.JobTemplateArgs(
            task_count=1,
            template=gcp.cloudrunv2.JobTemplateTemplateArgs(
                service_account=job_sa.email,
                # Unresolved entries are retried by the next scheduled run
                # The cloud run job has default timeout of 10 mins
                max_retries=0,
                containers=[
                    gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                        # The server image also contains the job
                        image=f'{REGION}-docker.pkg.dev/{PROJECT}/images/server:{SERVER_IMAGE_TAG}',
                        commands=['python', 'seqera_config_resolver.py'],
                        envs=[
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='CONFIG_PATH_PREFIX',
                                value=f'gs://{CPG_CONFIG_BUCKET}',
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='ANALYSIS_RUNNER_PROJECT_ID',
                                value=PROJECT,
                            ),
                            *(
                                gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                    name=name, value=value
                                )
                                for name, value in CONFIG_RESOLVER_ENV.items()
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
        f'{CONFIG_RESOLVER}-scheduler-sa',
        account_id=f'{CONFIG_RESOLVER}-scheduler',
        project=PROJECT,
        display_name=f'{CONFIG_RESOLVER}-scheduler',
        description='Runs the Analysis Runner Config Resolver job on schedule',
    )

    gcp.cloudrunv2.JobIamMember(
        f'{CONFIG_RESOLVER}-scheduler-executor',
        project=PROJECT,
        location=REGION,
        name=job.name,
        role='roles/run.invoker',
        member=scheduler_sa.email.apply(lambda e: f'serviceAccount:{e}'),
    )

    is_paused = config.get_bool('config_resolver_paused') or False

    scheduler = gcp.cloudscheduler.Job(
        f'{CONFIG_RESOLVER}-scheduler',
        name=CONFIG_RESOLVER,
        project=PROJECT,
        region=REGION,
        description='Runs the Analysis Runner Config Resolver job',
        schedule=CONFIG_RESOLVER_SCHEDULE,
        time_zone='UTC',
        paused=is_paused,
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
                scope='https://www.googleapis.com/auth/cloud-platform',
            ),
        ),
    )

    return {
        f'{CONFIG_RESOLVER}_sa': job_sa,
        f'{CONFIG_RESOLVER}_job': job,
        f'{CONFIG_RESOLVER}_scheduler': scheduler,
    }
