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

SECRET_NAMES: list[str] = config.require_object('config_resolver_secrets')
JOB_ENV_VARIABLES: dict[str, str] = config.get_object('config_resolver_env') or {}

CONFIG_RESOLVER = 'config-resolver'

NOTIFICATION_CHANNEL = config.require('notification_channel')
UNRESOLVED_LOG_TEXT = (
    'Analysis Runner Config Resolver: Seqera submission config not resolved for'
)


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
        for secret_name in SECRET_NAMES
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
                                for name, value in JOB_ENV_VARIABLES.items()
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
        schedule='*/5 * * * *',
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

    unresolved_alert, failed_runs_alert = _create_alerts(job)

    return {
        f'{CONFIG_RESOLVER}_sa': job_sa,
        f'{CONFIG_RESOLVER}_job': job,
        f'{CONFIG_RESOLVER}_scheduler': scheduler,
        f'{CONFIG_RESOLVER}_unresolved_alert': unresolved_alert,
        f'{CONFIG_RESOLVER}_failed_runs_alert': failed_runs_alert,
    }


def _create_alerts(
    job: gcp.cloudrunv2.Job,
) -> tuple[gcp.monitoring.AlertPolicy, gcp.monitoring.AlertPolicy]:

    job_logs_filter = job.name.apply(
        lambda name: (
            f'resource.type="cloud_run_job" AND resource.labels.job_name="{name}"'
        )
    )

    # Counts the unresolved configs the job gave up on
    unresolved_metric = gcp.logging.Metric(
        f'{CONFIG_RESOLVER}-unresolved-metric',
        name=f'{CONFIG_RESOLVER}-unresolved',
        project=PROJECT,
        description='Analysis Runner Seqera submissions whose config could not be resolved',
        filter=job_logs_filter.apply(
            lambda f: f'{f} AND textPayload:"{UNRESOLVED_LOG_TEXT}"'
        ),
        metric_descriptor=gcp.logging.MetricMetricDescriptorArgs(
            metric_kind='DELTA',
            value_type='INT64',
        ),
    )

    unresolved_alert = gcp.monitoring.AlertPolicy(
        f'{CONFIG_RESOLVER}-unresolved-alert',
        project=PROJECT,
        display_name='Analysis Runner Config Resolver: unresolved configs',
        combiner='OR',
        notification_channels=[NOTIFICATION_CHANNEL],
        conditions=[
            gcp.monitoring.AlertPolicyConditionArgs(
                display_name='Submission config not resolved',
                condition_threshold=gcp.monitoring.AlertPolicyConditionConditionThresholdArgs(
                    filter=unresolved_metric.name.apply(
                        lambda name: (
                            f'metric.type="logging.googleapis.com/user/{name}" '
                            'AND resource.type="cloud_run_job"'
                        )
                    ),
                    comparison='COMPARISON_GT',
                    threshold_value=0,
                    duration='60s',
                    aggregations=[
                        # An unresolved alert stays open until no submission fails for this long
                        gcp.monitoring.AlertPolicyConditionConditionThresholdAggregationArgs(
                            alignment_period='7200s',
                            per_series_aligner='ALIGN_SUM',
                            cross_series_reducer='REDUCE_SUM',
                        ),
                    ],
                    # No failures in the window means no data: close the incident
                    evaluation_missing_data='EVALUATION_MISSING_DATA_INACTIVE',
                ),
            ),
        ],
        alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
            notification_prompts=['OPENED', 'CLOSED'],
        ),
        documentation=gcp.monitoring.AlertPolicyDocumentationArgs(
            subject=(
                'Analysis Runner Config Resolver (${resource.label.project_id}): '
                'unresolved Seqera submissions'
            ),
            mime_type='text/markdown',
            content=job_logs_filter.apply(
                lambda f: (
                    'The Analysis Runner config resolver could not resolve one or more Seqera submissions for 2h.\n\n'
                    'Check for matching log entries starting incident start time in Logs Explorer:\n\n'
                    f'```\n{f}\ntextPayload:"{UNRESOLVED_LOG_TEXT}"\n```'
                )
            ),
        ),
    )

    # Triggers an alert if the Config resolver job keeps on failing
    # Alert will trigger if failed > 5 times in 30 mins
    failed_runs_alert = gcp.monitoring.AlertPolicy(
        f'{CONFIG_RESOLVER}-failed-runs-alert',
        project=PROJECT,
        display_name='Analysis Runner Config Resolver: job keeps failing',
        combiner='OR',
        notification_channels=[NOTIFICATION_CHANNEL],
        conditions=[
            gcp.monitoring.AlertPolicyConditionArgs(
                display_name='All runs failed for 30 min',
                condition_threshold=gcp.monitoring.AlertPolicyConditionConditionThresholdArgs(
                    filter=job.name.apply(
                        lambda name: (
                            'metric.type="run.googleapis.com/job/completed_execution_count" '
                            'AND resource.type="cloud_run_job" '
                            f'AND resource.labels.job_name="{name}" '
                            'AND metric.labels.result="failed"'
                        )
                    ),
                    comparison='COMPARISON_GT',
                    threshold_value=5,
                    duration='60s',
                    aggregations=[
                        gcp.monitoring.AlertPolicyConditionConditionThresholdAggregationArgs(
                            alignment_period='1800s',
                            per_series_aligner='ALIGN_SUM',
                            cross_series_reducer='REDUCE_SUM',
                        ),
                    ],
                    evaluation_missing_data='EVALUATION_MISSING_DATA_INACTIVE',
                ),
            ),
        ],
        alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
            notification_prompts=['OPENED', 'CLOSED'],
        ),
        documentation=gcp.monitoring.AlertPolicyDocumentationArgs(
            subject=('Analysis Runner Config Resolver: job keeps failing'),
            mime_type='text/markdown',
            content='Every config resolver run in the last 30 min failed.',
        ),
    )

    return unresolved_alert, failed_runs_alert
