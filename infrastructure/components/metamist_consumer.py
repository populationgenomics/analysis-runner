"""
Metamist consumer
"""
# TODO, do be removed later

import pulumi
import pulumi_gcp as gcp

gcp_config = pulumi.Config('gcp')
REGION = gcp_config.require('region')
PROJECT = gcp_config.require('project')


def create_metamist_consumer_resources(
    submissions_topic: gcp.pubsub.Topic,
) -> dict[str, pulumi.Resource]:
    consumer_sa = gcp.serviceaccount.Account(
        'sample-metadata-sa',
        account_id='sample-metadata',
        project=PROJECT,
        display_name='sample-metadata',
        description='Submits analysis to sample-metadata log',
        opts=pulumi.ResourceOptions(protect=True),
    )

    function = gcp.cloudfunctions.Function(
        'sample-metadata-function',
        name='sample_metadata',
        region=REGION,
        runtime='python311',
        entry_point='main',
        max_instances=3000,
        docker_registry='ARTIFACT_REGISTRY',
        service_account_email=consumer_sa.email,
        event_trigger=gcp.cloudfunctions.FunctionEventTriggerArgs(
            event_type='google.pubsub.topic.publish',
            resource=submissions_topic.id,
        ),
        # TODO check here
        opts=pulumi.ResourceOptions(
            protect=True,
            ignore_changes=[
                # Source is uploaded by `gcloud functions deploy`
                'sourceArchiveBucket',
                'sourceArchiveObject',
                'sourceRepository',
                'automaticUpdatePolicy',
                'labels',
            ],
        ),
    )

    return {'consumer_sa': consumer_sa, 'function': function}
