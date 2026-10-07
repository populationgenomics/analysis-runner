"""
Resources shared by the server and web components
"""

import pulumi
import pulumi_gcp as gcp

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()


def create_common_resources() -> dict[str, pulumi.Resource]:
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

    resources: dict[str, pulumi.Resource] = {'images_repo': images_repo}

    if config.get_bool('create_members_cache_bucket'):
        resources['members_cache_bucket'] = gcp.storage.Bucket(
            'members-cache-bucket',
            name=config.require('members_cache_bucket'),
            project=PROJECT,
            location=REGION.upper(),
            storage_class='STANDARD',
            uniform_bucket_level_access=True,
            public_access_prevention='enforced',
            versioning=gcp.storage.BucketVersioningArgs(enabled=True),
            opts=pulumi.ResourceOptions(protect=True),
        )

    return resources
