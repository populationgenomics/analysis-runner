"""
Resources shared by the server and web components
"""

import pulumi
import pulumi_gcp as gcp
from infra_util import member_resource_name

gcp_config = pulumi.Config('gcp')
PROJECT = gcp_config.require('project')
REGION = gcp_config.require('region')

config = pulumi.Config()


def create_common_resources() -> dict[str, pulumi.Resource]:
    # Server, driver and web images.
    # Development environment image clean up policies applied.
    # Production environment keeps every image.
    images_cleanup = config.get_bool('images_cleanup') or False
    images_repo = gcp.artifactregistry.Repository(
        'images-repository',
        repository_id='images',
        location=REGION,
        project=PROJECT,
        format='DOCKER',
        cleanup_policies=_images_cleanup_policies() if images_cleanup else None,
        cleanup_policy_dry_run=not images_cleanup,
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Specifically for the DEV env
    # Grant public dataset groups test level access
    # Production env is managed in cpg-infra
    for member in config.get_object('images_readers') or []:
        gcp.artifactregistry.RepositoryIamMember(
            f'images-reader-{member_resource_name(member)}',
            project=PROJECT,
            location=REGION,
            repository=images_repo.repository_id,
            role='roles/artifactregistry.reader',
            member=member,
        )

    resources: dict[str, pulumi.Resource] = {'images_repo': images_repo}

    # Dev bucket is created/managed in the dev analysis runner GCP project space
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
        )

    return resources


def _images_cleanup_policies() -> list[
    gcp.artifactregistry.RepositoryCleanupPolicyArgs
]:

    return [
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id='delete-older-than-14-days',
            action='DELETE',
            condition=gcp.artifactregistry.RepositoryCleanupPolicyConditionArgs(
                tag_state='ANY',
                older_than='1209600s',  # 14 days
            ),
        ),
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id='keep-10-most-recent',
            action='KEEP',
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=10,
            ),
        ),
    ]
