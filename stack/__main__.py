"""
Pulumi stack to set up buckets and permission groups.

Convenience groups:
    - `access`: For persons, also gives access to analysis-runner
    - `web`: For persons, also gives access to web buckets

Service account levels for Hail, Cromwell, Dataproc:
    - `test`: Full access to test buckets
    - `standard`: Read + append access to main buckets
    - `full`: Full access to test + main buckets

Other service accounts:
    - main-upload:
        Create / Destroy on main-upload buckets

Depends on relationships:
    - Full: groups get equivalent access in sub-datasets
    - Readonly: groups get readonly access in sub-datasets

Sample-metadata:
    - Now use `test-read`, `test-full`, `main-list`, `main-create`

Groups:
    - `test-read`: Read only access to the test bucket
        - self: `test-full` (no self SAs or groups have read-only access to test)
            Adding test-full here makes sample-metadata groups easier)
        - depends-read-only: `test-read-only`, `test-full`
    - `test-full`: Full access to the test bucket:
        - self: `access` group, full SAs, testSAs (not standard)
        - readonly-depends: none
    - `main-list`: List only access to the bucket.
        - self: `access` group
        - readonly-depends: `main-list`
    - `main-read`: List + read access to the main bucket.
        - self: `main-create`, `full`
        - readonly-depends: `main-read`, `main-create`, `full`
    - `main-create`: Append access to the main bucket.:
        - self: standard SAs, `full`
    - `full`: Full access to both test + main buckets.
        - self: full SAs
"""

from collections import defaultdict, namedtuple
import base64
from typing import Optional, List, Dict
import pulumi
import pulumi_gcp as gcp

DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
REGION = 'australia-southeast1'
ANALYSIS_RUNNER_PROJECT = 'analysis-runner'
CPG_COMMON_PROJECT = 'cpg-common'
ANALYSIS_RUNNER_SERVICE_ACCOUNT = (
    'analysis-runner-server@analysis-runner.iam.gserviceaccount.com'
)
ANALYSIS_RUNNER_LOGGER_SERVICE_ACCOUNT = (
    'sample-metadata@analysis-runner.iam.gserviceaccount.com'
)
WEB_SERVER_SERVICE_ACCOUNT = 'web-server@analysis-runner.iam.gserviceaccount.com'
ACCESS_GROUP_CACHE_SERVICE_ACCOUNT = (
    'access-group-cache@analysis-runner.iam.gserviceaccount.com'
)
REFERENCE_BUCKET_NAME = 'cpg-reference'
HAIL_WHEEL_BUCKET_NAME = 'cpg-hail-ci'
NOTEBOOKS_PROJECT = 'notebooks-314505'
# cromwell-submission-access@populationgenomics.org.au
CROMWELL_ACCESS_GROUP_ID = 'groups/03cqmetx2922fyu'
CROMWELL_RUNNER_ACCOUNT = 'cromwell-runner@cromwell-305305.iam.gserviceaccount.com'
SAMPLE_METADATA_PROJECT = 'sample-metadata'
SAMPLE_METADATA_API_SERVICE_ACCOUNT = (
    'sample-metadata-api@sample-metadata.iam.gserviceaccount.com'
)
ACCESS_LEVELS = ('test', 'standard', 'full')
TMP_BUCKET_PERIOD_IN_DAYS = 8  # tmp content gets deleted afterwards.

SampleMetadataAccessorMembership = namedtuple(
    # the member_key for a group might be group.group_key.id
    'SampleMetadataAccessorMembership',
    ['name', 'member_key', 'permissions'],
)

UNDELETE_RULE = gcp.storage.BucketLifecycleRuleArgs(
    action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
    condition=gcp.storage.BucketLifecycleRuleConditionArgs(
        age=30, with_state='ARCHIVED'
    ),
)


def main():  # pylint: disable=too-many-locals,too-many-branches
    """Main entry point."""

    # Fetch configuration.
    config = pulumi.Config()
    enable_release = config.get_bool('enable_release')
    archive_age = config.get_int('archive_age') or 30

    dataset = pulumi.get_stack()

    organization = gcp.organizations.get_organization(domain=DOMAIN)
    project_id = gcp.organizations.get_project().project_id

    dependency_stacks = {}
    for dependency in config.get_object('depends_on') or ():
        dependency_stacks[dependency] = pulumi.StackReference(dependency)

    def org_role_id(id_suffix: str) -> str:
        return f'{organization.id}/roles/{id_suffix}'

    lister_role_id = org_role_id('StorageLister')
    viewer_creator_role_id = org_role_id('StorageViewerAndCreator')
    viewer_role_id = org_role_id('StorageObjectAndBucketViewer')

    # The Cloud Resource Manager API is required for the Cloud Identity API.
    cloudresourcemanager = gcp.projects.Service(
        'cloudresourcemanager-service',
        service='cloudresourcemanager.googleapis.com',
        disable_on_destroy=False,
    )

    # The Cloud Identity API is required for creating access groups and service accounts.
    cloudidentity = gcp.projects.Service(
        'cloudidentity-service',
        service='cloudidentity.googleapis.com',
        disable_on_destroy=False,
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudresourcemanager]),
    )

    # Enable Dataproc until the Hail Query Service is ready.
    _ = gcp.projects.Service(
        'dataproc-service',
        service='dataproc.googleapis.com',
        disable_on_destroy=False,
    )

    service_accounts = defaultdict(list)
    for kind in 'hail', 'deployment':
        for access_level in ACCESS_LEVELS:
            service_account = config.get(f'{kind}_service_account_{access_level}')
            if service_account:
                service_accounts[kind].append((access_level, service_account))

    # Create Dataproc and Cromwell service accounts.
    for kind in 'dataproc', 'cromwell':
        service_accounts[kind] = []
        for access_level in ACCESS_LEVELS:
            account = gcp.serviceaccount.Account(
                f'{kind}-service-account-{access_level}',
                account_id=f'{kind}-{access_level}',
                opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
            )
            service_accounts[kind].append((access_level, account.email))

    def service_accounts_gen():
        for kind, values in service_accounts.items():
            for access_level, service_account in values:
                yield kind, access_level, service_account

    def bucket_name(kind: str) -> str:
        """Returns the bucket name for the given dataset."""
        return f'cpg-{dataset}-{kind}'

    def create_bucket(
        name: str, enable_versioning=True, **kwargs
    ) -> gcp.storage.Bucket:
        """Returns a new GCS bucket."""
        return gcp.storage.Bucket(
            name,
            name=name,
            location=REGION,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=enable_versioning),
            labels={'bucket': name},
            **kwargs,
        )

    def get_name_from_bucket(bucket) -> str:
        if isinstance(bucket, str):
            return bucket
        bname = bucket.name
        if bname.startswith(f'cpg-{dataset}'):
            return bname[len(f'cpg-{dataset}')+1:]
        return bname

    def bucket_member(*args, **kwargs):
        """Wraps gcp.storage.BucketIAMMember.

        When resources are renamed, it can be useful to explicitly apply changes in two
        phases: delete followed by create; that's opposite of the default create followed by
        delete, which can end up with missing permissions. To implement the first phase
        (delete), simply change this implementation to a no-op temporarily.
        """
        gcp.storage.BucketIAMMember(*args, **kwargs)

    def group_mail(kind: str) -> str:
        """Returns the email address of a permissions group."""
        return f'{dataset}-{kind}@{DOMAIN}'

    def create_group(mail: str) -> gcp.cloudidentity.Group:
        """Returns a new Cloud Identity group for the given email address."""
        name = mail.split('@')[0]
        grp = gcp.cloudidentity.Group(
            name,
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{CUSTOMER_ID}',
            opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
        )

        dn = grp.display_name.apply(lambda a: a)
        raise Exception(dn)


    def create_secret(resource_name: str, secret_id: str, **kwargs):
        return gcp.secretmanager.Secret(
            resource_name,
            secret_id=secret_id,
            replication=gcp.secretmanager.SecretReplicationArgs(
                user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                    replicas=[
                        gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                            location='australia-southeast1',
                        ),
                    ],
                ),
            ),
            opts=pulumi.resource.ResourceOptions(depends_on=[secretmanager]),
            **kwargs,
        )

    grp_test_read = create_group(group_mail('test_read'))
    grp_test_full = create_group(group_mail('test_full'))
    grp_main_list = create_group(group_mail('main_list'))
    grp_main_read = create_group(group_mail('main_read'))
    grp_main_create = create_group(group_mail('main_create'))
    grp_full = create_group(group_mail('full'))

    grp_access = create_group(group_mail('access'))
    grp_web_access = create_group(group_mail('web-access'))

    groups_to_export = [
        grp_test_read,
        grp_test_full,
        grp_main_list,
        grp_main_read,
        grp_main_create,
        grp_full,
        grp_access,
        grp_web_access,
    ]

    for group in groups_to_export:
        group_name = group.display_name
        # other stacks require all groups to exist
        pulumi.export(group_name + '-group-id', group.id)

        # add access-group-cache so it can lookup members of group
        gcp.cloudidentity.GroupMembership(
            f'access-group-cache-member-{group_name}',
            group=group,
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=ACCESS_GROUP_CACHE_SERVICE_ACCOUNT
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
            opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
        )

    # UPLOAD BUCKETS + ACCOUNTS
    main_upload_account = gcp.serviceaccount.Account(
        'main-upload-service-account',
        account_id='main-upload',
        display_name='main-upload',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    main_upload_buckets = {
        'main-upload': create_bucket(
            bucket_name('main-upload'), lifecycle_rules=[UNDELETE_RULE]
        )
    }

    for additional_upload_bucket in (
        config.get_object('additional_upload_buckets') or ()
    ):
        main_upload_buckets[additional_upload_bucket] = create_bucket(
            additional_upload_bucket, lifecycle_rules=[UNDELETE_RULE]
        )

    test_upload_bucket = create_bucket(
        bucket_name('test-upload'), lifecycle_rules=[UNDELETE_RULE]
    )

    # Grant admin permissions as composite uploads need to delete temporary files.
    for bname, upload_bucket in main_upload_buckets.items():
        bucket_member(
            f'main-upload-service-account-{bname}-bucket-creator',
            bucket=upload_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', main_upload_account.email),
        )

    archive_bucket = create_bucket(
        bucket_name('archive'),
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(
                    type='SetStorageClass', storage_class='ARCHIVE'
                ),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=archive_age),
            ),
            UNDELETE_RULE,
        ],
    )

    test_bucket = create_bucket(bucket_name('test'), lifecycle_rules=[UNDELETE_RULE])

    # tmp buckets don't have an undelete lifecycle rule, to avoid paying for
    # intermediate results that get cleaned up immediately after workflow runs.
    test_tmp_bucket = create_bucket(
        bucket_name('test-tmp'),
        enable_versioning=False,
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                    age=TMP_BUCKET_PERIOD_IN_DAYS
                ),
            )
        ],
    )

    test_analysis_bucket = create_bucket(
        bucket_name('test-analysis'), lifecycle_rules=[UNDELETE_RULE]
    )

    test_web_bucket = create_bucket(
        bucket_name('test-web'), lifecycle_rules=[UNDELETE_RULE]
    )

    main_bucket = create_bucket(bucket_name('main'), lifecycle_rules=[UNDELETE_RULE])

    # tmp buckets don't have an undelete lifecycle rule, to avoid paying for
    # intermediate results that get cleaned up immediately after workflow runs.
    main_tmp_bucket = create_bucket(
        bucket_name('main-tmp'),
        enable_versioning=False,
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                    age=TMP_BUCKET_PERIOD_IN_DAYS
                ),
            )
        ],
    )

    main_analysis_bucket = create_bucket(
        bucket_name('main-analysis'), lifecycle_rules=[UNDELETE_RULE]
    )

    main_web_bucket = create_bucket(
        bucket_name('main-web'), lifecycle_rules=[UNDELETE_RULE]
    )

    # # Create groups for each access level.
    # access_level_groups = {}
    # for access_level in ACCESS_LEVELS:
    #     group = create_group(group_mail(dataset, access_level))
    #     access_level_groups[access_level] = group
    #
    #     # The group provider ID is used by other stacks that depend on this one.
    #     group_provider_id_name = f'{access_level}-access-group-id'
    #     pulumi.export(group_provider_id_name, group.id)
    #
    #     # Allow the access group cache to list memberships.
    #     gcp.cloudidentity.GroupMembership(
    #         f'access-group-cache-{access_level}-access-level-group-membership',
    #         group=group.id,
    #         preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
    #             id=ACCESS_GROUP_CACHE_SERVICE_ACCOUNT
    #         ),
    #         roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #         opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #     )
    #
    #     # Provide access transitively to datasets we depend on
    #     for dependency in config.get_object('depends_on') or ():
    #         dependency_group_id = dependency_stacks[dependency].get_output(
    #             group_provider_id_name,
    #         )
    #
    #         dependency_group = gcp.cloudidentity.Group.get(
    #             f'{dependency}-{access_level}-access-level-group',
    #             dependency_group_id,
    #         )
    #
    #         gcp.cloudidentity.GroupMembership(
    #             f'{dependency}-{access_level}-access-level-group-membership',
    #             group=dependency_group.id,
    #             preferred_member_key=group.group_key,
    #             roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #             opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #         )

    # for dependency, dstack in dependency_stacks.items():
    #     # add the {dataset}-access group to the dependency
    #     depends_on_access_group_name = (
    #         group_mail(dependency, 'access').split('@')[0] + '-group-id'
    #     )
    #     depends_on_access_group_id = dstack.get_output(
    #         depends_on_access_group_name,
    #     )
    #     depends_on_access_group = gcp.cloudidentity.Group.get(
    #         depends_on_access_group_name, depends_on_access_group_id
    #     )
    #     gcp.cloudidentity.GroupMembership(
    #         f'{dataset}-{dependency}-access',
    #         group=depends_on_access_group,
    #         preferred_member_key=access_group.group_key,
    #         roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #         opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #     )

    # for kind, access_level, service_account in service_accounts_gen():
    #     gcp.cloudidentity.GroupMembership(
    #         f'{kind}-{access_level}-access-level-group-membership',
    #         group=access_level_groups[access_level],
    #         preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
    #             id=service_account
    #         ),
    #         roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #         opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #     )

    secretmanager = gcp.projects.Service(
        'secretmanager-service',
        service='secretmanager.googleapis.com',
        disable_on_destroy=False,
    )

    # Sample metadata access

    # permissions for read / write
    #   - 4 secrets, main-read, main-write, test-read, test-write
    # sm_groups = {}
    # for env in ('main', 'test'):
    #     for rs in ('read', 'write'):
    #         key = f'sample-metadata-{env}-{rs}'
    #
    #         group = create_group(group_mail(dataset, key))
    #         sm_groups[f'{env}-{rs}'] = group
    #
    #         gcp.cloudidentity.GroupMembership(
    #             f'sample-metadata-group-cache-{env}-{rs}-group-membership',
    #             group=group,
    #             preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
    #                 id=ACCESS_GROUP_CACHE_SERVICE_ACCOUNT
    #             ),
    #             roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #             opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #         )
    #
    #         secret = create_secret(
    #             f'{key}-group-cache-secret',
    #             secret_id=f'{dataset}-{key}-members-cache',
    #         )
    #         add_access_group_cache_as_secret_member(secret, resource_prefix=key)
    #
    #         gcp.secretmanager.SecretIamMember(
    #             f'{key}-api-secret-accessor',
    #             secret_id=secret.id,
    #             role='roles/secretmanager.secretAccessor',
    #             member=f'serviceAccount:{SAMPLE_METADATA_API_SERVICE_ACCOUNT}',
    #         )

    # Add cloud run invoker to analysis-runner for the access-group
    gcp.cloudrun.IamMember(
        f'analysis-runner-access-invoker',
        location=REGION,
        project=ANALYSIS_RUNNER_PROJECT,
        service='server',
        role='roles/run.invoker',
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )

    # Declare access to sample-metadata API of format ({env}-{read,write})
    # access_levels: List[SampleMetadataAccessorMembership] = [
    #     SampleMetadataAccessorMembership(
    #         name='human',
    #         member_key=access_group.group_key.id,
    #         permissions=('main-read', 'test-read', 'test-write'),
    #     ),
    #     SampleMetadataAccessorMembership(
    #         name='test',
    #         member_key=access_level_groups['test'].group_key.id,
    #         permissions=('main-read', 'test-read', 'test-write'),
    #     ),
    #     SampleMetadataAccessorMembership(
    #         name='standard',
    #         member_key=access_level_groups['standard'].group_key.id,
    #         permissions=('main-read', 'main-write'),
    #     ),
    #     SampleMetadataAccessorMembership(
    #         name='full',
    #         member_key=access_level_groups['full'].group_key.id,
    #         permissions=sm_groups.keys(),
    #     ),
    #     # allow the analysis-runner logging cloud function to update the sample-metadata project
    #     SampleMetadataAccessorMembership(
    #         name='analysis-runner-logger',
    #         member_key=ANALYSIS_RUNNER_LOGGER_SERVICE_ACCOUNT,
    #         permissions=sm_groups.keys(),
    #     ),
    # ]
    #
    # # give access to sample_metadata groups (and hence sample-metadata API through secrets)
    # for name, service_account, permission in sm_access_levels:
    #     for kind in permission:
    #         gcp.cloudidentity.GroupMembership(
    #             f'sample-metadata-{kind}-{name}-access-level-group-membership',
    #             group=sm_groups[kind],
    #             preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
    #                 id=service_account
    #             ),
    #             roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #             opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    #         )

    gcp.projects.IAMMember(
        'project-buckets-lister',
        role=lister_role_id,
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )

    # Grant visibility to Dataproc utilization metrics etc.
    gcp.projects.IAMMember(
        'project-monitoring-viewer',
        role='roles/monitoring.viewer',
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )


    # NEW PERMISSIONS

    test_read_members = {'test_full': grp_test_full}

    sas_by_level_then_kind_unique_key: Dict[str, Dict[str, str]] = defaultdict(dict)
    for kind, access_level, sa in service_accounts_gen():
        sas_by_level_then_kind_unique_key[access_level][access_level + '_' + kind] = sa

    test_full_members = {
        'access': grp_access.group_key.id,
        **sas_by_level_then_kind_unique_key['full'],
        **sas_by_level_then_kind_unique_key['standard'],
        **sas_by_level_then_kind_unique_key['test'],
    }
    main_list_members = {'access': grp_access.group_key.id}
    main_read_members = {
        'main_create': grp_main_create.group_key.id,
        'full': grp_full.group_key.id,
    }
    main_create_members = {
        **sas_by_level_then_kind_unique_key['standard'],
        **sas_by_level_then_kind_unique_key['test'],
    }

    full_members = {**sas_by_level_then_kind_unique_key['full']}

    members_map = [
        (grp_test_read, test_read_members),
        (grp_test_full, test_full_members),
        (grp_main_list, main_list_members),
        (grp_main_read, main_read_members),
        (grp_main_create, main_create_members),
        (grp_full, full_members),
    ]

    for grp, members in members_map:
        grp_name = grp.display_name.apply()
        for key, member in members.items():
            gcp.cloudidentity.GroupMembership(
                f'{grp_name}-{key}-membership',
                group=grp.id,
                preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                    id=ACCESS_GROUP_CACHE_SERVICE_ACCOUNT
                ),
                roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
                opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
            )

    # NEW BUCKET PERMISSIONS

    test_buckets = [
        test_bucket,
        test_web_bucket,
        test_tmp_bucket,
        test_analysis_bucket,
        test_upload_bucket,
    ]

    for bucket in test_buckets:
        # test FULL
        bname = get_name_from_bucket(bucket)
        bucket_member(
            f'{bname}-bucket-admin',
            bucket=bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('group:', grp_test_full.group_key.id),
        )

        # test READ
        bucket_member(
            f'{bname}-group-{bname}-bucket-viewer',
            bucket=bucket.name,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', grp_test_read.group_key.id),
        )

    main_buckets = [
        main_bucket,
        main_analysis_bucket,
        main_tmp_bucket,
        main_web_bucket,
        *main_upload_buckets.values()
    ]
    for bucket in main_buckets:
        bname = bucket.name
        if bname.startswith(f'cpg-{dataset}'):
            bname = bname[len(f'cpg-{dataset}')+1:]

        # main FULL
        bucket_member(
            f'{bname}-bucket-admin',
            bucket=bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('group:', grp_full.group_key.id),
        )

        # main CREATE
        bucket_member(
            f'{bname}-group-main-create-bucket-viewer',
            bucket=bucket.name,
            role=viewer_creator_role_id,
            member=pulumi.Output.concat('group:', grp_main_create.group_key.id),
        )

        # main READ
        bucket_member(
            f'{bname}-group-main-read-bucket-viewer',
            bucket=bucket.name,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', grp_test_read.group_key.id),
        )

    bucket_member(
        f'main-archive-group-full-bucket-viewer',
        bucket=archive_bucket.name,
        role=viewer_role_id,
        member=pulumi.Output.concat('group:', grp_full.group_key.id),
    )

    # OLD PERMISSIONS

    # bucket_member(
    #     'access-group-test-bucket-admin',
    #     bucket=test_bucket.name,
    #     role='roles/storage.admin',
    #     member=pulumi.Output.concat('group:', grp_access.group_key.id),
    # )

    # bucket_member(
    #     'access-group-test-upload-bucket-admin',
    #     bucket=test_upload_bucket.name,
    #     role='roles/storage.admin',
    #     member=pulumi.Output.concat('group:', grp_access.group_key.id),
    # )

    # bucket_member(
    #     'access-group-test-tmp-bucket-admin',
    #     bucket=test_tmp_bucket.name,
    #     role='roles/storage.admin',
    #     member=pulumi.Output.concat('group:', grp_access.group_key.id),
    # )

    # bucket_member(
    #     'access-group-test-analysis-bucket-admin',
    #     bucket=test_analysis_bucket.name,
    #     role='roles/storage.admin',
    #     member=pulumi.Output.concat('group:', grp_access.group_key.id),
    # )

    # bucket_member(
    #     'access-group-test-web-bucket-admin',
    #     bucket=test_web_bucket.name,
    #     role='roles/storage.admin',
    #     member=pulumi.Output.concat('group:', grp_access.group_key.id),
    # )
    # for bname, upload_bucket in main_upload_buckets.items():
    #     bucket_member(
    #         f'access-group-{bname}-bucket-viewer',
    #         bucket=upload_bucket.name,
    #         role=viewer_role_id,
    #         member=pulumi.Output.concat('group:', grp_access.group_key.id),
    #     )

    # access group can view main-analysis
    bucket_member(
        'access-group-main-analysis-bucket-viewer',
        bucket=main_analysis_bucket.name,
        role=viewer_role_id,
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )
    # access group can view main-web
    bucket_member(
        'access-group-main-web-bucket-viewer',
        bucket=main_web_bucket.name,
        role=viewer_role_id,
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )
    # give WEB_SERVER_SERVICE_ACCOUNT access to web buckes
    for bucket in test_web_bucket, main_web_bucket:
        bname = get_name_from_bucket(bucket)
        bucket_member(
            f'web-server-{bname}-bucket-viewer',
            bucket=bucket.name,
            role=viewer_role_id,
            member=pulumi.Output.concat('serviceAccount:', WEB_SERVER_SERVICE_ACCOUNT),
        )

    # give service-accounts access to various GCP services
    for kind, access_level, service_account in service_accounts_gen():
        assert isinstance(service_account, str)
        # Allow the service accounts to pull images. Note that the global project will
        # refer to the dataset, but the Docker images are stored in the "analysis-runner"
        # and "cpg-common" projects' Artifact Registry repositories.
        for project in [ANALYSIS_RUNNER_PROJECT, CPG_COMMON_PROJECT]:
            gcp.artifactregistry.RepositoryIamMember(
                f'{access_level}-images-reader-in-{project}',
                project=project,
                location=REGION,
                repository='images',
                role='roles/artifactregistry.reader',
                member=pulumi.Output.concat('group:', service_account),
            )

        # Allow non-test service accounts to write images to the "cpg-common" Artifact
        # TODO: should this practically only be hail accounts too?
        if access_level != 'test':
            gcp.artifactregistry.RepositoryIamMember(
                f'{access_level}-images-writer-in-cpg-common',
                project=CPG_COMMON_PROJECT,
                location=REGION,
                repository='images',
                role='roles/artifactregistry.writer',
                member=pulumi.Output.concat('group:', service_account),
            )

        # Read access to reference data.
        bucket_member(
            f'{access_level}-reference-bucket-viewer',
            bucket=REFERENCE_BUCKET_NAME,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', service_account),
        )

        # Read access to Hail wheels.
        bucket_member(
            f'{access_level}-hail-wheels-viewer',
            bucket=HAIL_WHEEL_BUCKET_NAME,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', service_account),
        )

        # Allow the usage of requester-pays buckets.
        gcp.projects.IAMMember(
            f'{access_level}-serviceusage-consumer',
            role='roles/serviceusage.serviceUsageConsumer',
            member=pulumi.Output.concat('group:', service_account),
        )

        # Give hail / dataproc / cromwell access to sample-metadata cloud run service
        gcp.cloudrun.IamMember(
            f'sample-metadata-service-account-{access_level}-invoker',
            location=REGION,
            project=SAMPLE_METADATA_PROJECT,
            service='sample-metadata-api',
            role='roles/run.invoker',
            member=pulumi.Output.concat('group:', group.group_key.id),
        )




    if enable_release:
        release_bucket = create_bucket(
            bucket_name('release-requester-pays'),
            lifecycle_rules=[UNDELETE_RULE],
            requester_pays=True,
        )

        bucket_member(
            'access-group-release-bucket-viewer',
            bucket=release_bucket.name,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', grp_access.group_key.id),
        )

        release_access_group = create_group(group_mail('release-access'))

        bucket_member(
            'release-access-group-release-bucket-viewer',
            bucket=release_bucket.name,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', release_access_group.group_key.id),
        )



    # TODO: if only PEOPLE are in ACCESS, why not give everyone access to resources?
    #       feels like a hack to give users access in each dataset
    if True:
        # Allow access_group to READ from Artifact registry - why?
        gcp.artifactregistry.RepositoryIamMember(
            f'access-group-images-reader-in-cpg-common',
            project=CPG_COMMON_PROJECT,
            location=REGION,
            repository='images',
            role='roles/artifactregistry.reader',
            member=pulumi.Output.concat('group:', grp_access.group_key.id),
        )

        # Read access to reference data.
        bucket_member(
            'access-group-reference-bucket-viewer',
            bucket=REFERENCE_BUCKET_NAME,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', grp_access.group_key.id),
        )

        # Read access to Hail wheels.
        bucket_member(
            'access-group-hail-wheels-viewer',
            bucket=HAIL_WHEEL_BUCKET_NAME,
            role=viewer_role_id,
            member=pulumi.Output.concat('group:', grp_access.group_key.id),
        )

        # Allow the usage of requester-pays buckets.
        gcp.projects.IAMMember(
            f'access-group-serviceusage-consumer',
            role='roles/serviceusage.serviceUsageConsumer',
            member=pulumi.Output.concat('group:', grp_access.group_key.id),
        )


    # All members of the access group have web access automatically.
    # gcp.cloudidentity.GroupMembership(
    #     'web-access-group-access-group-membership',
    #     group=grp_web_access.id,
    #     preferred_member_key=grp_access.group_key,
    #     roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    #     opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    # )

    # for access_level, group in access_level_groups.items():
    #     # Allow the service accounts to pull images. Note that the global project will
    #     # refer to the dataset, but the Docker images are stored in the "analysis-runner"
    #     # and "cpg-common" projects' Artifact Registry repositories.
    #     for project in [ANALYSIS_RUNNER_PROJECT, CPG_COMMON_PROJECT]:
    #         gcp.artifactregistry.RepositoryIamMember(
    #             f'{access_level}-images-reader-in-{project}',
    #             project=project,
    #             location=REGION,
    #             repository='images',
    #             role='roles/artifactregistry.reader',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #     # Allow non-test service accounts to write images to the "cpg-common" Artifact
    #     # Registry repository.
    #     if access_level != 'test':
    #         gcp.artifactregistry.RepositoryIamMember(
    #             f'{access_level}-images-writer-in-cpg-common',
    #             project=CPG_COMMON_PROJECT,
    #             location=REGION,
    #             repository='images',
    #             role='roles/artifactregistry.writer',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #     # Read access to reference data.
    #     bucket_member(
    #         f'{access_level}-reference-bucket-viewer',
    #         bucket=REFERENCE_BUCKET_NAME,
    #         role=viewer_role_id,
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # Read access to Hail wheels.
    #     bucket_member(
    #         f'{access_level}-hail-wheels-viewer',
    #         bucket=HAIL_WHEEL_BUCKET_NAME,
    #         role=viewer_role_id,
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # Allow the usage of requester-pays buckets.
    #     gcp.projects.IAMMember(
    #         f'{access_level}-serviceusage-consumer',
    #         role='roles/serviceusage.serviceUsageConsumer',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )

    # The bucket used for Hail Batch pipelines.
    hail_bucket = create_bucket(bucket_name('hail'), lifecycle_rules=[UNDELETE_RULE])

    for access_level, service_account in service_accounts['hail']:
        # Full access to the Hail Batch bucket.
        bucket_member(
            f'hail-service-account-{access_level}-hail-bucket-admin',
            bucket=hail_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    # The analysis-runner also needs Hail bucket access for compiled code.
    bucket_member(
        f'analysis-runner-hail-bucket-admin',
        bucket=hail_bucket.name,
        role='roles/storage.admin',
        member=pulumi.Output.concat('serviceAccount:', ANALYSIS_RUNNER_SERVICE_ACCOUNT),
    )

    # Permissions increase by access level:
    # - test: view / create on any "test" bucket
    # - standard: view / create on any "test" or "main" bucket
    # - full: view / create / delete anywhere
    # for access_level, group in access_level_groups.items():
    #     # test bucket
    #     bucket_member(
    #         f'{access_level}-test-bucket-admin',
    #         bucket=test_bucket.name,
    #         role='roles/storage.admin',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # test-upload bucket
    #     bucket_member(
    #         f'{access_level}-test-upload-bucket-admin',
    #         bucket=test_upload_bucket.name,
    #         role='roles/storage.admin',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # test-tmp bucket
    #     bucket_member(
    #         f'{access_level}-test-tmp-bucket-admin',
    #         bucket=test_tmp_bucket.name,
    #         role='roles/storage.admin',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # test-analysis bucket
    #     bucket_member(
    #         f'{access_level}-test-analysis-bucket-admin',
    #         bucket=test_analysis_bucket.name,
    #         role='roles/storage.admin',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     # test-web bucket
    #     bucket_member(
    #         f'{access_level}-test-web-bucket-admin',
    #         bucket=test_web_bucket.name,
    #         role='roles/storage.admin',
    #         member=pulumi.Output.concat('group:', group.group_key.id),
    #     )
    #
    #     if access_level == 'standard':
    #         # main bucket
    #         bucket_member(
    #             f'standard-main-bucket-view-create',
    #             bucket=main_bucket.name,
    #             role=viewer_creator_role_id,
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-upload bucket
    #         for bname, upload_bucket in main_upload_buckets.items():
    #             bucket_member(
    #                 f'standard-{bname}-bucket-viewer',
    #                 bucket=upload_bucket.name,
    #                 role=viewer_role_id,
    #                 member=pulumi.Output.concat('group:', group.group_key.id),
    #             )
    #
    #         # main-tmp bucket
    #         bucket_member(
    #             f'standard-main-tmp-bucket-view-create',
    #             bucket=main_tmp_bucket.name,
    #             role=viewer_creator_role_id,
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-analysis bucket
    #         bucket_member(
    #             f'standard-main-analysis-bucket-view-create',
    #             bucket=main_analysis_bucket.name,
    #             role=viewer_creator_role_id,
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-web bucket
    #         bucket_member(
    #             f'standard-main-web-bucket-view-create',
    #             bucket=main_web_bucket.name,
    #             role=viewer_creator_role_id,
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #     if access_level == 'full':
    #         # main bucket
    #         bucket_member(
    #             f'full-main-bucket-admin',
    #             bucket=main_bucket.name,
    #             role='roles/storage.admin',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-upload bucket
    #         for bname, upload_bucket in main_upload_buckets.items():
    #             bucket_member(
    #                 f'full-{bname}-bucket-admin',
    #                 bucket=upload_bucket.name,
    #                 role='roles/storage.admin',
    #                 member=pulumi.Output.concat('group:', group.group_key.id),
    #             )
    #
    #         # main-tmp bucket
    #         bucket_member(
    #             f'full-main-tmp-bucket-admin',
    #             bucket=main_tmp_bucket.name,
    #             role='roles/storage.admin',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-analysis bucket
    #         bucket_member(
    #             f'full-main-analysis-bucket-admin',
    #             bucket=main_analysis_bucket.name,
    #             role='roles/storage.admin',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # main-web bucket
    #         bucket_member(
    #             f'full-main-web-bucket-admin',
    #             bucket=main_web_bucket.name,
    #             role='roles/storage.admin',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # archive bucket
    #         bucket_member(
    #             f'full-archive-bucket-admin',
    #             bucket=archive_bucket.name,
    #             role='roles/storage.admin',
    #             member=pulumi.Output.concat('group:', group.group_key.id),
    #         )
    #
    #         # release bucket
    #         if enable_release:
    #             bucket_member(
    #                 f'full-release-bucket-admin',
    #                 bucket=release_bucket.name,
    #                 role='roles/storage.admin',
    #                 member=pulumi.Output.concat('group:', group.group_key.id),
    #             )

    # Notebook permissions
    notebook_account = gcp.serviceaccount.Account(
        'notebook-account',
        project=NOTEBOOKS_PROJECT,
        account_id=f'notebook-{dataset}',
        display_name=f'Notebook service account for dataset {dataset}',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    gcp.projects.IAMMember(
        'notebook-account-compute-admin',
        project=NOTEBOOKS_PROJECT,
        role='roles/compute.admin',
        member=pulumi.Output.concat('serviceAccount:', notebook_account.email),
    )

    gcp.serviceaccount.IAMMember(
        'notebook-account-users',
        service_account_id=notebook_account,
        role='roles/iam.serviceAccountUser',
        member=pulumi.Output.concat('group:', grp_access.group_key.id),
    )

    # Grant the notebook account the same permissions as the access group members.
    gcp.cloudidentity.GroupMembership(
        'notebook-service-account-access-group-member',
        group=grp_access.id,
        preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
            id=notebook_account.email
        ),
        roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    def find_service_account(kind: str, access_level: str) -> Optional[str]:
        for local_access_level, service_account in service_accounts[kind]:
            if access_level == local_access_level:
                return service_account
        return None

    for access_level, service_account in service_accounts['dataproc']:
        # Hail Batch service accounts need to be able to act as Dataproc service
        # accounts to start Dataproc clusters.
        gcp.serviceaccount.IAMMember(
            f'hail-service-account-{access_level}-dataproc-service-account-user',
            service_account_id=pulumi.Output.concat(
                'projects/', project_id, '/serviceAccounts/', service_account
            ),
            role='roles/iam.serviceAccountUser',
            member=pulumi.Output.concat(
                'serviceAccount:', find_service_account('hail', access_level)
            ),
        )

        gcp.projects.IAMMember(
            f'dataproc-service-account-{access_level}-dataproc-worker',
            role='roles/dataproc.worker',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    for access_level, service_account in service_accounts['hail']:
        # The Hail service account creates the cluster, specifying the Dataproc service
        # account as the worker.
        gcp.projects.IAMMember(
            f'hail-service-account-{access_level}-dataproc-admin',
            role='roles/dataproc.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Worker permissions are necessary to submit jobs.
        gcp.projects.IAMMember(
            f'hail-service-account-{access_level}-dataproc-worker',
            role='roles/dataproc.worker',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Add Hail service accounts to Cromwell access group.
        gcp.cloudidentity.GroupMembership(
            f'hail-service-account-{access_level}-cromwell-access',
            group=CROMWELL_ACCESS_GROUP_ID,
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=service_account,
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
            opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
        )

    for access_level, service_account in service_accounts['cromwell']:
        # Allow the Cromwell server to run worker VMs using the Cromwell service
        # accounts.
        gcp.serviceaccount.IAMMember(
            f'cromwell-runner-{access_level}-service-account-user',
            service_account_id=pulumi.Output.concat(
                'projects/', project_id, '/serviceAccounts/', service_account
            ),
            role='roles/iam.serviceAccountUser',
            member=f'serviceAccount:{CROMWELL_RUNNER_ACCOUNT}',
        )

        # To use a service account for VMs, Cromwell accounts need to be allowed to act
        # on their own behalf ;).
        gcp.serviceaccount.IAMMember(
            f'cromwell-service-account-{access_level}-service-account-user',
            service_account_id=pulumi.Output.concat(
                'projects/', project_id, '/serviceAccounts/', service_account
            ),
            role='roles/iam.serviceAccountUser',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Allow the Cromwell service accounts to run workflows.
        gcp.projects.IAMMember(
            f'cromwell-service-account-{access_level}-workflows-runner',
            role='roles/lifesciences.workflowsRunner',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Store the service account key as a secret that's readable by the
        # analysis-runner.
        key = gcp.serviceaccount.Key(
            f'cromwell-service-account-{access_level}-key',
            service_account_id=service_account,
        )

        secret = create_secret(
            f'cromwell-service-account-{access_level}-secret',
            secret_id=f'{dataset}-cromwell-{access_level}-key',
            project=ANALYSIS_RUNNER_PROJECT,
        )

        gcp.secretmanager.SecretVersion(
            f'cromwell-service-account-{access_level}-secret-version',
            secret=secret.id,
            secret_data=key.private_key.apply(
                lambda s: base64.b64decode(s).decode('utf-8')
            ),
        )

        gcp.secretmanager.SecretIamMember(
            f'cromwell-service-account-{access_level}-secret-accessor',
            project=ANALYSIS_RUNNER_PROJECT,
            secret_id=secret.id,
            role='roles/secretmanager.secretAccessor',
            member=f'serviceAccount:{ANALYSIS_RUNNER_SERVICE_ACCOUNT}',
        )

        # Allow the Hail service account to access its corresponding cromwell key
        hail_service_account = find_service_account('hail', access_level)
        gcp.secretmanager.SecretIamMember(
            f'cromwell-service-account-{access_level}-self-accessor',
            project=ANALYSIS_RUNNER_PROJECT,
            secret_id=secret.id,
            role='roles/secretmanager.secretAccessor',
            member=f'serviceAccount:{hail_service_account}',
        )



if __name__ == '__main__':
    main()
