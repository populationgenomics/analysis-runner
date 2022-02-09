import pulumi
import pulumi_gcp as gcp
import pulumi_azure_native as az
import pulumi_azuread as azuread

GCP_REGION = 'australia-southeast1'
DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
ACCESS_LEVELS = ('test', 'standard', 'full')

# some resources we only want to create once, but we don't want
# the main() function to know it has to create a storage_account
# FIRST before it create
az_storage_account = None


def main():  # pylint: disable=too-many-locals,too-many-branches
    """Main entry point."""

    # Fetch configuration.
    config = pulumi.Config()
    dataset = pulumi.get_stack()

    # create azure resource group
    rg = az.resources.ResourceGroup(dataset)

    gcp_svc_crman, gcp_svc_identity = enable_gcp_services()

    # create groups
    gcp_access_grp, az_acccess_grp = create_group(
        f'{dataset}-access', gcp_svc_cloudidentity=gcp_svc_identity
    )
    # create bucket
    gcp_bucket, az_bucket = create_bucket(rg, f'cpg-{dataset}-main')

    # give permission for groups to access bucket
    add_gcp_group_to_bucket(gcp_access_grp, gcp_bucket)
    add_az_group_to_blob(az_acccess_grp, az_bucket)

    # Now, we are relying on something to bridge the identities
    # of the hail batch service principals to Google Identity.
    # We'll add the members directly to the Google Group, and
    # something later will sync them to the Azure equivalent group.
    members = get_all_hail_service_accounts(config)
    add_members_to_gcp_group(gcp_access_grp, members, gcp_svc_identity=gcp_svc_identity)


def get_all_hail_service_accounts(config):
    """Get all hail service accounts from config"""
    sas = []

    for kind in 'gcp', 'az':
        for access_level in ACCESS_LEVELS:
            service_account = config.get(f'{kind}_service_account_{access_level}')
            if service_account:
                sas.append(service_account)

    return sas


def add_gcp_group_to_bucket(gcp_grp, bucket):
    """Add GCP group to GCP bucket"""
    return gcp.storage.BucketIamMember(
        f'{gcp_grp.name}-access',
        bucket=bucket.name,
        role='roles/storage.legacyBucketWriter',
        member=f'group:{gcp_grp.name}',
    )


def add_az_group_to_blob(az_grp, blob):
    """Add Azure group to Azure blob storage"""
    return az.storage.BlobAcl(
        f'{az_grp.name}-access',
        container_name=blob.container_name,
        blob_name=blob.name,
        access_tier=az.storage.AccessTier.HOT,
        permission=az.storage.Permission.READ,
        start=None,
        expiry=None,
        principal_id=f'{az_grp.name}@{DOMAIN}',
    )


def enable_gcp_services():
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

    gcp.artifactregistry.Repository(
        'gcp-docker-artifact-registry',
        repository_id='images',
        location=GCP_REGION,
        format='DOCKER',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudresourcemanager]),
    )

    return [cloudresourcemanager, cloudidentity]


def create_group(group_name, gcp_svc_cloudidentity):

    # GCP
    gcp_grp_email = f'{group_name}@{DOMAIN}'
    gcp_grp = gcp.cloudidentity.Group(
        group_name,
        display_name=group_name,
        group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=gcp_grp_email),
        labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
        parent=f'customers/{CUSTOMER_ID}',
        opts=pulumi.resource.ResourceOptions(depends_on=[gcp_svc_cloudidentity]),
    )

    az_grp = azuread.Group(
        group_name, display_name=group_name, owners=[], security_enabled=True
    )

    return gcp_grp, az_grp


def az_get_storage_account(rg):
    global az_storage_account
    if az_storage_account is None:
        az_storage_account = az.storage.StorageAccount(
            'default',
            resource_group_name=rg.name,
            sku=az.storage.SkuArgs(
                name=az.storage.SkuName.STANDARD_LRS,
            ),
            kind=az.storage.Kind.STORAGE_V2,
        )
    return az_storage_account


def create_bucket(rg, bucket_name):
    """Create bucket in GCP and Azure"""

    # GCP
    gcp_bucket = gcp.storage.Bucket(
        bucket_name,
        name=bucket_name,
        location=GCP_REGION,
        uniform_bucket_level_access=True,
        labels={'bucket': bucket_name},
        # **kwargs,
    )

    # AZURE
    storage_ac = az_get_storage_account(rg)
    az_container = az.storage.BlobContainer(
        'default-blob-container',
        container_name=bucket_name,
        account_name=storage_ac.name,
        resource_group_name=rg.name,
    )

    return gcp_bucket, az_container


def add_members_to_gcp_group(gcp_grp, members, gcp_svc_identity):
    """Add members to GCP group"""
    return [
        gcp.cloudidentity.GroupMembership(
            f'{member.name}-{gcp_grp.name}-membership',
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=member
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
            opts=pulumi.resource.ResourceOptions(depends_on=[gcp_svc_identity]),
        )
        for member in members
    ]


if __name__ == '__main__':
    main()
