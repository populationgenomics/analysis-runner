import pulumi
import pulumi_gcp as gcp
import pulumi_azure_native as az
import pulumi_azuread as azuread

GCP_REGION = 'australia-southeast1'
DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'

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

    [gcp_svc_identity] = enable_gcp_services()

    # create AD group, and give permission to the group to read from the bucket
    gcp_access_grp, az_acccess_grp = create_group(f'{dataset}-access', gcp_svc_cloudidentity=gcp_svc_identity)

    create_bucket(rg, f'cpg-{dataset}-main')


def enable_gcp_services():
    return [None]


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

    az_grp = azuread.Group(group_name,
                            display_name=group_name,
                            owners=[],
                            security_enabled=True)


    return gcp_grp, az_grp



def az_get_storage_account(rg):
    global az_storage_account
    if az_storage_account is None:
        az_storage_account = az.storage.StorageAccount('default',
                              resource_group_name=rg.name,
                              sku=az.storage.SkuArgs(
                                  name=az.storage.SkuName.STANDARD_LRS,
                              ),
                              kind=az.storage.Kind.STORAGE_V2)
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


if __name__ == '__main__':
    main()
