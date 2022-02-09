# Setting up Azure for GCP

> EVERYTHING IN THIS FILE / DIR IS UNTESTED! JUST A COLLECTION OF NOTES SO FAR!

This directory is a PoC, to see how possible it is to set-up a tiny
infrastructure on both GCP and Azure, as a model for how we intend to
set it up.

## Setting up an Azure Tenant

For this process, I've set up a `populationgenomics` tenant in Azure,
mainly keep testing fairly contained, and to avoid polluting our parent AD.
I've created the subscription `na1278-testing`, whose home tenant is the
`populationgenomics.onmicrosoft.com`.

> You'll probably need to be guested 

Note, to run this pulumi script, you should be in the NA12878-subscription which 
exists within this new tenant. You may need to log in to this tenant in the CLI using:

```shell
# login to new tenant
az login --tenant populationgenomics.onmicrosoft.com

# check this subscription exists in the list
az account list

# Find the ID, and set the current subscription
az account set --subscription=<id>
```

## Cloud run service / `simplestackserver`

One complication, is that we intend to run some server on GCP behind CloudRun.
We want a Hail Batch job on Azure to authenticate with this CloudRun service,
which requires:

- The authenticating principal to be present in the Google Identity svc
- This is so we can add it to a Google Group (which the Google Group will have
   `cloudrun.invoker` permissions).

To deploy this cloud service:

```shell
GCP_PROJECT="cpg-na12878"
GCP_REGION="australia-southeast-1"
SERVICE_NAME="simplestackserver"
GROUP_NAME="na12878-access"

gcloud config set project $GCP_PROJECT

# build image
IMAGE="GCP_REGION-docker.pkg.dev/$GCP_PROJECT/images/$SERVICE_NAME:latest"
exit 1; # implement the building and pushing, then we can talk about deploying
gcloud run deploy $SERVICE_NAME \
  --image=$IMAGE \
  --region=GCP_REGION \
  --update-env-vars GCP_PATH="gs://cpg-na12878/file.txt",AZ_PATH="az://<container>/cpg-na12878/file.txt"

gcloud run services add-iam-policy-binding $SERVICE_NAME \
   --member="$GROUP_NAME@populationgenomics.org.au" \
   --role="roles/run.invoker"
   
URL=$(gcloud run services describe $SERVICE_NAME --region $GCP_GCP_REGION --platform managed --format "value(status.url)")
echo "Deployed cloud run service, and available at: $URL"
```
