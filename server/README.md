# Server deployment using Cloud Run

This directory contains the server component of the analysis runner, which is
deployed as a Cloud Run container.

It uses `aiohttp` instead of `flask` because Hail Batch's async event queue
doesn't mix well with `flask`'s threads.

Each project / [storage
stack](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
has its own Cloud Run deployment. This way, memberships in the respective
permission groups (`$STACK-restricted-access@populationgenomics.org.au`)
can be checked by assigning the *Cloud Run Invoker* IAM role to the group.
While there's also a Cloud Identity API to check group memberships, this
feature unfortunately is [only available in Google Workspace Enterprise
editions](https://googlecloudproject.com/identity/docs/reference/rest/v1/groups.memberships/checkTransitiveMembership).

To build a new Docker image, run:

```bash
gcloud config set project analysis-runner

IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/server
COMMIT_HASH=$(git rev-parse --short=12 HEAD)
gcloud builds submit --timeout 1h --tag $IMAGE:$COMMIT_HASH
docker tag $IMAGE:$COMMIT_HASH $IMAGE:latest
```

Deployment is handled as part of the [Pulumi
configuration](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#automation),
with the exception of [copying the Hail token](../tokens) for the project's
Hail Batch service account.

As the Cloud Run HTTPS deployment endpoint addresses seem to be unpredictable,
they currently need to be added manually to the [CLI tool](../cli).

# TODO: move this to pulumi
gcloud beta run deploy server --source . --service-acount server@analysis-runner.iam.gserviceaccount.com --region australia-southeast1 --no-allow-unauthenticated --platform managed

The deployment runs under the `server@analysis-runner.iam.gserviceaccount.com`
service account, which needs *Identity Platform Viewer* and *Secret Manager
Secret Accessor* roles.

## Testing locally

See [Testing the Container Image Locally](https://cloud.google.com/run/docs/testing/local)
for details.

Download a JSON key for the `server@analysis-runner.iam.gserviceaccount.com`
service account. Store the file name in the `$GSA_KEY_FILE` environment
variable. Then run:

```bash
docker build -t analysis-runner-server .

docker run -it -p 8080:8080 -v $GSA_KEY_FILE:/gsa-key/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json analysis-runner-server
```

This will start a server that listens locally on port 8080.