# Server deployment using Cloud Run

This directory contains the server component of the analysis runner, which is
deployed as a Cloud Run container.

It uses `aiohttp` instead of `flask` because Hail Batch's async event queue
doesn't mix well with `flask`'s threads.

Each dataset / [storage
stack](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
has its own Cloud Run deployment. This way, memberships in the respective
permission groups (`$STACK-restricted-access@populationgenomics.org.au`)
can be checked by assigning the _Cloud Run Invoker_ IAM role to the group.
While there's also a Cloud Identity API to check group memberships, this
feature unfortunately is [only available in Google Workspace Enterprise
editions](https://googlecloudproject.com/identity/docs/reference/rest/v1/groups.memberships/checkTransitiveMembership).

To build a new Docker image for the server, run:

```bash
gcloud config set project analysis-runner

IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/server
COMMIT_HASH=$(git rev-parse --short=12 HEAD)
echo $COMMIT_HASH
gcloud builds submit --timeout 1h --tag $IMAGE:$COMMIT_HASH
```

Deployment is handled as part of the [Pulumi
configuration](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#automation),
which references the `$COMMIT_HASH` in the Docker image reference.

Hail service account [tokens](../tokens) need to be copied to Secret Manager secrets
separately, after the stack has been set up.

As the Cloud Run HTTPS deployment endpoint addresses seem to be unpredictable,
they currently need to be added manually to the [CLI tool](../cli).

## Testing locally

See [Testing the Container Image Locally](https://cloud.google.com/run/docs/testing/local)
for details.

Download a JSON key for the `analysis-runner-server` service account for a `$DATASET`
in its corresponding `$GCP_PROJECT`. Store the file name in the `$GSA_KEY_FILE`
environment variable. Then run:

```bash
docker build -t analysis-runner-server .

docker run -it -p 8080:8080 -v $GSA_KEY_FILE:/gsa-key/key.json -e GCP_PROJECT=$GCP_PROJECT -e DATASET=$DATASET -e GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json analysis-runner-server
```

This will start a server that listens locally on port 8080.

From another terminal, send a request like this, replacing the JSON parameters
accordingly.

```bash
TOKEN=$(gcloud auth print-identity-token) curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type:application/json" -d '{"output": "gs://test-bucket/test", "repo": "hail-batch-test", "commit": "0fa3abfe59692618578c4e1551b2a9357566d2ad", "script": ["main.py"], "description": "test"}' localhost:8080
```
