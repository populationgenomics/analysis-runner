# Server deployment using Cloud Run

This directory contains the server component of the analysis runner, which is
deployed as a Cloud Run container.

It uses `aiohttp` instead of `flask` because Hail Batch's async event queue
doesn't mix well with `flask`'s threads.

Each dataset / [storage
stack](https://github.com/populationgenomics/team-docs/tree/main/storage_policies)
has its own permissions groups, which are checked through a single analysis-runner instance.

To build a new Docker image for the server, run:

```bash
gcloud config set project analysis-runner

IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/server
COMMIT_HASH=$(git rev-parse --short=12 HEAD)
echo $COMMIT_HASH
gcloud builds submit --timeout 1h --tag $IMAGE:$COMMIT_HASH
```

Deployment happens continuously using the [`hail_update` workflow](https://github.com/populationgenomics/analysis-runner/blob/main/.github/workflows/hail_update.yaml). However, if you ever need to deploy manually, run:

```bash
gcloud run deploy server --region australia-southeast1 --no-allow-unauthenticated \
    --service-account analysis-runner-server@analysis-runner.iam.gserviceaccount.com \
    --platform managed --set-env-vars=DRIVER_IMAGE=$DRIVER_IMAGE --image $IMAGE:$COMMIT_HASH
```

Hail service account [tokens](../tokens) need to be copied to a Secret Manager secret
separately, after the stacks have been set up.

The Cloud Run HTTPS deployment endpoint is hardcoded in the [CLI tool](../analysis_runner).

## Testing locally

See [Testing the Container Image Locally](https://cloud.google.com/run/docs/testing/local)
for details.

Download a JSON key for the `analysis-runner-server` service account. Store the file name in the `$GSA_KEY_FILE` environment variable. Then run:

```bash
docker build -t analysis-runner-server .

docker run -it -p 8080:8080 -v $GSA_KEY_FILE:/gsa-key/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json -e DRIVER_IMAGE=$DRIVER_IMAGE analysis-runner-server
```

This will start a server that listens locally on port 8080.

From another terminal, send a request like this, replacing the JSON parameters
accordingly.

```bash
TOKEN=$(gcloud auth print-identity-token) curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type:application/json" -d '{"output": "gs://test-bucket/test", "dataset": "fewgenomes", "accessLevel": "test", "repo": "hail-batch-test", "commit": "0fa3abfe59692618578c4e1551b2a9357566d2ad", "script": ["main.py"], "description": "test"}' localhost:8080
```
