# Batch driver Docker image

This `Dockerfile` defines the "driver" image that's run to launch a user's Hail
Batch pipeline. That means within a container using this image, the user's code
is checked out using git, followed by executing the user's Python script that
defines the Hail Batch pipeline.

Therefore, the main dependency that's installed is Hail, which comes with the
Batch API bindings.

To build this image, make sure that you don't have any uncommitted changes.
Then run:

```bash
gcloud config set project analysis-runner

IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/driver
COMMIT_HASH=$(git rev-parse --short=12 HEAD)
gcloud builds submit --timeout 1h --tag $IMAGE:$COMMIT_HASH
docker tag $IMAGE:$COMMIT_HASH $IMAGE:latest
```

Update the corresponding version reference in the [server](../server) and
redeploy.
