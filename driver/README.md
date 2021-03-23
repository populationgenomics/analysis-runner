# Batch driver Docker image

This `Dockerfile` defines the "driver" image that's run to launch a user's Hail
Batch pipeline. That means within a container using this image, the user's code
is checked out using git, followed by executing the user's Python script that
defines the Hail Batch pipeline.

Therefore, the main dependency that's installed is Hail, which comes with the
Batch API bindings.

The driver image gets rebuilt and pushed automatically as part of the [Hail update workflow](../.github/workflows/hail_update.yaml).


