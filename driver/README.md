# Batch driver Docker images

This `Dockerfile` defines the "driver" images that are used run to launch a user's Hail Batch pipeline. That means within a container using this image, the user's code is checked out using git, typically followed by executing the user's Python script that defines a Hail Batch pipeline.

## `base`

Any driver image should be derived from the [`base`](Dockerfile.base) image that includes the critical dependencies for the analysis-runner. As it changes infrequently, it gets built manually:

```shell
DOCKER_IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-base:1.0
docker build -f Dockerfile.base --tag=$DOCKER_IMAGE . && docker push $DOCKER_IMAGE
```

## `hail`

The [`hail`](Dockerfile.hail) image adds Hail support and is used by default in the analysis-runner and gets built and pushed automatically as part of the [Hail update workflow](../.github/workflows/hail_update.yaml).

## `r`

The [`r`](Dockerfile.r) image adds R-tidyverse packages to the `base` image. As it changes infrequently, it's built manually. Also see the [R example](../examples/r) on how to use this image.

```shell
DOCKER_IMAGE=australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-r:1.0
docker build -f Dockerfile.r --tag=$DOCKER_IMAGE . && docker push $DOCKER_IMAGE
```
