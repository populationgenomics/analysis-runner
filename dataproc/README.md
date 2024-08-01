# Dataproc at CPG

Dataproc is a managed Spark and Hadoop service provided by Google that we use for running Hail query analyses. For the most part, we want to use Query on Batch, but occasionally we must fallback to Dataproc.

The `cpg-utils` package includes helper functions for managing Dataproc clusters, including spinning up and down clusters, and submitting jobs. See [Team Docs: cpg-utils/dataproc](https://populationgenomics.readthedocs.io/en/latest/cpg-utils/dataproc.html) for a guide on how to make Hail Query interact with Dataproc.

> ### Issues with running Dataproc
>
> If you have issues with dataproc, please see the [Playbooks: Hail Query on Dataproc](https://populationgenomics.readthedocs.io/en/latest/playbooks/hail_query_on_dataproc.html) guide.

## Docker driver image

The dataproc image is effectively a driver image for the _setting up_, _submitting to_, and _spinning down_ the cluster. It doesn't do anything too fancy, but it's useful to keep this disconnected from the regular analysis-runner driver image to avoid any arbitrary changes to pip_dependencies in the deploy_config (`/usr/local/lib/python3.10/dist-packages/hailtop/hailctl/deploy.yaml`).

Note that our [Hail fork](https://github.com/populationgenomics/hail) at `HEAD` is used to build the image. You can use the `COMMIT_HASH` arg to build an image with a specific version of hail instead of the latest version, which is what `HEAD` will use. We manually build our version of Hail in the dataproc container. (Ideally it would be good to use a multistage build process to reduce this image size, but we don't want to support dataproc into the indefinite future - so not much motivation to do so).

## Spinning up a dataproc cluster

A dataproc cluster is spun up within a specific dataset's GCP project for billing reasons.

We call `hailctl dataproc start`, as configured in the `analysis_runner/dataproc` module. We specify a number of default packages in this module as a sensible default. We by default specify the init script (`gs://cpg-common-main/hail_dataproc/${HAIL_VERSION}/`), but you can override this on cluster configuration.

By default, Hail specifies the image to use on Dataproc. The image version comes from the command [`dataproc cluster image version lists`](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters#debian_images), and is specified here: [hail:hail/python/hailtop/hailctl/dataproc/start.py#L147](https://github.com/populationgenomics/hail/blob/main/hail/python/hailtop/hailctl/dataproc/start.py#L147).

At the time of writing (2023-11-24), this was using Python 3.10.8.

### Initialization script

The scripts in [`init_scripts`](init_scripts) are used to install dependencies on Dataproc master nodes through the `init` parameter of the `setup_dataproc` function. The scripts get copied to `gs://cpg-common-main/hail_dataproc/${HAIL_VERSION}` [automatically](../.github/workflows/copy_dataproc_init_scripts.yaml) on pushes to `main`.

## Updating dataproc

When you're trying to update the default version of dataproc, you should:

1. Bump the `DEFAULT_HAIL_VERSION` in [`cpg-utils/dataproc.py`](https://github.com/populationgenomics/cpg-utils/blob/214958b7be037e5153ef60f5d4b71b5be8409db8/cpg_utils/dataproc.py#L28).
    * Side note: hail must be released before this happens, including the wheel at `gs://cpg-hail-ci/wheels/hail-{HAIL_VERSION}-py3-none-any.whl`.
2. Completely release the cpg-utils CLI (merge to main with a bumpversion commit).
3. Release the `init_scripts` by running this [GitHub workflow](https://github.com/populationgenomics/analysis-runner/actions/workflows/copy_dataproc_init_scripts.yaml).
4. Rebuild the dataproc image, using the [command below](#rebuilding-the-dataproc-driver-image).
5. Rebuild the analysis-runner driver image:
   * This implicitly picks up the new version of cpg-utils, and is the important one for scripts to set up jobs to use the new version of dataproc.
   * You _may_ also need to update the prod-pipes image.

Note, cpg-utils support specifying the hail_version, but only a very select number of versions are available (due to the init scripts not always being updated).

### Rebuilding the dataproc driver image

```sh
gcloud config set project analysis-runner
# grab the HAIL_VERSION from cpg-utils/dataproc.py
HAIL_VERSION="0.2.132"

# if from repo root
cd dataproc
gcloud builds submit \
  --timeout=1h \
  --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-$HAIL_VERSION \
  .
```
