# Dataproc Docker image

Dataproc is a managed Spark and Hadoop service provided by Google that we use for running Hail query analyses. For the most part, we want to use Query on Batch, but occasionally we must fallback to Dataproc.

The analysis-runner package includes helper functions for managing Dataproc clusters, including spinning up and down clusters, and submitting jobs.

```python
from analysis_runner import dataproc
from cpg_utils.hail_batch import get_batch

b = get_batch()

# this command adds 3 jobs to:
#   1. Start the dataproc cluster
#   2. Submit the file to the cluster, which:
#       1. Checks out the repository you're currently in (at the specific commit of the repo you're in on run)
#       2. Submits the file to the dataproc cluster using the hailctl command
#   3. Shut down the cluster
j = dataproc.hail_dataproc_job(
    batch=b,
    script=script,
    packages=['cpg_workflows', 'google', 'fsspec', 'gcloud'],
    num_workers=2,
    num_secondary_workers=20,
    job_name=job_name,
    depends_on=depends_on,
    scopes=['cloud-platform'],
    pyfiles=pyfiles,
)
```

## Docker image

The dataproc image is effectively a driver image for the _setting up_, _submitting to_, and _spinning down_ the cluster. It doesn't do anything too fancy, but it's useful to keep this disconnected from the regular analysis-runner driver image to avoid any arbitrary changes to pip_dependencies in the deploy_config (`/usr/local/lib/python3.10/dist-packages/hailtop/hailctl/deploy.yaml`).

Note that our [Hail fork](https://github.com/populationgenomics/hail) at `HEAD` is used to build the image. You can use the `COMMIT_HASH` arg to build an image with a specific version of hail instead of the latest version, which is what `HEAD` will use.

We manually build our version of Hail in the dataproc container. (Ideally it would be good to use a multistage build process to reduce this image size, but we don't want to support dataproc into the indefinite future).

## Spinning up a dataproc cluster

A dataproc cluster is spun up within a specific dataset's GCP project for billing reasons.

We call `hailctl dataproc start`, as configured in the `analysis_runner/dataproc` module. We specify a number of default packages in this module as a sensible default. We by default specify the init script (`gs://cpg-common-main/hail_dataproc/${HAIL_VERSION}/`), but you can override this on cluster configuration.

### Initialization script

The scripts in [`init_scripts`](init_scripts) are used to install dependencies on Dataproc master nodes through the `init` parameter of the `setup_dataproc` function. The scripts get copied to `gs://cpg-common-main/hail_dataproc/${HAIL_VERSION}` [automatically](../.github/workflows/copy_dataproc_init_scripts.yaml) on pushes to `main`.

## Updating dataproc

When you're trying to update the default version of dataproc, you should:

1. Bump the `DEFAULT_HAIL_VERSION` in `analysis_runner/dataproc.py`
    * Side note: hail must be released before this happens, including the wheel at `gs://cpg-hail-ci/wheels/hail-{HAIL_VERSION}-py3-none-any.whl`
2. Completely release the analysis-runner CLI (merge to main with a bumpversion commit)
3. Rebuild the dataproc image, using the [command below](#rebuilding-the-dataproc-driver-image)
4. Rebuild the analysis-runner driver image

Note, we support specifying the hail_version, but only a very select number of versions are available (due to the init scripts not always being updated).

### Rebuilding the dataproc driver image

```sh
gcloud config set project analysis-runner
# grab the HAIL_VERSION from analysis_runner/dataproc.py
HAIL_VERSION=$(grep "DEFAULT_HAIL_VERSION = '" analysis_runner/dataproc.py | awk -F\' '{print $2}')
gcloud builds submit --timeout=1h --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-$HAIL_VERSION .
```

## Known Issues

### hail*.whl is not a valid wheel filename

You may see the following error when the `./init_scripts/install_common.sh` script is being executed during cluster initialization:

```log
pip3 install --no-dependencies '/home/hail/hail*.whl'
WARNING: Requirement '/home/hail/hail*.whl' looks like a filename, but the file does not exist
ERROR: hail*.whl is not a valid wheel filename.
```

This is due to the hail wheel not being correctly localised to the image. This could due to a few reasons:

1. The hail wheel blob is misconfigured (hence doesn't get copied to the instance, so the wildcard fails to resolve)
    * [https://centrepopgen.slack.com/archives/C030X7WGFCL/p1681793836586189](https://centrepopgen.slack.com/archives/C030X7WGFCL/p1681793836586189)
    * [https://centrepopgen.slack.com/archives/C030X7WGFCL/p1680587094857899?thread_ts=1679886207.438349&cid=C030X7WGFCL](https://centrepopgen.slack.com/archives/C030X7WGFCL/p1680587094857899?thread_ts=1679886207.438349&cid=C030X7WGFCL)
    * [https://centrepopgen.slack.com/archives/C030X7WGFCL/p1669677571568879?thread_ts=1669161375.010299&cid=C030X7WGFCL](https://centrepopgen.slack.com/archives/C030X7WGFCL/p1669677571568879?thread_ts=1669161375.010299&cid=C030X7WGFCL)

1. It's just transient (eg: [https://centrepopgen.slack.com/archives/C030X7WGFCL/p1685164583164109](https://centrepopgen.slack.com/archives/C030X7WGFCL/p1685164583164109))

1. A failure at a previous step which uses the `deploy.yaml` file. This file contains info on pip dependencies and the location of the hail wheel file. So, if one part of this step fails, for example the pip intall from the `pip_dependencies` section of this yaml file, then the hail wheel won't be copied over to the cluster resulting in the above error. However, remember to check your error logs for the exact error message.

> The `STRIP_PIP_VERSIONS` Docker arg strips all pinned versions from the `deploy.yaml` file, which is created during the hail build step. This file is used by the hailctl command line program to set the `metadata` argument of the dataproc cluster create command. The metadata argument is by used the cluster to install specific versions of Python packages on the cluster. Set this to `true` if you are having issues with pip dependency resolution during cluster creation, which may be helpful for debugging.

### Dataproc cluster fails to start

You might see a log like this in Hail Batch. You'll need to check the output of this log.

```text
Initialization action failed. Failed action 'gs://cpg-common-main/hail_dataproc/<version>/install_common.sh', see output in: gs://cpg-$DATASET-$NAMESPACE-tmp/google-cloud-dataproc-metainfo/<cluster-id>/<cluster-name>-m/dataproc-initialization-script-1_output.
```

You may need to check other logs from this google cloud directory.
