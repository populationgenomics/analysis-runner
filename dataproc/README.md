# Dataproc Docker image

This Docker image is used to launch Dataproc clusters when using the
[dataproc helper module](../analysis_runner/dataproc.py).

Note that our [Hail fork](https://github.com/populationgenomics/hail) at `HEAD` is used to build the image. You can use the `COMMIT_HASH` arg to build an image with a specific version of hail instead of the latest version, which is what `HEAD` will use.

When you change the `HAIL_VERSION` below, make sure to update [dataproc.py](../analysis_runner/dataproc.py) accordingly and release a new `analysis-runner` library package.

To build, run:

```sh
gcloud config set project analysis-runner
HAIL_VERSION=0.2.126
gcloud builds submit --timeout=1h --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-$HAIL_VERSION .
```

## Initialization script

The scripts in [`init_scripts`](init_scripts) are used to install dependencies on Dataproc master nodes through the `init` parameter of the `setup_dataproc` function. The scripts get copied to `gs://cpg-common-main/hail_dataproc/` [automatically](../.github/workflows/copy_dataproc_init_scripts.yaml) on pushes to `main`.

## Known Issues

You may see the following error when the `./init_scripts/install_common.sh` script is being executed during cluster initialization:

```log
pip3 install --no-dependencies '/home/hail/hail*.whl'
WARNING: Requirement '/home/hail/hail*.whl' looks like a filename, but the file does not exist
ERROR: hail*.whl is not a valid wheel filename.
```

This may be due to a failure at a previous step which uses the `deploy.yaml` file. This file contains info on pip dependencies and the location of the hail wheel file. So, if one part of this step fails, for example the pip intall from the `pip_dependencies` section of this yaml file, then the hail wheel won't be copied over to the cluster resulting in the above error. However, remember to check your error logs for the exact error message.

The `STRIP_PIP_VERSIONS` Docker arg strips all pinned versions from the `deploy.yaml` file, which is created during the hail build step. This file is used by the hailctl command line program to set the `metadata` argument of the dataproc cluster create command. The metadata argument is by used the cluster to install specific versions of Python packages on the cluster. Set this to `true` if you are having issues with pip dependency resolution during cluster creation.
