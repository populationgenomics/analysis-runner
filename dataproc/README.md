# Dataproc Docker image

This Docker image is used to launch Dataproc clusters when using the
[dataproc helper module](../analysis_runner/dataproc.py).

Note that our [Hail fork](https://github.com/populationgenomics/hail) at `HEAD` is used to build the image.

When you change the `HAIL_VERSION` below, make sure to update [dataproc.py](../analysis_runner/dataproc.py) accordingly and release a new `analysis-runner` library package.

To build, run:

```sh
gcloud config set project analysis-runner
HAIL_VERSION=0.2.110
gcloud builds submit --timeout=1h --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-$HAIL_VERSION .
```

## Initialization script

The scripts in [`init_scripts`](init_scripts) are used to install dependencies on Dataproc master nodes through the `init` parameter of the `setup_dataproc` function. The scripts get copied to `gs://cpg-common-main/hail_dataproc/` [automatically](../.github/workflows/copy_dataproc_init_scripts.yaml) on pushes to `main`.
