# Dataproc Docker image

This Docker image is used to launch Dataproc clusters when using the
[dataproc helper module](../analysis_runner/dataproc.py).

Note that our [Hail fork](https://github.com/populationgenomics/hail) at `HEAD` is used to build the image.

When you change the `HAIL_VERSION` below, make sure to update [dataproc.py](../analysis_runner/dataproc.py) accordingly and release a new `analysis-runner` library package.

To build, run:

```sh
gcloud config set project analysis-runner
HAIL_VERSION=0.2.97
gcloud builds submit --timeout=1h --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-$HAIL_VERSION .
```
