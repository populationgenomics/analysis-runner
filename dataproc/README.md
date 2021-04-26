# Dataproc Docker image

This Docker image is used to launch Dataproc clusters when using the
[dataproc helper module](../analysis_runner/dataproc.py).

To build, run:

```sh
gcloud builds submit --tag australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-0.2.65 .
```
