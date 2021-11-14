# Dataproc Docker image

This Docker image is used to launch Dataproc clusters when using the
[dataproc helper module](../analysis_runner/dataproc.py).

To build, run:

```sh
gcloud config set project analysis-runner

gcloud builds submit --timeout=1h --tag=australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:cpg-hail==0.2.78 .
```

The image uses the CPG build of hail python package:

* until [this pull request](https://github.com/hail-is/hail/pull/10863) is merged; so we can add a package in a zip file using `--pyfiles`;
* until package is build for py39.

To build the package:

```sh
git clone -b dataproc-pyfiles https://github.com/populationgenomics/hail.git
cd hail
make wheel
twine upload build/deploy/dist/cpg-hail-0.2.78.tar.gz
```
