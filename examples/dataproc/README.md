# Dataproc example

This example shows how to run a Hail query script in Dataproc using Hail Batch. After installing the conda package for the analysis runner, you can run this as follows:

```bash
cd examples/dataproc

analysis-runner --dataset fewgenomes --access-level test --output-dir "gs://cpg-fewgenomes-test-tmp/$(whoami)-dataproc-example" --description "dataproc example" main.py
```
