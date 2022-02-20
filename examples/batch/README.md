# Hail Batch workflow examples

## Hello world

This example creates a Hail Batch workflow with two jobs:

1. Prints "Hello, $name" to a file
2. Cats the output of the file from the previous job

```bash
cd examples/batch
analysis-runner \
  --access-level test \
  --dataset fewgenomes \
  --description "Run Batch" \
  --output-dir "$(whoami)/hello-world" \
  hello.py \
  --name-to-print $(whoami)
```

## Process cram

Here we run a bash script that runs a Hail Batch workflow on a CRAM file.

```bash
cd examples/batch
# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level test \
  --dataset fewgenomes \
  --description "Run Batch" \
  --output-dir "$(whoami)-test-batch" \
  batch.py \
  gs://cpg-fewgenomes-test/analysis-runner-test/batch/input.cram \
  chr21:1-10000
```
