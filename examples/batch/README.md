# Example of a Hail Batch workflow

Here we run a bash script that runs a Hail Batch workflow on a CRAM file.

```bash
cd examples/batch
# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level test \
  --dataset fewgenomes \
  --description "Run Batch" \
  --output-dir "$(whoami)_test_batch" \
  batch.py \
  --cram gs://cpg-fewgenomes-test/benchmark/inputs/toy/NA12878-chr21-tiny.cram
```

Check that the analysis-runner output GCS bucket contains
files (analysis-runner metadata):

```bash
gsutil ls gs://cpg-fewgenomes-test-tmp/$(whoami)_test_batch/metadata/
```

Check that the pipeline output GCS bucket contains an output file:

```bash
gsutil ls s://cpg-fewgenomes-test-tmp/$(whoami)_test_batch/outputs/
```
