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
  --cram gs://cpg-fewgenomes-test/benchmark/inputs/toy/NA12878-chr21-tiny.cram \
  --output gs://cpg-fewgenomes-test/analysis-runner-test/batch/output/NA12878-chr21-tiny-subset.cram
```

Check that the pipeline output GCS bucket contains the expected output file:

```bash
gsutil ls gs://cpg-fewgenomes-test/analysis-runner-test/batch/output/
gs://cpg-fewgenomes-test/analysis-runner-test/batch/output/NA12878-chr21-tiny-subset.cram
```
