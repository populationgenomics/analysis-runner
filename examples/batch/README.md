# Example of a Hail Batch workflow

Here we run a bash script that runs a Hail Batch workflow on a CRAM file.

```bash
cd examples/batch
# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level test \
  --dataset fewgenomes \
  --description "Run Batch" \
  --output-dir "analysis-runner-test/$(whoami)/batch" \
  batch.py \
  --cram gs://cpg-fewgenomes-test/analysis-runner-test/batch/input.cram \
  --region chr21:1-10000
```

Check that the pipeline output GCS bucket contains the expected output file:

```bash
gsutil ls gs://cpg-fewgenomes-test/analysis-runner-test/$(whoami)/batch
# gs://cpg-fewgenomes-test/analysis-runner-test/$(whoami)/batch/input-subset.cram
```
