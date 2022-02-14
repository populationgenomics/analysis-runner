# Example of a Hail Batch workflow

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
