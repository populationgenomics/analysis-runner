# Example of a Hail Batch workflow

Here we run a bash script that runs a Hail Batch workflow, that would print 'Hello, world!', or 'Hello, $1!' if you supply an argument.

```bash
cd examples/pipeline
# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level test \
  --dataset fewgenomes \
  --description "Run Batch with cpg-pipes" \
  --output-dir "$(whoami)-test-pipeline" \
  run.sh

batch.py \
--name $(whoami) \
--output-bucket "gs://cpg-fewgenomes-test-tmp/$(whoami)-test-pipeline/outputs"

```

Check that the analysis-runner output GCS bucket contains
files (analysis-runner metadata):

```bash
gsutil ls gs://cpg-fewgenomes-test-tmp/$(whoami)-test-pipeline/metadata/
```

Check that the pipeline output GCS bucket contains an output file:

```bash
gsutil ls s://cpg-fewgenomes-test-tmp/$(whoami)-test-pipeline/outputs/
```
