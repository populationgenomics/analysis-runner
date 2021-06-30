# R example

Here we run a simple R script that writes a `data.frame` to a TSV file, and
exports a ggplot2 plot to a PNG file. These files are subsequently copied to
the specified output GCS bucket.

```bash
cd examples/r

# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level "test" \
  --dataset "tob-wgs" \
  --description "testing R" \
  --output-dir "$(whoami)-test-r" \
  script.R
```

Check that the output GCS bucket contains the files:

```bash
gsutil ls gs://cpg-tob-wgs-test-tmp/$(whoami)-test-r/
```
