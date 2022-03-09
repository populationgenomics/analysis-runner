# R example

Here we run a simple R script that writes a `data.frame` to a TSV file, and
exports a ggplot2 plot to a PNG file. These files are subsequently copied to
the specified output GCS bucket.

```bash
cd examples/r

dataset="fewgenomes"
outdir="$(whoami)-test-r-1"
# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level "test" \
  --dataset ${dataset} \
  --description "testing R" \
  --output-dir ${outdir} \
  --image australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-r:1.0.0
  script.R

# Check that the output GCS bucket contains the files:
gsutil ls gs://cpg-${dataset}-test-tmp/${outdir}
```
