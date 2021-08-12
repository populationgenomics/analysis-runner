# Bash example

Here we run a simple bash script that prints 'Hello, world!', or 'Hello, $1!' if you supply an argument.

```bash
cd examples/bash

# make sure you've installed https://anaconda.org/cpg/analysis-runner
analysis-runner \
  --access-level "test" \
  --dataset "fewgenomes" \
  --description "Hello, analysis-runner" \
  --output-dir "$(whoami)-test-bash" \
  hello.sh $(whoami)
```

Check that the output GCS bucket contains the files:

```bash
gsutil ls gs://cpg-fewgenomes-test-tmp/$(whoami)-test-bash/
```
