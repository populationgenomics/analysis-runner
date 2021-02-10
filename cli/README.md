# Analysis Runner CLI

CLI for interacting with the REMOTE analysis-runner (externally hosted `server/main.py` script).

Motivation: [Source](https://github.com/populationgenomics/analysis-runner/issues/8)

Usage:

```bash
analysis-runner \
    --dataset <dataset> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

If you provide a `--repository`, you MUST supply
a `--commit <SHA / or tag>`, eg:

```bash
analysis-runner \
    --repository my-approved-repo \
    --commit <commit-sha> \
    --dataset <dataset> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

## CLI Overview

Process:

1. Fill in the mising info (repository, commit hash)
2. Get the gcloud auth token
    * `gcloud auth print-identity-token`
    * `google.auth.default()[0].id_token` (after credentials refresh)
3. Get the submit URL from the `dataset` parameter
    * Do this by looking up a JSON map from `servermap.json` (in this repo)
4. Form a POST request with params:
    * output
    * repo
    * commit
    * script
    * description
5. Collect and print response

## Requirements

These packages are pinned to specific versions in the `conda/analysis-runner/meta.yaml` config.

* `click` for the command line
* `google-auth` to request the identity token
