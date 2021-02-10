# Analysis Runner CLI

CLI for interacting with the REMOTE analysis-runner (externally hosted `server/main.py` script).

Motivation: [Source](https://github.com/populationgenomics/analysis-runner/issues/8)

> A simple CLI tool that gathers some of the parameters for the request that's sent to the
> server automatically: e.g. GitHub repository, commit hash. It should also map a
> human-readable project name to the correct HTTPS endpoint.

Called with:

```bash
analysis-runner run \
    --dataset <ds-id> \
    --output-dir gs://some-path/ \
    [--commit-hash <current-hash>] \
    [--repo <current-repo>]  \
    script.py to run with args
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
