# Analysis Runner CLI

CLI for interacting with the REMOTE analysis-runner (externally hosted `server/main.py` script).

Motivation: [Source](https://github.com/populationgenomics/analysis-runner/issues/8)

There are two ways to run the analysis-runner CLI: with the `--repository` parameter, and without:

1. Omitting the `--repository` parameter: use the repository of the local directory that you're in: (a) get the repository name from the git remote; (b) use the commit of HEAD (if the `--commit` parameter is omitted); (c) Make the script path relative to the root of the git repository.

```bash
# cwd is the git directory root
cd path/to/script
analysis-runner \
    --dataset <dataset> \
    --description "Description of the run" \
    --output-dir gs://<bucket> \
    main.py and some arguments # becomes path/to/script/main.py and some arguments
```

1. Providing the `--repository` parameter, making the script path relative to the git repository is disabled, and you must provide a commit hash too. For example:

```bash
# You must specify relative path from git root to script
analysis-runner \
    --dataset <dataset> \
    --description "Description of the run" \
    --output-dir gs://<bucket> \
    --repository <repository> \
    --commit <hash> \
    path/to/script/main.py and some arguments
```

## CLI Overview

Process:

1. Fill in the mising info (repository, commit hash)
1. Get the gcloud auth token
   - `gcloud auth print-identity-token`
   - `google.auth.default()[0].id_token` (after credentials refresh)
1. Form a POST request with params:
   - dataset
   - output
   - repo
   - extendedAccess
   - commit
   - script
   - description
1. Collect and print response

## Requirements

These packages are pinned to specific versions in the `conda/analysis-runner/meta.yaml` config.

- `click` for the command line
- `google-auth` to request the identity token
