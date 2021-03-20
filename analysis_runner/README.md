# Analysis Runner CLI and library

## CLI

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

## Library

This package also contains convenience modules for writing scripts that will be
executed through the analysis-runner.

- [`dataproc.py`](dataproc.py): a wrapper for starting a Dataproc cluster from within
  Hail Batch and submitting a Query script to it ([example](../examples/dataproc)).
