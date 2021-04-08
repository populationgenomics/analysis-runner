# Analysis Runner CLI and library

## CLI

There are two ways to run the analysis-runner CLI:

- **Omitting** the `--repository` parameter: the repository containing the local
  directory that you're in is used. The repository name will be retrieved from
  the git remote URL, while the commit of HEAD is used (if the `--commit`
  parameter is omitted). The script path is made relative to the root of the git
  repository. For example:

```bash
# cwd is the git directory root
cd path/to/script
analysis-runner \
    --dataset <dataset> \
    --description "Description of the run" \
    --output-dir gs://<bucket> \
    main.py and some arguments # becomes path/to/script/main.py and some arguments
```

- **Providing** the `--repository` parameter: making the script path relative to
  the git repository is disabled, and you must provide a commit hash too. For
  example:

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

### [Dataproc](dataproc.py)

Provides a wrapper for starting a Dataproc cluster from within Hail Batch and
submitting a Query script to it ([example](../examples/dataproc)). This is
particularly useful as an intermediate solution before all Hail Query features
are supported by the `ServiceBackend`.

**Note:** for this to work, the Hail Batch service accounts will need the IAM
permissions below, which are not set by default. Reach out in the
`#team-software` channel if you need this to be set up for your project.

- _Dataproc Administrator_ (at the project level)
- _Dataproc Worker_ (at the project level)
- _Service Account User_ (on the account itself...!)
