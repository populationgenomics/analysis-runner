# Analysis runner

This tool helps to [make analysis results reproducible](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md),
by automating the following aspects:

- Only run code that has been committed to a repository.
- Link the output data with the exact program invocation of how the data has
  been generated.

One of our main workflow pipeline systems at the CPG is Hail Batch. By
default, its pipelines are defined by running a Python program
_locally_. This tool instead lets you run the "driver" on Hail Batch itself.

Furthermore, all invocations are logged together with the output data, as well as [Airtable](https://airtable.com/tblx9NarwtJwGqTPA/viwIomAHV49Stq5zr).

When using the analysis-runner, the batches are not run under your standard
Hail Batch service account user. Instead, a separate Hail Batch account is
used to run the batch on your behalf. There's a dedicated Batch service
account for each dataset (e.g. "tob-wgs"), which helps with bucket permission
management and billing budgets.

## CLI

CLI helps request the analysis-runner to start pipelines based on a GitHub
repository, commit, and command to run. To install it, use conda:

```bash
conda install -c cpg -c conda-forge analysis-runner
```

Usage:

```bash
Usage: analysis-runner [OPTIONS] [SCRIPT]...
Options:
  --dataset TEXT             The dataset name, which determines which
                             analysis-runner server to send the request to
                             [required]

  -o, --output-dir TEXT      The output directory of the run, MUST start with
                             gs://  [required]

  --repository, --repo TEXT  The URI of the repository to run, must be
                             approved by the appropriate server. Default
                             behavior is to find the repository of the current
                             working directory with `git remote get-url
                             origin`

  --commit TEXT              The commit HASH or TAG of a commit to run, the
                             default behavior is to use the current commit of
                             the local repository, however the literal value
                             "HEAD" is not allowed.

  --description TEXT         Description of job, otherwise defaults to: "$USER
                             FROM LOCAL: $REPO@$COMMIT"  [required]

  --version                  Show the version and exit.
  --help                     Show this message and exit.
```

If you're in the directory of the project you want to run, you can omit
the `--commit` and `--repository` parameters, which will use your current REMOTE
and commit HEAD.

For example:

```bash
analysis-runner \
    --dataset <dataset> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

If you provide a `--repository`, you MUST supply a `--commit <SHA>`, eg:

```bash
analysis-runner \
    --repository my-approved-repo \
    --commit <commit-sha> \
    --dataset <dataset> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

## Development

You can ignore this section if you just want to run the tool.

To bring up a stack corresponding to a dataset as described in the
[storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies),
see the [stack](stack) directory.

To set up a development environment for the analysis runner using conda, run
the following:

```bash
conda env create --file environment-dev.yml

conda activate analysis-runner

pre-commit install

pip install --editable .
```

### Deployment

1. Add a Hail Batch service account for all supported datasets.
1. [Copy the Hail tokens](tokens) to the Secret Manager.
1. Deploy the [server](server) by invoking the [`hail_update` workflow](https://github.com/populationgenomics/analysis-runner/blob/main/.github/workflows/hail_update.yaml) manually, specifying the Hail package version in conda.
1. Deploy the [Airtable](airtable) publisher.
1. Publish the [CLI tool and library](analysis_runner) to conda.

Note that the [`hail_update` workflow](https://github.com/populationgenomics/analysis-runner/blob/main/.github/workflows/hail_update.yaml) gets invoked whenever a new Hail package is published to conda. You can test this manually as follows:

```bash
curl \
  -X POST \
  -H "Authorization: token $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/populationgenomics/analysis-runner/actions/workflows/6364059/dispatches \
  -d '{"ref": "main", "inputs": {"hail_version": "0.2.63.deveb7251e548b1"}}'
```

The CLI tool is shipped as a conda package. To build a new version,
we use [bump2version](https://pypi.org/project/bump2version/).
For example, to increment the patch section of the version tag 1.0.0 and make
it 1.0.1, run:

```bash
git checkout -b bump-version-for-release
bump2version patch
git push --set-upstream origin add-new-version
# Open pull request
open "https://github.com/populationgenomics/analysis-runner/pull/new/add-new-version"
```

It's important the pull request name start with "Bump version:" (which should happen by default).
Once this is merged into `main`, a GitHub action workflow will build a new conda package, that
will be uploaded to the conda [CPG channel](https://anaconda.org/cpg/),
and become available to install with `conda install -c cpg -c conda-forge ...`
