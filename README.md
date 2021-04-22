# Analysis runner

This tool helps to [make analysis results reproducible](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md),
by automating the following aspects:

- Allow quick iteration using an environment that resembles production.
- Only allow access to production datasets through code that has been reviewed.
- Link the output data with the exact program invocation of how the data has
  been generated.

One of our main workflow pipeline systems at the CPG is
[Hail Batch](https://hail.is/docs/batch/getting_started.html). By default, its
pipelines are defined by running a Python program
_locally_. This tool instead lets you run the "driver" on Hail Batch itself.

Furthermore, all invocations are logged together with the output data, as well as [Airtable](https://airtable.com/tblx9NarwtJwGqTPA/viwIomAHV49Stq5zr).

When using the analysis-runner, the batch jobs are not run under your standard
Hail Batch [service account user](https://hail.is/docs/batch/service.html#sign-up)
(`<USERNAME>-trial`). Instead, a separate Hail Batch account is
used to run the batch jobs on your behalf. There's a dedicated Batch service
account for each dataset (e.g. "tob-wgs", "fewgenomes") and access level
("test", "standard", or "full", as documented in the team docs
[storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#analysis-runner)),
which helps with bucket permission management and billing budgets.

## CLI

CLI helps request the analysis-runner to start pipelines based on a GitHub
repository, commit, and command to run. To install it, use mamba:

```bash
mamba install -c cpg -c conda-forge analysis-runner
```

Run `analysis-runner --help` to see usage information.

Make sure that you're logged into GCP:

```bash
gcloud auth application-default login
```

If you're in the directory of the project you want to run, you can omit the
`--commit` and `--repository` parameters, which will use your current git remote and
commit HEAD.

For example:

```bash
analysis-runner \
    --dataset <dataset> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

If you provide a `--repository`, you MUST supply a `--commit <SHA>`, e.g.:

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

To set up a development environment for the analysis runner using mamba, run
the following:

```bash
mamba env create --file environment-dev.yml

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
git checkout -b add-new-version
bump2version patch
git push --set-upstream origin add-new-version
# Open pull request
open "https://github.com/populationgenomics/analysis-runner/pull/new/add-new-version"
```

It's important the pull request name start with "Bump version:" (which should happen
by default). Once this is merged into `main`, a GitHub action workflow will build a
new conda package that will be uploaded to the conda [CPG
channel](https://anaconda.org/cpg/), and become available to install with `mamba install -c cpg -c conda-forge ...`

## Limitations

- Your script name cannot contain spaces, even if you quote it.
