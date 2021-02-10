# Analysis runner

This tool helps to [make analysis results reproducible](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md),
by automating the following aspects:

- Only run code that has been committed to a repository.
- Link the output data with the exact program invocation of how the data has
  been generated.

One of our main workflow pipeline systems at the CPG is Hail Batch. By
default, its pipelines are defined by running a Python program
_locally_. This tool instead lets you run the "driver" on Hail Batch itself.

Furthermore, all invocations are logged together with the output data.

When using the analysis-runner, the batches are not run under your standard
Hail Batch service account user. Instead, a separate Hail Batch account is
used to run the batch on your behalf. There's a dedicated Batch service
account for each project (e.g. "tob-wgs"), which helps with bucket permission
management and billing budgets.

## Access through CLI

If you have the `analysis-runner` CLI installed, you can request the analysis-runner
to start pipelines based on a GitHub repository, commit, and command to run.

If you're in the directory of the project you want to run, you can omit
the `--commit` and `--repository` parameters, which will use your current REMOTE
and commit HEAD.

For example:

```bash
cpg-analysisrunner \
    --dataset <data-set> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

If you provide a `--repository`, you MUST supply a `--commit <SHA>`, eg:

```bash
cpg-analysisrunner \
    --repository my-approved-repo \
    --commit <commit-sha> \
    --dataset <data-set> \
    --description <description> \
    --output-dir gs://<bucket-path> \
    script_to_run.py with arguments
```

## Usage

**TODO(@lgruen):** Add instructions

## Deployment

You can ignore this section if you just want to run the tool.

To set up a development environment using conda, run the following:

```bash
conda env create --file environment.yml

conda activate analysis-runner

pre-commit install
```

1. Add a Hail Batch service account for all supported datasets.
1. [Copy the Hail tokens](tokens) to the Secret Manager.
1. Build the [driver image](driver).
1. Deploy the [server](server) for each dataset.
1. Publish the [CLI tool](cli) to conda.
