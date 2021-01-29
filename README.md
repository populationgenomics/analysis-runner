# Analysis runner

This tool helps to [make analysis results reproducible](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md),
by automating the following aspects:

- Only run code that has been committed to a repository.
- Link the output data with the exact program invocation of how the data has
  been generated.

One of our main workflow pipeline systems at the CPG is Hail Batch. By
default, its pipelines are defined by running a Python program
*locally*. This tool instead lets you run the "driver" on Hail Batch itself.

Furthermore, all invocations are logged together with the output data.

When using the analysis-runner, the batches are not run under your standard
Hail Batch service account user. Instead, a separate Hail Batch account is
used to run the batch on your behalf. There's a dedicated Batch service
account for each project (e.g. "tob-wgs"), which helps with bucket permission
management and billing budgets.

## Usage

**TODO(@lgruen):** Add instructions

## Deployment

You can ignore this section if you just want to run the tool.

To set up a development environment using conda, run the following:

```bash
conda create --name analysis-runner -c cpg -c bioconda -c conda-forge hail pre-commit kubernetes=12.0.1 google-cloud-secret-manager=2.2.0

conda activate analysis-runner

pre-commit install
```

1. Add a Hail Batch service account for all supported projects.
1. [Copy the Hail tokens](tokens) to the Secret Manager. This step needs to be
   repeated whenever a new project is added.
1. Build the [driver image](driver).
