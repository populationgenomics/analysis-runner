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

Furthermore, all invocations are logged together with the output data, as well as [Airtable](https://airtable.com/tblx9NarwtJwGqTPA/viwIomAHV49Stq5zr) and the sample-metadata server.

When using the analysis-runner, the batch jobs are not run under your standard
Hail Batch [service account user](https://hail.is/docs/batch/service.html#sign-up)
(`<USERNAME>-trial`). Instead, a separate Hail Batch account is
used to run the batch jobs on your behalf. There's a dedicated Batch service
account for each dataset (e.g. "tob-wgs", "fewgenomes") and access level
("test", "standard", or "full", as documented in the team docs
[storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#analysis-runner)),
which helps with bucket permission management and billing budgets.

Note that you can use the analysis-runner to start arbitrary jobs, e.g. R scripts. They're just launched in the Hail Batch environment, but you can use any Docker image you like.

The analysis-runner is also integrated with our Cromwell server to run WDL based workflows.

## CLI

The analysis-runner CLI can be used to start pipelines based on a GitHub repository,
commit, and command to run. To install it, use pip:

```bash
pip install analysis-runner
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
    --access-level <level> \
    --output-dir <directory-within-bucket> \
    script_to_run.py with arguments
```

`<level>` corresponds to an [access level](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#analysis-runner) as defined in the storage policies.

`<directory-within-bucket>` does _not_ contain a prefix like `gs://cpg-fewgenomes-main/`. For example, if you want your results to be stored in `gs://cpg-fewgenomes-main/1kg_pca/v2`, specify `--output-dir 1kg_pca/v2`.

If you provide a `--repository`, you MUST supply a `--commit <SHA>`, e.g.:

```bash
analysis-runner \
    --repository my-approved-repo \
    --commit <commit-sha> \
    --dataset <dataset> \
    --description <description> \
    --access-level <level>
    --output-dir <directory-within-bucket> \
    script_to_run.py with arguments
```

For more examples (including for running an R script and dataproc), see the
[examples](examples) directory.

## Helper for Hail Batch

The analysis-runner package has a number of functions that make it easier to run reproducible analysis through Hail Batch.

This is installed in the analysis runner driver image, ie: you can access the analysis_runner module when running scripts through the analysis-runner.

### Checking out a git repository at the current commit

```python
import hailtop.batch as hb
from analysis_runner.git import (
  prepare_git_job,
  get_repo_name_from_current_directory,
  get_git_commit_ref_of_current_repository,
)

b = hb.Batch('do-some-analysis')
j = b.new_job('checkout_repo')
prepare_git_job(
  job=j,
  # you could specify a name here, like 'analysis-runner'
  repo_name=get_repo_name_from_current_directory(),
  # you could specify the specific commit here, eg: '1be7bb44de6182d834d9bbac6036b841f459a11a'
  commit=get_git_commit_ref_of_current_repository(),
)

# Now, the working directory of j is the checkout out repository
j.command('examples/bash/hello.sh')
```

### Running a dataproc script

```python
import hailtop.batch as hb
from analysis_runner.dataproc import setup_dataproc

b = hb.Batch('do-some-analysis')

# starts up a cluster, and submits a script to the cluster,
# see the definition for more information about how you can configure the cluster
# https://github.com/populationgenomics/analysis-runner/blob/main/analysis_runner/dataproc.py#L80
cluster = dataproc.setup_dataproc(
    b,
    max_age='1h',
    packages=['click', 'selenium'],
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    cluster_name='My Cluster with max-age=1h',
)
cluster.add_job('examples/dataproc/query.py', job_name='example')
```

## Development

You can ignore this section if you just want to run the tool.

To bring up a stack corresponding to a dataset as described in the
[storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies),
see the [stack](stack) directory.

To set up a development environment for the analysis runner using pip, run
the following:

```bash
pip install -r requirements-dev.txt

pre-commit install --install-hooks

pip install --editable .
```

### Deployment

1. Add a Hail Batch service account for all supported datasets.
1. [Copy the Hail tokens](tokens) to the Secret Manager.
1. Deploy the [server](server) by invoking the [`hail_update` workflow](https://github.com/populationgenomics/analysis-runner/blob/main/.github/workflows/hail_update.yaml) manually, specifying the Hail package version.
1. Deploy the [Airtable](airtable) publisher.
1. Publish the [CLI tool and library](analysis_runner) to PyPI.

Note that the [`hail_update` workflow](https://github.com/populationgenomics/analysis-runner/blob/main/.github/workflows/hail_update.yaml) gets invoked whenever a new Hail package is published to PyPI. You can test this manually as follows:

```bash
curl \
  -X POST \
  -H "Authorization: token $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/populationgenomics/analysis-runner/actions/workflows/6364059/dispatches \
  -d '{"ref": "main", "inputs": {"hail_version": "0.2.84"}}'
```

The CLI tool is shipped as a pip package. To build a new version,
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
new package that will be uploaded to PyPI, and become available to install with `pip install`.
