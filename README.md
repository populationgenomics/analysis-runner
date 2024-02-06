# Analysis runner

This tool helps to [improve analysis provenance](https://github.com/populationgenomics/team-docs/blob/main/reproducible_analyses.md) and theoretical reproducibility by automating the following aspects:

- Allow quick iteration using an environment that resembles production.
- Only allow access to production datasets through code that has been reviewed.
- Link the output data with the exact program invocation of how the data has been generated.

One of our main workflow pipeline systems at the CPG is [Hail Batch](https://hail.is/docs/batch/getting_started.html). By default, its pipelines are defined by running a Python program _locally_ and submitting the resulting DAG to the Hail Batch server. By specifying a repo, commit, and file, this tool will run your script inside a "driver" image on Hail Batch, with the correct permissions.

All invocations are logged to metamist, in the [analysis-runner page](https://sample-metadata.populationgenomics.org.au/analysis-runner/).

When using the analysis-runner, the jobs are run as a specific Hail Batch service account to give appropriate permissions based on the dataset, and access level ("test", "standard", or "full", as documented in the team docs [storage policies](https://github.com/populationgenomics/team-docs/tree/main/storage_policies#analysis-runner)). This helps with bucket permission management and billing budgets.

By default, we run your script in a driver image, that contains a number of common tools - but you can in fact run any container inside the cpg-common artifact registry (and any container if running using the test access level).

The analysis-runner is also integrated with our Cromwell server to run WDL based workflows.

## CLI

The analysis-runner CLI is used to start pipelines based on a GitHub repository, commit, and command to run.

First, make sure that your environment provides Python 3.10 or newer. We recommend using `pyenv` to manage your python versions

```sh
pyenv install 3.10.12
pyenv global 3.10.12
> python3 --version
Python 3.10.12
```

Then install the `analysis-runner` Python package using `pip`:

```bash
python3 -m pip install analysis-runner
```

Run `analysis-runner --help` to see usage information.

Make sure that you're logged into GCP with _application-default_ credentials:

```bash
gcloud auth application-default login
```

If you're in the directory of the project you want to run, you can omit the `--commit` and `--repository` parameters, which will use your current git remote and commit HEAD.

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

### GitHub Authentication

If you are submitting an analysis-runner job that needs to clone a private repository owned by populationgenomics on GitHub (eg submitting a script to analysis-runner from a private repository), the analysis-runner should insert the following items into your `config.toml`:

```toml
[infrastructure]
git_credentials_secret_name = '<ask_software_team_for_secret_name>'
git_credentials_secret_project = '<ask_software_team_for_secret_project>'
```

If you are specifying multiple configuration files, please don't accidentally override these values.

## Custom Docker images

The default driver image that's used to run scripts comes with Hail and some statistics libraries preinstalled (see the corresponding [Hail Dockerfile](driver/Dockerfile.hail)). It's possible to use any custom Docker image instead, using the `--image` parameter. Note that any such image needs to contain the critical dependencies as specified in the [`base` image](driver/Dockerfile.base).

For R scripts, we add the R-tidyverse set of packages to the base image, see the corresponding [R Dockerfile](driver/Dockerfile.r) and the [R example](examples/r) for more details.

## Helper for Hail Batch

The analysis-runner package has a number of functions that make it easier to run reproducible analysis through Hail Batch.

This is installed in the analysis runner driver image, ie: you can access the analysis_runner module when running scripts through the analysis-runner.

### Checking out a git repository at the current commit

```python
from cpg_utils.hail_batch import get_batch
from analysis_runner.git import (
  prepare_git_job,
  get_repo_name_from_current_directory,
  get_git_commit_ref_of_current_repository,
)

b = get_batch(name='do-some-analysis')
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
from cpg_utils.hail_batch import get_batch
from analysis_runner.dataproc import setup_dataproc

b = get_batch(name='do-some-analysis')

# starts up a cluster, and submits a script to the cluster,
# see the definition for more information about how you can configure the cluster
# https://github.com/populationgenomics/analysis-runner/blob/main/analysis_runner/dataproc.py#L80
cluster = dataproc.setup_dataproc(
    b,
    max_age='1h',
    packages=['click', 'selenium'],
    cluster_name='My Cluster with max-age=1h',
)
cluster.add_job('examples/dataproc/query.py', job_name='example')
```

## Development

You can ignore this section if you just want to run the tool.

To set up a development environment for the analysis runner using pip, run
the following:

```bash
pip install -r requirements-dev.txt
pip install --editable .
```

### Deployment

The server can be deployed by manually running the [`deploy_server.yaml`](https://github.com/populationgenomics/analysis-runner/actions/workflows/deploy_server.yaml) GitHub action. This will also deploy the driver image.

The CLI tool is shipped as a pip package, this happens automatically on pushes to `main.py`. To build a new version, you should add a [bump2version](https://pypi.org/project/bump2version/) commit to your branch.
