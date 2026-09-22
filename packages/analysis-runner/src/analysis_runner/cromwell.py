# ruff: noqa: F401,ANN002,ANN003

from deprecated import deprecated

# old import that's deprecated, use the cpg_utils one instead
from cpg_utils.cromwell import CromwellOutputType


@deprecated('Use cpg_utils.cromwell.run_cromwell_workflow_from_repo_and_get_outputs')
def run_cromwell_workflow_from_repo_and_get_outputs(*args, **kwargs):
    from cpg_utils.cromwell import run_cromwell_workflow_from_repo_and_get_outputs as f

    return f(*args, **kwargs)
