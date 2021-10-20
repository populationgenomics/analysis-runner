import os
import hailtop.batch as hb
from analysis_runner.cromwell import (
    run_cromwell_workflow,
    watch_workflow_and_get_output,
)
from analysis_runner.git import (
    prepare_git_job,
    get_git_commit_ref_of_current_repository,
)

# DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:dev'
DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver@sha256:bf7353b7104f028df8662d6a27b5e737575e328d9278844694de5439ed2d57d9'

OUTPUT_SUFFIX = 'mfranklin/analysis-runner-test/out/'
BUCKET = 'gs://cpg-fewgenomes-test/' + OUTPUT_SUFFIX

sb = hb.ServiceBackend(billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET'))
b = hb.Batch(backend=sb, default_image=DRIVER_IMAGE)

job = b.new_job('submit_cromwell')

prepare_git_job(
    job,
    repo='analysis-runner',
    commit=get_git_commit_ref_of_current_repository(),
    is_test=True,
)

workflow_id_file = run_cromwell_workflow(
    job=job,
    dataset='fewgenomes',
    access_level='test',
    workflow='hello_all_in_one_file.wdl',
    cwd="examples/cromwell",
    libs=[],
    output_suffix=OUTPUT_SUFFIX,
    input_dict={"hello.inp": "test michael franklin"},
)

outputs_dict = watch_workflow_and_get_output(
    b,
    job_prefix='cromwell-workflow',
    workflow_id_file=workflow_id_file,
    outputs_to_collect={'hello.out': None},
    driver_image=DRIVER_IMAGE,
)

process_j = b.new_job('do-something-with-input')
process_j.command(f'echo ${{$(cat {outputs_dict["hello.out"]})}}')
process_j.command(f'echo ${{$(cat {outputs_dict["hello.out"]})}} > {process_j.out}')

b.write_output(process_j.out, BUCKET)

b.run(wait=False)
