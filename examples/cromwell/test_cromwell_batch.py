"""
Test script, to demonstrate how you can run a cromwell workflow
from within a batch environment, and operate on the result(s)
"""
import os
import hailtop.batch as hb
from analysis_runner.cromwell import (
    run_cromwell_workflow_from_repo_and_get_outputs,
)

# DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:dev'
DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver@sha256:bf7353b7104f028df8662d6a27b5e737575e328d9278844694de5439ed2d57d9'

OUTPUT_SUFFIX = 'mfranklin/analysis-runner-test/out/'
BUCKET = 'gs://cpg-fewgenomes-test/' + OUTPUT_SUFFIX

sb = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)
b = hb.Batch(backend=sb, default_image=DRIVER_IMAGE)

inputs = ['Hello, analysis-runner ;)', 'Hello, second output!']

workflow_id_file = run_cromwell_workflow_from_repo_and_get_outputs(
    b=b,
    job_prefix='hello',
    dataset='fewgenomes',
    access_level='test',
    workflow='hello_all_in_one_file.wdl',
    cwd='examples/cromwell',
    libs=[],
    output_suffix=OUTPUT_SUFFIX,
    input_dict={'hello.inps': inputs},
    outputs_to_collect={'hello.out': len(inputs)},
    driver_image=DRIVER_IMAGE,
)

for idx, out in enumerate(workflow_id_file['hello.out']):

    process_j = b.new_job(f'do-something-with-input-{idx+1}')
    process_j.command(
        f"""
# print uppercase value
cat {out} | awk '{{print toupper($0)}}'
cat {out} | awk '{{print toupper($0)}}' > {process_j.out}
    """
    )
    b.write_output(process_j.out, BUCKET + f'file-{idx+1}.txt')

b.run(wait=False)
