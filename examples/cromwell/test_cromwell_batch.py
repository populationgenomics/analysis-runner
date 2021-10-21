"""
Test script, to demonstrate how you can run a cromwell workflow
from within a batch environment, and operate on the result(s)
"""
import os
import hailtop.batch as hb
from analysis_runner.cromwell import (
    run_cromwell_workflow_from_repo_and_get_outputs,
)

OUTPUT_SUFFIX = 'mfranklin/analysis-runner-test/out/'
BUCKET = os.getenv('BUCKET')
OUTPUT_PATH = os.path.join(f'gs://{BUCKET}', OUTPUT_SUFFIX)

sb = hb.ServiceBackend(billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=BUCKET)
b = hb.Batch(backend=sb, default_image=os.getenv('DRIVER_IMAGE'))

inputs = ['Hello, analysis-runner ;)', 'Hello, second output!']

workflow_outputs = run_cromwell_workflow_from_repo_and_get_outputs(
    b=b,
    job_prefix='hello',
    dataset='fewgenomes',
    access_level='test',
    workflow='hello_all_in_one_file.wdl',
    cwd='examples/cromwell',
    libs=[],  # hello_all_in_one_file is self-contained, so no dependencies
    output_suffix=OUTPUT_SUFFIX,
    input_dict={'hello.inps': inputs},
    outputs_to_collect={'hello.outs': len(inputs), 'hello.joined_out': None},
)

process_j = b.new_job('do-something-with-string-output')
process_j.command(
    f"cat {workflow_outputs['hello.joined_out']} | awk '{{print toupper($0)}}'"
)

for idx, out in enumerate(workflow_outputs['hello.outs']):

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
