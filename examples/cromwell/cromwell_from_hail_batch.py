"""
Test script, to demonstrate how you can run a cromwell workflow
from within a batch environment, and operate on the result(s)
"""
import os
import hailtop.batch as hb
from analysis_runner.cromwell import (
    run_cromwell_workflow_from_repo_and_get_outputs,
    CromwellOutputType,
)

OUTPUT_SUFFIX = 'mfranklin/analysis-runner-test/out/'
DATASET = os.getenv('DATASET')
BUCKET = os.getenv('HAIL_BUCKET')
OUTPUT_PATH = os.path.join(f'gs://{BUCKET}', OUTPUT_SUFFIX)
BILLING_PROJECT = os.getenv('HAIL_BILLING_PROJECT')
ACCESS_LEVEL = os.getenv('ACCESS_LEVEL')

DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver@sha256:4370e54695c8c1ae22bcb968c0ffb99c2be558b26fe3d8a2da3ff21af425a178'

sb = hb.ServiceBackend(billing_project=BILLING_PROJECT, bucket=BUCKET)
b = hb.Batch(backend=sb, default_image=os.getenv('DRIVER_IMAGE'))

inputs = ['Hello, analysis-runner ;)', 'Hello, second output!']


workflow_outputs = run_cromwell_workflow_from_repo_and_get_outputs(
    b=b,
    job_prefix='hello',
    workflow='hello_all_in_one_file.wdl',
    cwd='examples/cromwell',
    input_dict={'hello.inps': inputs},
    outputs_to_collect={
        'hello.outs': CromwellOutputType.array(len(inputs)),
        'hello.joined_out': CromwellOutputType.single(),
        'hello.texts': CromwellOutputType.array_resource_group(
            len(inputs),
            resource_group={'txt': 'hello.out_txts', 'md5': 'hello.out_txt_md5s'},
        ),
    },
    libs=[],  # hello_all_in_one_file is self-contained, so no dependencies
    output_suffix=OUTPUT_SUFFIX,
    dataset=DATASET,
    access_level=ACCESS_LEVEL,
    driver_image=DRIVER_IMAGE,
)
print(workflow_outputs)
# {
#   'hello.outs': [__RESOURCE_FILE__2, __RESOURCE_FILE__3],
#   'hello.joined_out': __RESOURCE_FILE__4,
#   'hello.texts': [
#       <hailtop.batch.resource.ResourceGroup object at 0x7ffed2d56dd0>,
#       <hailtop.batch.resource.ResourceGroup object at 0x7ffed2d56590>
#   ]
# }

process_j = b.new_job('do-something-with-string-output')
process_j.command(
    f"cat {workflow_outputs['hello.joined_out']} | awk '{{print toupper($0)}}'"
)

for idx, out in enumerate(workflow_outputs['hello.texts']):

    process_j = b.new_job(f'do-something-with-input-{idx+1}')
    process_j.command(
        f"""
# md5
echo '{out}'
cat {out.md5} | awk '{{print toupper($0)}}'
cat {out.txt} | awk '{{print toupper($0)}}' > {process_j.out}
    """
    )
    b.write_output(process_j.out, OUTPUT_PATH + f'file-{idx+1}.txt')

b.run(dry_run=True)
