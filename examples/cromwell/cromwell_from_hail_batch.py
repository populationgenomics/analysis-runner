"""
Test script, to demonstrate how you can run a cromwell workflow
from within a batch environment, and operate on the result(s)
"""
import hailtop.batch as hb
from cpg_utils.config import get_config
from cpg_utils.hail_batch import output_path, remote_tmpdir
from analysis_runner.cromwell import (
    run_cromwell_workflow_from_repo_and_get_outputs,
    CromwellOutputType,
)

OUTPUT_PREFIX = 'mfranklin/analysis-runner-test/out/'
OUTPUT_PATH = output_path(OUTPUT_PREFIX)

_config = get_config()
BILLING_PROJECT = _config['hail']['billing_project']
DATASET = _config['workflow']['dataset']
ACCESS_LEVEL = _config['workflow']['access_level']

sb = hb.ServiceBackend(billing_project=BILLING_PROJECT, remote_tmpdir=remote_tmpdir())
b = hb.Batch(backend=sb, default_image=_config['workflow']['driver_image'])

inputs = ['Hello, analysis-runner ;)', 'Hello, second output!']


submit_j, workflow_outputs = run_cromwell_workflow_from_repo_and_get_outputs(
    b=b,
    job_prefix='hello',
    workflow='hello_all_in_one_file.wdl',
    cwd='examples/cromwell',
    input_dict={'hello.inps': inputs},
    outputs_to_collect={
        'joined_out': CromwellOutputType.single('hello.joined_out'),
        'outs': CromwellOutputType.array('hello.outs', len(inputs)),
        'out_paths': CromwellOutputType.array_path('hello.outs', len(inputs)),
        'texts': CromwellOutputType.array_resource_group(
            'hello.texts',
            len(inputs),
            resource_group={'txt': 'hello.out_txts', 'md5': 'hello.out_txt_md5s'},
        ),
    },
    libs=[],  # hello_all_in_one_file is self-contained, so no dependencies
    output_prefix=OUTPUT_PREFIX,
    dataset=DATASET,
    access_level=ACCESS_LEVEL,
)
print(workflow_outputs)
# {
#   'joined_out': __RESOURCE_FILE__2,
#   'outs': [__RESOURCE_FILE__3, __RESOURCE_FILE__4],
#   'out_paths': [__RESOURCE_FILE__5, __RESOURCE_FILE__6],
#   'texts': [
#       <hailtop.batch.resource.ResourceGroup object at 0x7f4de22e0ed0>,
#       <hailtop.batch.resource.ResourceGroup object at 0x7f4de22e0b90>
#   ]
# }

process_j = b.new_job('do-something-with-string-output')
process_j.command(f"cat {workflow_outputs['joined_out']} | awk '{{print toupper($0)}}'")


# Use python job to process file paths
def process_paths_python(*files):
    """Collect a list of output files, and log to console"""
    inner_paths = []
    for file in files:
        with open(file, encoding='utf-8') as f:
            inner_paths.append(f.read().strip())
    # maybe update sample_metadata server?
    print('Processed paths: ' + ', '.join(inner_paths))


process_paths_job = b.new_python_job('process_paths')
process_paths_job.call(process_paths_python, *workflow_outputs['out_paths'])

# Here, we're showing that you can use the output of a
# resource group that we defined earlier in different tasks.
for idx, out in enumerate(workflow_outputs['texts']):
    process_j = b.new_job(f'do-something-with-input-{idx+1}')

    # For example:
    #   convert the .md5 file to uppercase and print it to the console
    #   convert the .txt file to uppercase and write it as an output
    process_j.command(
        f"""\
cat {out.md5} | awk '{{print toupper($0)}}'
cat {out.txt} | awk '{{print toupper($0)}}' > {process_j.out}"""
    )
    b.write_output(process_j.out, OUTPUT_PATH + f'file-{idx+1}.txt')

b.run(wait=False)
