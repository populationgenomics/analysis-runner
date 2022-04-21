import os
import hailtop.batch as hb

from analysis_runner.cromwell import (
    run_cromwell_workflow_from_repo_and_get_outputs,
    CromwellOutputType,
)

OUTPUT_PREFIX = 'vb-test/analysis-runner-test/out/'
BILLING_PROJECT = os.getenv('HAIL_BILLING_PROJECT')
DATASET = os.getenv('CPG_DATASET')
ACCESS_LEVEL = os.getenv('CPG_ACCESS_LEVEL')

inputs = ['Hello, analysis-runner', 'Hello, second output!']

backend = hb.ServiceBackend(
    billing_project="vivianbakiris-trial", remote_tmpdir="gs://vivian-dev-cromwell"
)
b = hb.Batch(backend=backend, name="test")


workflow_outputs = run_cromwell_workflow_from_repo_and_get_outputs(
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
b.run()