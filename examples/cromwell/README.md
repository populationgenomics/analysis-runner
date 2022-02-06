# Cromwell

It's possible to run workflows on a managed Cromwell instance
through the analysis-runner.

The implementation for the analysis-runner CLI is still in progress,
but you can create a POST request in the short term.

## Hello, world

```shell
pushd examples/cromwell
analysis-runner \
    --dataset fewgenomes \
    --output mfranklin-analysis-runner-test \
    --access-level test \
    --description 'Hello, fewgenomes!' \
    --workflow-input-prefix 'hello.' \
    --imports tools \
    hello.wdl \
    --inp 'Hello, fewgenomes!'
popd
# Translates to:
# curl --location \
#     --request POST 'https://server-a2pko7ameq-ts.a.run.app/cromwell' \
#     --header "Authorization: Bearer $(gcloud auth print-identity-token)" \
#     --header 'Content-Type: application/json' \
#     --data-raw '{
#         "output": "mfranklin-analysis-runner-test",
#         "dataset": "fewgenomes",
#         "repo": "analysis-runner",
#         "accessLevel": "test",
#         "commit": "c86b7816ba9243bd26c20fcc0113dfbd0ccff80a",
#         "inputs_dict": {
#             "hello.inp": "Hello, fewgenomes!"
#         },
#         "input_json_paths": [],
#         "workflow": "hello.wdl",
#         "dependencies": ["tools"],
#         "cwd": "examples/cromwell",
#         "description": "Hello, fewgenomes!"
#     }'
```

## MD5 test

```shell
pushd examples/cromwell
analysis-runner \
    --dataset fewgenomes \
    --output pdiakumis/analysis-runner-test \
    --access-level test \
    --description 'md5sum on a GVCF index'
    --workflow-input-prefix 'md5sum.' \
    --imports tools \
    md5sum_wf.wdl \
    --prefix "sampleB" \
    --inpf "gs://cpg-fewgenomes-test/gvcf/batch0/NA19983.g.vcf.gz.tbi"
popd

# Translates to
# curl --location \
#     --request POST 'https://server-a2pko7ameq-ts.a.run.app/cromwell' \
#     --header "Authorization: Bearer $(gcloud auth print-identity-token)" \
#     --header 'Content-Type: application/json' \
#     --data-raw '{
#         "output": "pdiakumis/analysis-runner-test",
#         "dataset": "fewgenomes",
#         "repo": "analysis-runner",
#         "accessLevel": "test",
#         "commit": "c86b7816ba9243bd26c20fcc0113dfbd0ccff80a",
#         "inputs_dict": {
#             "md5sum.prefix": "sampleB",
#             "md5sum.inpf": "gs://cpg-fewgenomes-test/gvcf/batch0/NA19983.g.vcf.gz.tbi"
#         },
#         "input_json_paths": [],
#         "workflow": "md5sum_wf.wdl",
#         "dependencies": ["tools"],
#         "cwd": "examples/cromwell",
#         "description": "md5sum on a GVCF index"
#     }'
```

## Hail batch -> Cromwell and back

File: `examples/cromwell/cromwell_from_hail_batch.py`

This workflow allows you to run a cromwell workflow from within a hail batch workflow,
and operate on the result back within hail-batch.

Unfortunately you need to know the output format of the cromwell workflow.
It currently only supports:
    - a single value, or
    - a list of values

For example:

```python
run_cromwell_workflow_from_repo_and_get_outputs(
    # ... other inputs
    outputs_to_collect={
        'hello.joined_output': None, # single output
        'hello.outs': 5, # array output of length=5
    }
)
```

To run the test:

```shell
analysis-runner \
  --dataset fewgenomes \
  --access-level test \
  --description "test hail-batch to cromwell support" \
  -o $OUTPUT_DIR \
  python examples/cromwell/cromwell_from_hail_batch.py
```

Relevant code snippet:

```python
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
# {'hello.outs': [__RESOURCE_FILE__2, __RESOURCE_FILE__3], 'hello.joined_out': __RESOURCE_FILE__4}
```
