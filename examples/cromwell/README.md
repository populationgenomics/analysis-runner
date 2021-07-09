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
    --description 'Hello, fewgenomes!'
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
