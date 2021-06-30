# Cromwell

It's possible to run workflows on a managed Cromwell instance
through the analysis-runner.

The implementation for the analysis-runner CLI is still in progress,
but you can create a POST request in the short term, for example:

```shell
curl --location \
    --request POST 'https://server-a2pko7ameq-ts.a.run.app/cromwell' \
    --header "Authorization: Bearer $(gcloud auth print-identity-token)" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "output": "mfranklin-analysis-runner-test",
        "dataset": "fewgenomes",
        "repo": "analysis-runner",
        "accessLevel": "test",
        "commit": "74f91a3a246fbba261f1c2a3e06bc847c77b89cf",
        "inputs_dict": {
            "hello.inp": "Hello, fewgenomes!"
        },
        "input_json_paths": [],
        "workflow": "hello.wdl",
        "dependencies": ["tools"],
        "cwd": "examples/cromwell",
        "description": "Hello, fewgenomes!"
    }'
```
