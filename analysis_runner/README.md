# Analysis Runner CLI and library

## CLI

See [these instructions](https://github.com/populationgenomics/analysis-runner#cli) for how to run the CLI.

## Library

This package also contains convenience modules for writing scripts that will be
executed through the analysis-runner.

### [Dataproc](dataproc.py)

Provides a wrapper for starting a Dataproc cluster from within Hail Batch and
submitting a Query script to it ([example](../examples/dataproc)). This is
particularly useful as an intermediate solution before all Hail Query features
are supported by the `ServiceBackend`.
