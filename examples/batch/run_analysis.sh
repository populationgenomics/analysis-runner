#!/bin/bash

analysis-runner \
    --dataset thousand-genomes \
    --description 'Test script for batch on Azure' \
    --output-dir test \
    --cloud azure \
    --access-level test \
    --config examples/batch/hail_batch_job.toml \
    --image cpg_workflows:latest \
    examples/batch/test_cpg_infra.py \
    test
