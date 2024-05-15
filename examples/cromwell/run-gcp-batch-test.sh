#!/usr/bin/env bash
pip3 install --no-deps --force git+https://github.com/populationgenomics/cpg-utils.git@update-workflow-options-for-cromwell-gcs-batch

python3 cromwell_from_hail_batch.py
