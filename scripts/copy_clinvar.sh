#!/usr/bin/env bash

# this should be run with high-core low memory resources
# e.g.
# analysis-runner analysis-runner --cpu 8 --memory 2Gi scripts/copy_clinvar.sh 2022-09-17

set -ex

DATE=${1}
CPG_ANNO=${2:-"gs://cpg-reference/seqr/v0-1/"}

gcloud storage cp -r "gs://seqr-reference-data/GRCh38/clinvar/clinvar.GRCh38.${DATE}.ht" "${CPG_ANNO}"