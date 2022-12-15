#!/usr/bin/env bash

# this should be run with high-core low memory resources
# e.g.
# analysis-runner analysis-runner
#  --dataset reference \
#  --description "Copy Clinvar Broad -> CPG" \
#  -o "clinvar_copy" \
#  --access-level standard \
#  --cpu 16
#  --memory lowmem
#  scripts/copy_clinvar.sh
#  2022-09-17

set -ex

if [ -z "$1" ]
  then
    echo "No date argument supplied"
    exit1
fi

DATE=${1}
CPG_ANNO=${2:-"gs://cpg-common-main/references/seqr/"}

gcloud alpha storage cp -r "gs://seqr-reference-data/GRCh38/clinvar/clinvar.GRCh38.${DATE}.ht" "${CPG_ANNO}"
