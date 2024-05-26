#!/usr/bin/env bash
set -x
gcloud auth list

# echo $GCE_METADATA_IP
# echo $GCE_METADATA_ROOT

# export GCE_METADATA_IP=169.254.169.254
# export GCE_METADATA_ROOT=169.254.169.254

gcloud storage ls -lh gs://cpg-fewgenomes-test/

