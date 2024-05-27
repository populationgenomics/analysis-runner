#!/usr/bin/env bash
set -x
gcloud auth list

# echo $GCE_METADATA_IP
# echo $GCE_METADATA_ROOT

# export GCE_METADATA_IP=169.254.169.254
# export GCE_METADATA_ROOT=169.254.169.254

gsutil ls -lh gs://cpg-fewgenomes-test/
gcloud storage ls gs://cpg-fewgenomes-test/

python3 -c '
from google.cloud import storage;
client = storage.Client();
bucket = client.get_bucket("cpg-fewgenomes-test");
print([a.name for a in bucket.list_blobs(prefix="", delimiter="/")])
'