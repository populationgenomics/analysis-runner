#!/usr/bin/env bash
set +x
gcloud auth list

echo $GCE_METADATA_IP
echo $GCE_METADATA_ROOT

export GCE_METADATA_IP=169.254.169.254
export GCE_METADATA_ROOT=169.254.169.254

gsutil ls -lh gs://cpg-fewgenomes-test/

curl \
    -H "Metadata-Flavor: Google" \
    "http://$GCE_METADATA_ROOT/computeMetadata/v1/instance/service-accounts"
