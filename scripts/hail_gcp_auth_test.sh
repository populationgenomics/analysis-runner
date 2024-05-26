#!/usr/bin/env bash
gcloud auth list

echo $GCE_METADATA_IP
echo $GCE_METADATA_ROOT

curl \
    -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"
