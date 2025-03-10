#!/bin/bash -ex
#
# Zips up the specified GCS files into a new GCS zip archive.
#
# Typical usage:
#
# analysis-runner --dataset DATASET --description 'Zip some files' \
#    --access-level standard --output-dir unused --storage 10G \
#    scripts/zip_gcs_files.sh gs://BUCKET/ZIPPATH gs://BUCKET/FILE...
#
# This will create gs://BUCKET/ZIPPATH containing the other specified
# files. The script will need storage space for the final zip archive
# and one localised input file at a time.

zip_path="$1"
shift

cd "$BATCH_TMPDIR"

zip_local=$(basename "$zip_path")

for file_path in "$@"
do
    file_local=$(basename "$file_path")
    gcloud storage cp -P "$file_path" "$file_local"

    zip -9 "$zip_local" "$file_local"
    rm "$file_local"
done

gcloud storage cp "$zip_local" "$zip_path"

md5sum "$zip_local"
zipinfo "$zip_local"
