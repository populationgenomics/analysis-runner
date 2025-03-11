#!/bin/bash -e
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
#
# The -p PREFIX option can be used to abbreviate similar file names
# so that
#
#    scripts/zip_gcs_files.sh gs://BUCKET/ZIPPATH -p gs://BUCKET/ A B C
#
# processes gs://BUCKET/A, gs://BUCKET/B, and gs://BUCKET/C.

zip_path="$1"
shift

prefix=
while getopts p: opt
do
    case $opt in
    p)  prefix="$OPTARG" ;;
    ?)  echo "Usage: zip_gcs_files.sh OUTZIP [-p PREFIX] FILE..." >&2
        exit 1
        ;;
    esac
done
shift $((OPTIND - 1))

set -x

cd "$BATCH_TMPDIR"

zip_local=$(basename "$zip_path")

for file_path in "$@"
do
    file_local=$(basename "$prefix$file_path")
    gcloud storage cp -P "$prefix$file_path" "$file_local"

    zip -9 "$zip_local" "$file_local"
    rm "$file_local"
done

gcloud storage cp "$zip_local" "$zip_path"

ls -lh "$zip_local"
md5sum "$zip_local"
zipinfo "$zip_local"
