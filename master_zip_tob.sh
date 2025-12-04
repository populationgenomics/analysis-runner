#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950/trs/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950/trs/master_zip/tob.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp "${BASE_PREFIX}/*.zip" .

echo "Creating tob.zip from:"
ls *.zip

zip -r tob.zip *.zip

echo "Uploading tob.zip to $OUTPUT_GCS ..."
gsutil cp tob.zip "$OUTPUT_GCS"

echo "Done."