#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/bioheart_n975/trs/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/bioheart_n975/trs/master_zip/bioheart.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp "${BASE_PREFIX}/*.zip" .

echo "Creating bioheart.zip from:"
ls *.zip

zip -r bioheart.zip *.zip

echo "Uploading bioheart.zip to $OUTPUT_GCS ..."
gsutil cp bioheart.zip "$OUTPUT_GCS"

echo "Done."