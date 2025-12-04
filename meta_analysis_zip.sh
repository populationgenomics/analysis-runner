#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/master_zip/meta_analysis.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp -r "${BASE_PREFIX}/*.zip" .

echo "Creating meta_analysis.zip from:"
ls *.zip

zip -r meta_analysis.zip *.zip

echo "Uploading meta_analysis.zip to $OUTPUT_GCS ..."
gsutil cp meta_analysis.zip "$OUTPUT_GCS"

echo "Done."