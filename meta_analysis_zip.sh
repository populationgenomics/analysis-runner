#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/master_zip/meta_ASDC_B_intermediate_B_memory_B_naive_CD14_Mono.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp "${BASE_PREFIX}/ASDC.zip" . 
gsutil -m cp "${BASE_PREFIX}/B_intermediate.zip" . 
gsutil -m cp "${BASE_PREFIX}/B_memory.zip" . 
gsutil -m cp "${BASE_PREFIX}/B_naive.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD14_Mono.zip" . 


echo "Creating meta_analysis.zip from:"
ls *.zip

zip -r meta_ASDC_B_intermediate_B_memory_B_naive_CD14_Mono.zip *.zip

echo "Uploading meta_analysis.zip to $OUTPUT_GCS ..."
gsutil cp meta_ASDC_B_intermediate_B_memory_B_naive_CD14_Mono.zip "$OUTPUT_GCS"

echo "Done."