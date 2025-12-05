#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/master_zip/meta_CD16_Mono_CD4_CTL_CD4_Naive_CD4_Proliferating_CD4_TCM.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp "${BASE_PREFIX}/CD16_Mono.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD4_CTL.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD4_Naive.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD4_Proliferating.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD4_TCM.zip" . 


echo "Creating meta_analysis.zip from:"
ls *.zip

zip -r meta_CD16_Mono_CD4_CTL_CD4_Naive_CD4_Proliferating_CD4_TCM.zip *.zip

echo "Uploading meta_analysis.zip to $OUTPUT_GCS ..."
gsutil cp meta_CD16_Mono_CD4_CTL_CD4_Naive_CD4_Proliferating_CD4_TCM.zip "$OUTPUT_GCS"

echo "Done."