#!/bin/bash
set -euo pipefail

BASE_PREFIX="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/zips"
OUTPUT_GCS="gs://cpg-tenk10k-test-analysis/str/associatr/final_freeze/tob_n950_and_bioheart_n975/trs_snps/master_zip/meta_CD4_TEM_CD8_Naive_CD8_Proliferating_CD8_TCM_CD8_TEM.zip"

# Work in a temp dir
TMPDIR=$(mktemp -d)
cd "$TMPDIR"

echo "Copying all cell-type zips from $BASE_PREFIX ..."
gsutil -m cp "${BASE_PREFIX}/CD4_TEM.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD8_Naive.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD8_Proliferating.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD8_TCM.zip" . 
gsutil -m cp "${BASE_PREFIX}/CD8_TEM.zip" . 


echo "Creating meta_analysis.zip from:"
ls *.zip

zip -r meta_CD4_TEM_CD8_Naive_CD8_Proliferating_CD8_TCM_CD8_TEM.zip *.zip

echo "Uploading meta_analysis.zip to $OUTPUT_GCS ..."
gsutil cp meta_CD4_TEM_CD8_Naive_CD8_Proliferating_CD8_TCM_CD8_TEM.zip "$OUTPUT_GCS"

echo "Done."