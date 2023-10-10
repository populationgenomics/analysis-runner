#!/usr/bin/env bash

gsutil -m cp -r dir "gs://cpg-tob-wgs-test/scrna-seq/grch38_association_files/gene_location_files/gencode.v42.annotation.gff3.gz" "gs://cpg-tob-wgs-main/tob_wgs_genetics/gencode.v42.annotation.gff3.gz"
