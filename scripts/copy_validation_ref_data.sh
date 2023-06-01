#!/usr/bin/env bash

# copies the reference data used for validation to cpg-common-main

# SDF files for ref genome indexing
gcloud storage cp --recursive gs://cpg-validation-test/refgenome_sdf \
    gs://cpg-common-main/references/validation/masked_reference_sdf

gcloud storage cp gs://cpg-validation-test/HG001/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz \
    gs://cpg-validation-test/HG001/HG001_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi \
    gs://cpg-validation-test/HG001/HG001_GRCh38_1_22_v4.2.1_benchmark.bed \
    gs://cpg-common-main/references/validation/HG001_NA12878

gcloud storage cp gs://cpg-validation-test/syndip/syndip_truth.vcf.gz \
    gs://cpg-validation-test/syndip/syndip_truth.vcf.gz.tbi \
    gs://cpg-common-main/references/validation/syndip/syndip.b38_20180222.bed \
    gs://cpg-common-main/references/validation/SYNDIP

gcloud storage cp gs://cpg-validation-test/HG002/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz \
    gs://cpg-validation-test/HG002/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi \
    gs://cpg-validation-test/HG002/HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed \
    gs://cpg-common-main/references/validation/HG002_NA24385

gcloud storage cp gs://cpg-validation-test/HG003/HG003_GRCh38_1_22.vcf.gz \
    gs://cpg-validation-test/HG003/HG003_GRCh38_1_22.vcf.gz.tbi \
    gs://cpg-validation-test/HG003/HG003_GRCh38_1_22.bed \
    gs://cpg-common-main/references/validation/HG003_NA24149

gcloud storage cp gs://cpg-validation-test/HG004/HG004_GRCh38_1_22.vcf.gz \
    gs://cpg-validation-test/HG004/HG004_GRCh38_1_22.vcf.gz.tbi \
    gs://cpg-validation-test/HG004/HG004_GRCh38_1_22.bed \
    gs://cpg-common-main/references/validation/HG004_NA24143

# VCGS exome sample
gcloud storage cp gs://cpg-validation-test/HG001/twist_exome_benchmark_truth.vcf.gz \
    gs://cpg-validation-test/HG001/twist_exome_benchmark_truth.vcf.gz.tbi \
    gs://cpg-validation-test/Twist_Exome_Core_Covered_Targets_hg38.bed \
    gs://cpg-common-main/references/validation/VCGS_NA12878

# folder of stratification files
gcloud storage cp --recursive gs://cpg-validation-test/GRCh38_regions \
    gs://cpg-common-main/references/validation/stratification
