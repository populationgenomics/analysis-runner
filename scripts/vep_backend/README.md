# Run VEP in parallel using batch backend

This runs a Hail query script in Dataproc using Hail Batch in order to run VEP on a hail matrix table. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner \
    --dataset <dataset> \
    --description <description>  \
    --output-dir <directory-to-output-annotated-vcf> \
    --access-level level> vep_batch_backend.py \
    --vep-version <vep-version> \
    --vcf-path <path-to-vcf> \
    --output-ht <output-of-annoatted-vcf> \
    --scatter-count <number-of-fragments> \
```

As an example, the following invocation would run VEP annotation for a VCF file in the `test` bucket for the `tx-adapt` dataset.

```sh
analysis-runner \
    --dataset tx-adapt \
    --description "run VEP using batch backend"  \
    --output-dir "vep_batch/v0" \
    --access-level test vep_batch_backend.py \
    --vep-version "105" \
    --vcf-path "vep/indels.vcf.bgz" \
    --output-ht "vep/batch/annotated_indels.ht" \
    --scatter-count 2 \
```
