#!/usr/bin/env python

"""
Example of running Batch script with analysis-runner.
"""
import os
import hailtop.batch as hb
import click

DATASET = os.getenv('DATASET')
BUCKET = os.getenv('HAIL_BUCKET')
OUTPUT_PREFIX = os.getenv('OUTPUT')
BILLING_PROJECT = os.getenv('HAIL_BILLING_PROJECT')
ACCESS_LEVEL = os.getenv('ACCESS_LEVEL')

REF_FASTA = 'gs://cpg-reference/hg38/v1/Homo_sapiens_assembly38.fasta'


@click.command()
@click.argument('cram_path')
@click.argument('region')
def main(cram_path: str, region: str):  # pylint: disable=missing-function-docstring
    """
    Subset CRAM or BAM file CRAM_PATH to REGION. Example: batch.py sample.cram chr21:1-10000
    """
    # Initializing Batch
    backend = hb.ServiceBackend(billing_project=BILLING_PROJECT, bucket=BUCKET)
    b = hb.Batch(backend=backend, default_image=os.getenv('DRIVER_IMAGE'))

    # Adding a job and giving it a descriptive name.
    j = b.new_job('Subset CRAM')

    # Making sure Hail Batch would localize both CRAM and the correponding CRAI index
    cram = b.read_input_group(**{'cram': cram_path, 'cram.crai': cram_path + '.crai'})

    # Working with CRAM files requires the reference fasta
    ref = b.read_input_group(
        **dict(
            base=REF_FASTA,
            fai=REF_FASTA + '.fai',
            dict=REF_FASTA.replace('.fasta', '').replace('.fna', '').replace('.fa', '')
            + '.dict',
        )
    )

    # This image contains basic bioinformatics tools like samtools, bcftools, Picard, etc.
    j.image('australia-southeast1-docker.pkg.dev/cpg-common/images/bioinformatics:v1-1')

    # For larger CRAMs, request more storage.
    j.storage('10G')

    # If you want to run a multithreaded command, e.g. samtools with -@, request more CPUs here.
    # Note that the machines use hyperthreading, so for every CPU, 2x threads are available.
    j.cpu(2)

    # The command that do the actual job.
    j.command(
        f"""
    samtools view {cram} -T {ref} -L {region} -o -Ocram {j.output_bam}
    """
    )

    # Speciying where to write the result
    out_fname = os.path.splitext(os.path.basename(cram_path)[0]) + '-split.cram'
    output_path = os.path.join(OUTPUT_PREFIX, out_fname)
    b.write_output(j.output_bam, output_path)

    # don't wait for the hail batch workflow to complete, otherwise
    # the workflow might get resubmitted if this VM gets preempted.
    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
