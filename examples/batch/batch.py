#!/usr/bin/env python

"""
Example of running Batch script with analysis-runner.
"""
import os
import hailtop.batch as hb
import click

DATASET = os.getenv('DATASET')
BUCKET = os.getenv('HAIL_BUCKET')
BILLING_PROJECT = os.getenv('HAIL_BILLING_PROJECT')
ACCESS_LEVEL = os.getenv('ACCESS_LEVEL')


@click.command()
@click.option('--cram', 'cram_path', 'Input CRAM or BAM file')
@click.option('--output', 'output_path', 'Output path to write the result')
def main(
    cram_path: str, output_path: str
):  # pylint: disable=missing-function-docstring
    # Initializing Batch
    backend = hb.ServiceBackend(billing_project=BILLING_PROJECT, bucket=BUCKET)
    b = hb.Batch(backend=backend, default_image=os.getenv('DRIVER_IMAGE'))

    j = b.new_job('Subset CRAM')

    # Making sure Hail Batch would localize both CRAM and the correponding CRAI index
    cram = b.read_input_group(**{'cram': cram_path, 'cram.crai': cram_path + '.crai'})

    # Working with CRAM files requires the refernece fasta
    ref_fasta = 'gs://cpg-reference/hg38/v1/Homo_sapiens_assembly38.fasta'
    ref = b.read_input_group(
        **dict(
            base=ref_fasta,
            fai=ref_fasta + '.fai',
            dict=ref_fasta.replace('.fasta', '').replace('.fna', '').replace('.fa', '')
            + '.dict',
        )
    )

    j.command(
        f"""
    samtools view {cram} -T {ref} -L chr21:1-10000 -o -Ocram {j.output_bam}
    """
    )

    # Writing the result
    b.write_output(j.output_bam, output_path)
    b.run()


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
