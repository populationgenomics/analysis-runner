#!/usr/bin/env python

"""
Example of running Batch script with analysis-runner.
"""
import os
import hailtop.batch as hb
import click
from cpg_utils.hail import output_path, remote_tmpdir

BILLING_PROJECT = os.getenv('HAIL_BILLING_PROJECT')
assert BILLING_PROJECT

REF_FASTA = 'gs://cpg-reference/hg38/v1/Homo_sapiens_assembly38.fasta'
SAMTOOLS_IMAGE = 'australia-southeast1-docker.pkg.dev/cpg-common/images/samtools:v0'


@click.command()
@click.argument('cram_path')
@click.argument('region')
def main(cram_path: str, region: str):  # pylint: disable=missing-function-docstring
    """
    Subset CRAM or BAM file CRAM_PATH to REGION. Example: batch.py sample.cram chr21:1-10000
    """
    # Initializing Batch
    backend = hb.ServiceBackend(
        billing_project=BILLING_PROJECT, remote_tmpdir=remote_tmpdir()
    )
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
    j.image(SAMTOOLS_IMAGE)

    # For larger CRAMs, request more storage.
    j.storage('10G')

    # If you want to run a multithreaded command, e.g. samtools with -@, request more CPUs here.
    # Note that the machines use hyperthreading, so for every CPU, 2x threads are available.
    j.cpu(2)

    # The command that do the actual job.
    j.command(
        f"""
    samtools view -T {ref.base} {cram['cram']} {region} -Ocram -o {j.output_bam}
    """
    )

    # Speciying where to write the result
    out_fname = os.path.splitext(os.path.basename(cram_path))[0] + '-split.cram'
    b.write_output(j.output_bam, output_path(out_fname))

    # don't wait for the hail batch workflow to complete, otherwise
    # the workflow might get resubmitted if this VM gets preempted.
    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
