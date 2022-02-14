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
def main(cram_path):  # pylint: disable=missing-function-docstring
    backend = hb.ServiceBackend(billing_project=BILLING_PROJECT, bucket=BUCKET)
    b = hb.Batch(backend=backend, default_image=os.getenv('DRIVER_IMAGE'))

    j = b.new_job('Subset CRAM')

    cram = b.read_input_group(**{'cram': cram_path, 'cram.crai': cram_path + '.crai'})

    j.command(
        f"""
    samtools view {cram} -Obam -L chr21:1-10000 -o {j.output_bam}
    """
    )

    b.write_output(
        j.output_bam,
        os.path.join(f'gs://{BUCKET}', 'analysis_runner_test/batch/result.bam'),
    )
    b.run()


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
