#!/usr/bin/env python3

"""Run VEP in parallel using batch backend"""

import click
from cpg_utils import to_path
from cpg_utils.hail_batch import dataset_path, output_path
from cpg_workflows.batch import get_batch
from cpg_workflows.jobs.vep import add_vep_jobs


@click.command()
@click.option('--vep-version', help='Version of VEP', default='105')
@click.option('--vcf-path', required=True, help='Path to VCF to run VEP on')
@click.option(
    '--output-ht',
    required=True,
    help='Path to where finished VEP-annotated VCF will be output',
)
@click.option(
    '--scatter-count',
    required=False,
    help='Number of fragments to generate; default is 2',
)
def main(vep_version: str, vcf_path: str, output_ht: str, scatter_count: int):
    """
    Run VEP in parallel using Picard tools intervals as partitions.
    """

    b = get_batch(f'Run VEP with Batch Backend, VEP v{vep_version}')
    add_vep_jobs(
        b=b,
        # I'm not sure what 'to_path' does here - is it needed?
        vcf_path=to_path(dataset_path(vcf_path)),
        # would something like this work to get the tmp path from the output_path input?
        tmp_prefix=to_path(output_path('vcf_fragments/', 'tmp')),
        out_path=to_path(dataset_path(output_ht)),
        scatter_count=scatter_count,
    )
    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
