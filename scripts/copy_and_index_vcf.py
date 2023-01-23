"""
test script
"""


from cpg_workflows.batch import get_batch
from cpg_utils.config import get_config

import click


@click.command()
@click.option('--input_vcf')
@click.option('--output_root')
def main(input_vcf: str, output_root: str):
    """
    does the stuff
    """

    bcftools_job = get_batch().new_job('Compress and index')
    bcftools_job.image(get_config()['images']['bcftools'])

    bcftools_job.declare_resource_group(
        vcf_sorted={
            'vcf.gz': '{root}.vcf.gz',
            'vcf.gz.tbi': '{root}.vcf.gz.tbi',
        }
    )

    # read the input file in
    vcf_in_batch = get_batch().read_input(input_vcf)
    cmd = (
        'echo "I AM RUNNING" \n'
        'set -ex \n'
        f'bgzip -c {vcf_in_batch} > {bcftools_job.vcf_sorted["vcf.gz"]} \n'
        f'tabix -p vcf {bcftools_job.vcf_sorted["vcf.gz"]} \n'
    )
    bcftools_job.command(cmd)

    get_batch().write_output(bcftools_job.vcf_sorted, output_root)

    get_batch().run(wait=False)


if __name__ == '__main__':

    main()
