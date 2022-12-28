"""
a hail batch script for pulling and copying the UMAP data into Hail Table
"""


import logging
import sys

import hail as hl

from cpg_utils.hail_batch import (
    copy_common_env,
    get_config,
    init_batch,
    output_path,
)
from cpg_workflows.batch import get_batch


# use logging to print statements, display at info level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr,
)


def table_things(block_zipped: str, output: str):
    """
    take the re-bgzipped bed file, ingest in hail
    update a datatype, and write out to cloud

    :param block_zipped:
    :param output:
    """
    init_batch()
    ht = hl.import_bed(block_zipped)

    # swap the type to a float (defaults to string)
    ht = ht.transmute(target=hl.float64(ht.target))
    ht.write(output)


if __name__ == '__main__':
    # australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_workflows
    # image = get_config()['workflow']['driver_image']
    init_batch()

    # add a script job
    bash_job = get_batch().new_bash_job(name='WGet UMap')
    file = 'https://bismap.hoffmanlab.org/raw/hg38/k50.umap.bedgraph.gz'
    bash_job.image('australia-southeast1-docker.pkg.dev/cpg-common/images/samtools:latest')
    bash_job.command(
        (
            f'wget -o intermediate.gz {file};'
            f'gunzip -c intermediate.gz | bgzip -@ 2 > {bash_job.output}'
        )
    )

    python_job = get_batch().new_bash_job(name='ingest and write table')
    copy_common_env(python_job)
    out_path_bgz = output_path('umap_bedgraph.bz')
    get_batch().write_output(bash_job.output, out_path_bgz)

    # python_job.image(image)
    # out_path = output_path('umap_table.ht')
    # python_job.call(table_things, block_zipped=bash_job.output, output=out_path)
    # python_job.depends_on(bash_job)

    get_batch().run(wait=False)
