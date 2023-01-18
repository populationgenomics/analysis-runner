"""
a hail batch script for pulling and copying the UMAP data into Hail Table
"""


import logging
import sys

import hail as hl

from cpg_utils.hail_batch import copy_common_env, init_batch, output_path
from cpg_workflows.batch import get_batch


# use logging to print statements, display at info level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr,
)


if __name__ == '__main__':
    """
    This could probably be streamlined better
    ran into some issues communicating the bashjob.output
    into a python job as input
    """

    init_batch(worker_memory='highmem')

    # add a script job
    bash_job = get_batch().new_bash_job(name='WGet UMap')
    file = 'https://bismap.hoffmanlab.org/raw/hg38/k50.umap.bedgraph.gz'
    bash_job.image('australia-southeast1-docker.pkg.dev/cpg-common/images/samtools:1.16.1')
    bash_job.command(
        (
            f'wget {file} &&'
            f'gunzip -c k50.umap.bedgraph.gz | bgzip > {bash_job.output}'
        )
    )

    python_job = get_batch().new_bash_job(name='ingest and write table')
    copy_common_env(python_job)
    out_path_bgz = output_path('umap_bedgraph.bgz')
    get_batch().write_output(bash_job.output, out_path_bgz)
    get_batch().run(wait=True)

    """
    take the re-bgzipped bed file, ingest in hail
    update a datatype, and write out to cloud
    """
    ht = hl.import_bed(out_path_bgz)

    # swap the type to a float (defaults to string)
    ht = ht.transmute(target=hl.float64(ht.target))
    ht.write(output_path('umap_table.ht'), overwrite=True)
