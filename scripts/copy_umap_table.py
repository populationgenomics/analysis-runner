"""
a hail batch script for pulling and copying the UMAP data into Hail Table
"""


import logging
import sys

import hail as hl

from cpg_utils.hail_batch import init_batch, output_path


# use logging to print statements, display at info level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr,
)


if __name__ == '__main__':
    init_batch(worker_memory='highmem')
    """
    take the re-bgzipped bed file, ingest in hail
    update a datatype, and write out to cloud
    """
    ht = hl.import_bed('gs://cpg-tob-wgs-test/matt_umap/umap_bedgraph.bgz')

    # swap the type to a float (defaults to string)
    ht = ht.transmute(target=hl.float64(ht.target))
    ht.write(output_path('umap_table.ht'), overwrite=True)
