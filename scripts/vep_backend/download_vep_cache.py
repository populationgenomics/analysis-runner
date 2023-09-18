#!/usr/bin/env python3

"""
download the VEP cache file and write it into GCP
"""


from cpg_workflows.batch import get_batch, dataset_path
from cpg_utils.config import get_config


# nothin' to it but to do it - copy that file
b = get_batch(name='download VEP 110 cache')
j = b.new_job(name='curl to local')
j.image(get_config()['workflow']['driver_image'])
j.command(f'curl https://ftp.ensembl.org/pub/release-110/variation/vep/homo_sapiens_vep_110_GRCh38.tar.gz > {j.ofile}')
b.write_output(j.ofile, dataset_path('VEP_110_cache'))
b.run(wait=False)
