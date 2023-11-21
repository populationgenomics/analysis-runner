#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""
import os

import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir

from analysis_runner import dataproc

QUERY_FILE_LOCATION = os.path.join(os.path.dirname(__file__), 'query.py')

config = get_config()

service_backend = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'], remote_tmpdir=remote_tmpdir()
)

batch = hb.Batch(name='dataproc example', backend=service_backend)


cluster = dataproc.setup_dataproc(
    batch,
    max_age='1h',
    packages=['click', 'selenium'],
    init=[
        'gs://cpg-common-main/hail_dataproc/2023-11-22-mfranklin-dev/install_common.sh'
    ],
    cluster_name='mfranklin-dataproc-test',
)
cluster.add_job(QUERY_FILE_LOCATION, job_name='example')


# Don't wait, which avoids resubmissions if this job gets preempted.
batch.run(wait=False)
