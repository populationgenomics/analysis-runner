#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""

import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir
from analysis_runner import dataproc

config = get_config()

service_backend = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'], remote_tmpdir=remote_tmpdir()
)

batch = hb.Batch(name='dataproc example', backend=service_backend)


cluster = dataproc.setup_dataproc(
    batch,
    max_age='1h',
    packages=['click', 'selenium'],
    init=['gs://cpg-common-main/references/hail_dataproc/install_common.sh'],
    cluster_name='My Cluster with max-age=1h',
)
cluster.add_job('query.py', job_name='example')


# Don't wait, which avoids resubmissions if this job gets preempted.
batch.run(wait=False)
