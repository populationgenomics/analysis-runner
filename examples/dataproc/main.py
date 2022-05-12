#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""

import os
import hailtop.batch as hb
from cpg_utils.hail_batch import remote_tmpdir
from analysis_runner import dataproc

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), remote_tmpdir=remote_tmpdir()
)

batch = hb.Batch(name='dataproc example', backend=service_backend)


cluster = dataproc.setup_dataproc(
    batch,
    max_age='1h',
    packages=['click', 'selenium'],
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    cluster_name='My Cluster with max-age=1h',
)
cluster.add_job('query.py', job_name='example')


# Don't wait, which avoids resubmissions if this job gets preempted.
batch.run(wait=False)
