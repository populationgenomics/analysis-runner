#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""

import os
import hailtop.batch as hb
from analysis_runner import dataproc

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)

batch = hb.Batch(name='dataproc example', backend=service_backend)


cluster = dataproc.setup_dataproc(
    batch,
    max_age='1h',
    packages=['click', 'selenium'],
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    cluster_title='My Cluster',
)
cluster.add_job('query.py', job_name='example')


# Don't wait, which avoids resubmissions if this job gets preempted.
batch.run(wait=False)
