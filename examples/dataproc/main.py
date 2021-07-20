#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""

import os
import hailtop.batch as hb
from analysis_runner import dataproc

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)

batch = hb.Batch(name='dataproc example', backend=service_backend)

dataproc.hail_dataproc_job(
    batch,
    'query.py',
    max_age='1h',
    packages=['click', 'selenium'],
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    job_name='example',
    labels={'compute_category': 'dataproc', 'stage': 'example_usage'},
)

batch.run()
