#!/usr/bin/env python3

"""Demonstrates the use of the dataproc module."""
import os

from cpg_utils.hail_batch import get_batch

from analysis_runner import dataproc

batch = get_batch(name='dataproc example')


# get relative path of cwd to the script using os
QUERY_PATH = os.path.join(
    os.path.relpath(os.path.dirname(__file__), os.getcwd()), 'query.py',
)

cluster = dataproc.setup_dataproc(
    batch,
    max_age='1h',
    packages=['click', 'selenium'],
    cluster_name='My Cluster with max-age=1h',
)
cluster.add_job(QUERY_PATH, job_name='example')


# # Don't wait, which avoids resubmissions if this job gets preempted.
batch.run(wait=False)
