"""A very simple batch that tests basic functionality."""

import os
import hail as hl
import hailtop.batch as hb

hl.init()

backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)

batch = hb.Batch(backend=backend, name='analysis-server-test')

job = batch.new_job(name='test')
job.image('australia-southeast1-docker.pkg.dev/analysis-runner/images/ubuntu:20.04')
job.command('echo "hello world"')

batch.run()
