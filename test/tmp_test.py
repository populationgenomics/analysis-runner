#!/usr/bin/env python3
"""Simple batch that tests basic functionality."""

import hailtop.batch as hb
from sample_metadata.api import AnalysisApi

aapi = AnalysisApi()
samples_without_analysis = aapi.get_all_sample_ids_without_analysis_type(
    'gvcf', 'test_samples'
)

batch = hb.Batch(name='Test Batch Move Standard')

j = batch.new_job(name='Simple Batch Test')

j.command('echo hello')

batch.run()
