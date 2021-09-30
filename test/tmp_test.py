#!/usr/bin/env python3
"""Simple batch that tests basic functionality."""

import hailtop.batch as hb

batch = hb.Batch(name='Test Batch Move Standard')

j = batch.new_job(name='Simple Batch Test')

j.command('echo hello')

batch.run()
