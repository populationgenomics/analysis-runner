#!/usr/bin/env python3
# pylint: disable=import-error
"""
Example of running Hail Query script with analysis-runner.
"""
import hail as hl
from cpg_utils.hail_batch import init_batch


init_batch()

mt = hl.read_matrix_table('gs://cpg-fewgenomes-test/mt/v1.mt')

mt.show()

print(f'There are {mt.count()} rows in the dataset')
