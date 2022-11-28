#!/usr/bin/env python
# pylint: disable=import-error
"""
Example of running Hail Query script with analysis-runner.
"""
import hail as hl
from cpg_utils.hail_batch import init_batch


init_batch()

hl.utils.get_movie_lens('data/')
users = hl.read_table('data/users.ht')

users.show()

print('There are {users.count()} users in the movie dataset')
