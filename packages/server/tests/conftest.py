import os

import pytest


def pytest_configure(config: pytest.Config):
    """Set dummy values for the environment variables used globally in util.py.
    These must be set before test collection, so a fixture would be too late.
    """
    os.environ['DRIVER_IMAGE'] = 'driver'
    os.environ['MEMBERS_CACHE_LOCATION'] = 'member-cache-dir'
