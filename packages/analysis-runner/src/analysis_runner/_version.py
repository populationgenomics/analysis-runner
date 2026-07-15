"""Can be used to access the version from the code"""

from importlib.metadata import version

# The canonical version lives in pyproject.toml ([project].version) and is bumped
# with `uv version`. Read it back from the installed package metadata.
__version__ = version('analysis-runner')
