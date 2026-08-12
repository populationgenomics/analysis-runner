# LEGACY VERSION-CHECK DECOY — safe to delete after the migration window.
#
# Installed analysis-runner CLIs (<= 3.2.6) fetch this file's raw URL from the
# main branch and regex-parse the line below to decide whether to print an
# "out of date" warning. The real package version now lives in
# packages/analysis-runner/pyproject.toml (bumped with `uv version`); this file
# exists only so pre-restructure clients keep receiving upgrade nudges.
#
# Maintenance: bump the line below ONCE, when the first post-restructure
# release ships, so old clients are nudged onto it. Do not track further
# releases here. Deletion is tracked in the follow-up GitHub issue.
__version__ = '3.2.6'
