# LEGACY VERSION-CHECK DECOY — safe to delete after the migration window.
#
# Installed analysis-runner CLIs (<= 3.3.1) fetch this file's raw URL from the
# main branch to decide whether to print an "out of date" warning; 3.3.2+
# clients read packages/analysis-runner/pyproject.toml instead. This is the
# final bump; deleting this file is tracked in SET-1255.
__version__ = '3.3.2'
