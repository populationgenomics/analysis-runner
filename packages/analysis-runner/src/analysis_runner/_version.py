from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version('analysis-runner')
except PackageNotFoundError:
    # Running from an uninstalled source tree.
    __version__ = '0.0.0+unknown'
