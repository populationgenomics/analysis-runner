"""Additional initialization actions for the Dataproc cluster."""

import subprocess

# bokeh needs phantomjs for plotting.
subprocess.check_call(['npm', 'install', '-g', 'phantomjs-prebuilt'])
