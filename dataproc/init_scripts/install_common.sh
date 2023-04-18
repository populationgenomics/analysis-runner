#!/usr/bin/env bash

set -ex

# Only run this on the master node.
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" != 'Master' ]]; then
    exit 0
fi

# Dataproc by default comes with Python 3.8 only. Updating the Python version
# is unfortunately not super simple.
# Install mamba as conda takes way too long to solve the environment otherwise.
conda install -c conda-forge mamba
# Can't update Python 3.10 without removing pinned versions first.
rm /opt/conda/miniconda3/conda-meta/pinned
# Remove a few packages that prevent installing Python 3.10.
mamba remove bcolz fiona pyqt spyder
# Finally, update Python.
mamba install python=3.10

# Reinstall the Hail wheel.
pip3 install /home/hail/hail*.whl

# Install some generally useful libraries.
pip3 install \
    analysis-runner \
    bokeh \
    botocore \
    cpg-utils \
    cpg-workflows \
    gcsfs \
    pyarrow \
    sample-metadata \
    selenium>=3.8.0 \
    statsmodels \
    cloudpathlib[all] \
    gnomad \
    cryptography==38.0.4

# Install phantomjs with a workaround for the libssl_conf.so on Debian Buster:
# https://github.com/bazelbuild/rules_closure/issues/351#issuecomment-854628326
cd /opt
# Source: https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2
gsutil cat gs://cpg-common-main/hail_dataproc/phantomjs-2.1.1-linux-x86_64.tar.bz2 | tar xj
cat <<EOF > /usr/local/bin/phantomjs
#!/bin/bash
export OPENSSL_CONF=/dev/null
/opt/phantomjs-2.1.1-linux-x86_64/bin/phantomjs "\$@"
EOF
chmod +x /usr/local/bin/phantomjs
