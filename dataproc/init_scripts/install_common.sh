#!/usr/bin/env bash

set -ex

# Only run this on the master node.
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" != 'Master' ]]; then
    exit 0
fi

# Reinstall the Hail wheel.
pip3 install /home/hail/hail*.whl

# 2024-01-12: Although I'm not _exactly_ sure why, cpg-utils is not being correctly installed
# in the initialisation script, the other dependencies seem fine, so this _hack_ resolves that.
pip3 install --no-deps 'cpg-utils>=5.0.4'

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
