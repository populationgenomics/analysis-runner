#!/usr/bin/env bash

set -ex

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
