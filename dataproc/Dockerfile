FROM ubuntu:22.04

# Give this Dockerfile the ability to checkout a specific commit hash. This is
# useful if you want to build a specific Hail version inside the image instead
# of the latest commit on master.
ARG COMMIT_HASH

# Give this Dockerfile the ability to strip all pinned versions from the
# 'deploy.yaml' file. This is useful if you are having issues with pip
# dependency resolution during cluster creation.
ARG STRIP_PIP_VERSIONS=false

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    g++ \
    git \
    gnupg \
    liblapack3 \
    libopenblas-base \
    make \
    openjdk-8-jdk-headless \
    python3-pip \
    rsync \
    zip && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Checkout a specific commit if supplied, to install a specific Hail version.
RUN git clone https://github.com/populationgenomics/hail.git
RUN if [ -n "$COMMIT_HASH" ]; then cd hail && git reset --hard ${COMMIT_HASH} && cd ..; fi

# Build Hail from source, this might take some time.
RUN cd hail/hail && \
    make install DEPLOY_REMOTE=1 && \
    cd ../.. && \
    rm -rf hail

# Install additional Python packages into the driver image. We need cpg-utils to
# read secrets from the config file for GitHub authentication to clone private 
# repositories. Files/scripts from the cloned repository are sent to the dataproc 
# cluster created by this driver image. These pip packages are separate from the 
# Python packages installed to the dataproc cluster via the init scripts.
RUN pip3 install cpg-utils

# This strips all pinned versions from the 'deploy.yaml' file, which is created
# during the hail build step. This file is used by the hailctl command line
# program to set the 'metadata' argument of the dataproc cluster create command.
# The metadata argument is by used the cluster to install specific versions of
# Python packages on the cluster. Strip  versions if you are having issues with
# pip dependency resolution during cluster creation.
#
# This may surface as the error below when the `./init_scripts/install_common.sh`
# script is being executed during cluster initialisation:
#
#   pip3 install --no-dependencies '/home/hail/hail*.whl'
#   WARNING: Requirement '/home/hail/hail*.whl' looks like a filename, but the file does not exist
#   ERROR: hail*.whl is not a valid wheel filename.
#
# This may be due to a failure at a previous step which uses the `deploy.yaml`
# file. This file contains info on pip dependencies and the location of the hail
# wheel file. So, if one part of this step fails, for example the pip intall from
# the `pip_dependencies` section of this yaml file, then the hail wheel won't be
# copied over to the cluster resulting in the above error. However, remember to
# check your error logs for the exact error message.
RUN if [ "$STRIP_PIP_VERSIONS" == "true" ]; then sed -i 's/[<>=~!][^|]*//g' /usr/local/lib/python3.10/dist-packages/hailtop/hailctl/deploy.yaml; fi
