FROM ubuntu:20.04

RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y \
        g++ \
        liblapack3 \
        libopenblas-base \
        openjdk-8-jdk-headless \
        python3-pip \
        rsync && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    # Install Hail from the CPG fork.
    git clone https://github.com/populationgenomics/hail.git && \
    cd hail/hail && \
    # Install locally, avoiding the need for a pip package.
    # DEPLOY_REMOTE avoids a dev suffix being appended to dataproc initialization paths.
    make install DEPLOY_REMOTE=1 && \
    cd ../.. && \
    rm -rf hail
