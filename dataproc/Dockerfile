FROM ubuntu:20.04

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
    apt update && apt install -y google-cloud-sdk && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    git clone https://github.com/populationgenomics/hail.git && \
    cd hail/hail && \
    make install DEPLOY_REMOTE=1 && \
    cd ../.. && \
    rm -rf hail
