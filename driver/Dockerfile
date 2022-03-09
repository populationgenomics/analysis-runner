# Any analysis-runner driver image must at least include gcloud and git.
FROM debian:bullseye-slim AS base

RUN apt update && apt install -y apt-transport-https bash bzip2 ca-certificates curl git gnupg jq skopeo wget zip && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt update && apt install -y google-cloud-sdk && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/*

# Adds Hail and some other Python-based stats libraries. This is the default image.
FROM base as hail

ARG HAIL_VERSION

ENV HAIL_QUERY_BACKEND service

# See https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199#23 about the mkdir.
RUN mkdir /usr/share/man/man1 && \
    apt update && apt install -y g++ libopenblas-base liblapack3 openjdk-11-jre-headless python3-pip && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    pip3 install \
        analysis-runner \
        bokeh \
        cpg-hail==$HAIL_VERSION \
        cpg-utils \
        phantomjs \
        sample-metadata \
        selenium \
        statsmodels

# Adds R (through conda), which isn't used as frequently.
FROM hail as r-tidyverse

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH

RUN wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX \
        -c cpg -c bioconda -c conda-forge \
        r-argparser \
        r-base=4.1.1 \
        r-essentials \
        r-tidyverse && \
    rm -r /root/micromamba/pkgs
