FROM debian:buster-slim

ARG HAIL_VERSION

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH
ENV HAIL_QUERY_BACKEND service

RUN apt-get update && apt-get install -y git wget bash bzip2 zip && \
    wget -O jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 && chmod +x ./jq && cp jq /usr/bin && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX \
        -c cpg -c bioconda -c conda-forge \
        hail=$HAIL_VERSION cpg-utils analysis-runner sample-metadata=3.4.0 bokeh selenium phantomjs statsmodels \
        r-base=4.1.1 r-essentials r-tidyverse r-argparser && \
    rm -r /root/micromamba/pkgs && \
    # hailctl dataproc uses gcloud beta dataproc.
    gcloud -q components install beta
