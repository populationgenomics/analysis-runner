FROM debian:buster-slim

ARG HAIL_VERSION

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH
ENV HAIL_QUERY_BACKEND local

RUN apt-get update && apt-get install -y apt-transport-https bash bzip2 ca-certificates git gnupg jq wget zip && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt update && apt install -y google-cloud-sdk && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX \
    -c cpg -c bioconda -c conda-forge \
    bokeh \
    phantomjs \
    r-argparser \
    r-base=4.1.1 \
    r-essentials \
    r-tidyverse \
    selenium \
    skopeo \
    statsmodels && \
    rm -r /root/micromamba/pkgs && \
    pip3 install \
    cpg-hail==$HAIL_VERSION \
    cpg-utils==2.1.1 \
    analysis-runner \
    sample-metadata==4.4.0 && \
    # hailctl dataproc uses gcloud beta dataproc.
    gcloud -q components install beta
