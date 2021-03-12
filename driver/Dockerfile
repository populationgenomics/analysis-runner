FROM debian:buster-slim

ARG HAIL_VERSION

# Use bash in RUN commands and make sure .bashrc is sourced to enable
# micromamba environment.
SHELL ["/bin/bash", "-c"]
ENV BASH_ENV ~/.bashrc

ENV HAIL_QUERY_BACKEND service

RUN apt-get update && apt-get install -y git wget bash bzip2 && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    wget -qO- https://micromamba.snakepit.net/api/micromamba/linux-64/latest | tar -xvj bin/micromamba --strip-components=1 && \
    ./micromamba shell init -s bash -p ~/micromamba && \
    echo "micromamba activate" >> ~/.bashrc && \
    source ~/.bashrc && \
    micromamba install -y -c cpg -c bioconda -c conda-forge hail=$HAIL_VERSION bokeh selenium phantomjs && \
    rm -r /root/micromamba/pkgs && \
    # hailctl dataproc uses gcloud beta dataproc.
    gcloud -q components install beta
