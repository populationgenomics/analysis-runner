# This image adds R support to the base image.
FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-base:1.3

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH

RUN apt update && \
    # Some R packages require a C compiler during installation.
    apt install -y build-essential && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX \
        -c cpg -c bioconda -c conda-forge \
        bioconductor-biomart \
        r-argparser \
        r-arrow \
        r-base \
        r-essentials \
        r-googlecloudstorager \
        r-tidyverse \
        r-viridis && \
    rm -r /root/micromamba/pkgs
