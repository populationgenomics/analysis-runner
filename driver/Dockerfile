# Cloud Run doesn't like Ubuntu 20.04:
# https://stackoverflow.com/questions/61744540/unable-to-deploy-ubuntu-20-04-docker-container-on-google-cloud-run
FROM marketplace.gcr.io/google/ubuntu1804

# Install git.
RUN apt-get update && apt-get install -y git &&\
    rm -r /var/lib/apt/lists/* &&\
    rm -r /var/cache/apt/*

ENV PATH /root/miniconda3/bin:$PATH

# Install Miniconda & Hail from the conda CPG channel
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-py37_4.9.2-Linux-x86_64.sh &&\
    bash miniconda.sh -b &&\
    rm miniconda.sh &&\
    conda install -y -c cpg -c bioconda -c conda-forge hail=0.2.62.devcdb8c94 &&\
    rm -r /root/miniconda3/pkgs /root/miniconda3/src.zip
