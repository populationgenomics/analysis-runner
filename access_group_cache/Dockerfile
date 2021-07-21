FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH

RUN apt-get update && apt-get install -y wget bzip2 && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX -c cpg -c conda-forge \
    cpg-utils==2.1.0 aiohttp flask gunicorn && \
    rm -r /root/micromamba/pkgs

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
