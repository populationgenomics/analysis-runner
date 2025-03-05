# Any analysis-runner driver image must at least include git.
FROM python:3.10-slim-bullseye AS basic

ARG HAIL_SHA=${HAIL_SHA:-d85618aaf4d2bf667aa72c5c6a142e9bafaae9c8}

ENV HAIL_QUERY_BACKEND="service"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
        apt-transport-https \
        bzip2 \
        ca-certificates \
        curl \
        g++ \
        gcc \
        git \
        gnupg \
        openjdk-11-jdk-headless \
        rsync \
        software-properties-common \
        wget \
        zip && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/*

FROM basic AS builder

ARG HAIL_SHA=${HAIL_SHA:-d85618aaf4d2bf667aa72c5c6a142e9bafaae9c8}

ENV HAIL_QUERY_BACKEND="service"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential \
        libfontconfig \
        liblapack3 \
        libopenblas-base \
        rsync \
        software-properties-common \
        wget \
        zip && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/*

RUN pip --no-cache-dir install build && \
    # Install Hail from the CPG fork.
    git clone https://github.com/populationgenomics/hail.git && \
    cd hail && \
    git checkout $HAIL_SHA && \
    cd hail && \
    # Install locally, avoiding the need for a pip package.
    # DEPLOY_REMOTE avoids a dev suffix being appended to dataproc initialization paths.
    make wheel DEPLOY_REMOTE=1 HAIL_RELEASE_MODE=1

FROM basic AS installer

ARG HAIL_VER=${HAIL_VER:-0.2.133}

# wheel needs to have a descriptive name, e.g. hail-0.2.133-py3-none-any.whl
COPY --from=builder /hail/hail/build/deploy/dist/*.whl /hail-${HAIL_VER}-py3-none-any.whl

RUN pip install --no-cache-dir wheel && \
    pip install --no-cache-dir /hail-${HAIL_VER}-py3-none-any.whl
