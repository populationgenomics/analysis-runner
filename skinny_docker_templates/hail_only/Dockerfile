# Any analysis-runner driver image must at least include git.
ARG PY_VER=${PY_VER:-3.10}

FROM python:${PY_VER}-slim-bookworm AS basic

ARG HAIL_SHA=${HAIL_SHA:-2bb197d63afd58f9452af306e0eb14d6924d0019}

ENV HAIL_QUERY_BACKEND="service"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
        apt-transport-https \
        bzip2 \
        ca-certificates \
        git \
        gnupg \
        openjdk-17-jdk-headless \
        software-properties-common \
        wget \
        zip && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    pip install --no-cache-dir --upgrade pip

FROM basic AS builder

ARG HAIL_SHA=${HAIL_SHA:-d85618aaf4d2bf667aa72c5c6a142e9bafaae9c8}

ENV HAIL_QUERY_BACKEND="service"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential \
        curl \
        g++ \
        gcc \
        libfontconfig \
        liblapack3 \
        libopenblas-dev \
        rsync \
        software-properties-common \
        wget \
        zip && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/*

RUN pip --no-cache-dir install \
        build && \
    # Install Hail from the CPG fork.
    git clone https://github.com/populationgenomics/hail.git && \
    cd hail && \
    git checkout $HAIL_SHA && \
    cd hail && \
    # Generate a Wheel, but don't install it in this layer
    # DEPLOY_REMOTE avoids a dev suffix being appended to dataproc initialization paths.
    make wheel DEPLOY_REMOTE=1 HAIL_RELEASE_MODE=1

FROM basic AS installer

# wheel needs to have a descriptive name, e.g. hail-0.2.134-py3-none-any.whl
# but this isn't knowable exactly from the hash/branch, so we use a wildcard
# this RUN command binds in the wheel from the build layer without copying it in permanently
RUN --mount=type=bind,from=builder,source=/hail/hail/build/deploy/dist,target=/wheel \
    pip install --no-cache-dir \
        /wheel/hail-*-py3-none-any.whl
