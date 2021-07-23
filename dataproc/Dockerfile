FROM ubuntu:20.04

RUN apt update && \
    apt install -y --no-install-recommends g++ openjdk-8-jdk-headless libopenblas-base liblapack3 python3-pip && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    pip3 install hail==0.2.73
