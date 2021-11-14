FROM ubuntu:21.04  # defaults to python3.9, for which hail package is built

# https://askubuntu.com/questions/909277/avoiding-user-interaction-with-tzdata-when-installing-certbot-in-a-docker-contai
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y --no-install-recommends apt-transport-https ca-certificates curl g++ git gnupg openjdk-8-jdk-headless libopenblas-base liblapack3 python3-pip zip && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt update && apt install -y google-cloud-sdk && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    pip3 install cpg-hail==0.2.78
