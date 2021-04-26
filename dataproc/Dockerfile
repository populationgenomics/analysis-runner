FROM google/cloud-sdk:335.0.0

ARG HAIL_VERSION=0.2.65

RUN apt-get update && apt-get install -y zip

RUN pip3 install hail==$HAIL_VERSION
