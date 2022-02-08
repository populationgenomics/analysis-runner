#!/usr/bin/env bash

IMAGE=australia-southeast1-docker.pkg.dev/cpg-common/images/cpg-pipes:0.2.8
docker pull $IMAGE
docker run -v $PWD:$PWD $IMAGE python $PWD/batch.py $@
