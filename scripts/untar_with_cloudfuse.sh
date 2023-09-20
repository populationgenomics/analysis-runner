#!/usr/bin/env bash

export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update
sudo apt-get install gcsfuse

gcsfuse -v

mkdir $HOME/mounted

gcsfuse --only-dir 2023-09-20 cpg-schr-neuro-test-upload $HOME/mounted

tar -xvf $HOME/mounted/2023-09-20/test.tar -C $HOME/mounted/2023-09-20/extracted