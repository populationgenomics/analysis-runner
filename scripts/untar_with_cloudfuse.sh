#!/usr/bin/env bash

export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

apt-get update
apt-get install -y gcsfuse

gcsfuse -v

echo "Making dir $HOME/mounted"
mkdir $HOME/mounted
ls -l $HOME

echo "Mounting bucket gs://cpg-schr-neuro-test-upload/2023-09-20/ to $HOME/mounted"
gcsfuse --foreground --debug_fuse --debug_fs --debug_gcs --debug_http --only-dir 2023-09-20 cpg-schr-neuro-test-upload $HOME/mounted
ls -l $HOME/mounted

echo "Extracting tar file"
tar -xvf $HOME/mounted/2023-09-20/test.tar -C $HOME/mounted/2023-09-20/extracted