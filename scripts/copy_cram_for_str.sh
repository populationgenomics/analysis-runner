#!/usr/bin/env bash

#check if segDup causing extremely high coverage 
gsutil cp "gs://cpg-tob-wgs-main/cram/nagim/CPG3772.cram" "gs://cpg-tob-wgs-test/cram/nagim/CPG3772.cram"
gsutil cp "gs://cpg-tob-wgs-main/cram/nagim/CPG3772.cram.crai" "gs://cpg-tob-wgs-test/cram/nagim/CPG3772.cram.crai"

# check optical duplicate issue
gsutil cp "gs://cpg-tob-wgs-main/cram/nagim/CPG67314.cram" "gs://cpg-tob-wgs-test/cram/nagim/CPG67314.cram"
gsutil cp "gs://cpg-tob-wgs-main/cram/nagim/CPG67314.cram.crai" "gs://cpg-tob-wgs-test/cram/nagim/CPG67314.cram.crai"
