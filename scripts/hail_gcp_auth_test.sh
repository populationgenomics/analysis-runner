#!/usr/bin/env bash
gcloud auth list

curl "http://metadata.google.internal"
