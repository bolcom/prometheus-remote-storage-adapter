#!/bin/bash 
set -e

if [[ $1 == "" ]]; then
  echo "Provide a tag for the container"
  exit 1
fi

go build

docker build -t eu.gcr.io/bolcom-sbx-monitoring-clients/prometheus_remote_storage_adapter:$1 .
gcloud docker -- push eu.gcr.io/bolcom-sbx-monitoring-clients/prometheus_remote_storage_adapter:$1
