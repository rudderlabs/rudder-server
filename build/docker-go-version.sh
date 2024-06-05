#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

DOCKERFILE_PATH="Dockerfile" 

v=`go version | { read _ _ v _; echo ${v#go}; }`
sed -i '' "s/ARG GO_VERSION=[0-9\.].*/ARG GO_VERSION=$v/" $DOCKERFILE_PATH