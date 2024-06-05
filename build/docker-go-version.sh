#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

v=`go version | { read _ _ v _; echo ${v#go}; }`

for DOCKERFILE_PATH in "$@"
do
    sed -i '' "s/ARG GO_VERSION=[0-9\.].*/ARG GO_VERSION=$v/" $DOCKERFILE_PATH
done