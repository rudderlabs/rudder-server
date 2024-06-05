#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

v=`go version | { read _ _ v _; echo ${v#go}; }`

SED="github.com/rwtodd/Go.Sed/cmd/sed-go@bb8ed5d"

for DOCKERFILE_PATH in "$@"
do
    go run $SED -i  -e "s/ARG GO_VERSION=.*/ARG GO_VERSION=$v/" "$DOCKERFILE_PATH" 
done