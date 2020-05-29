#!/bin/zsh

export GO111MODULE=on
VERSION="multinode-dev"
DATE=$(date "+%F,%T")

CGO_ENABLED=0 GOOS=linux go build -mod vendor -a -installsuffix cgo -ldflags="-s -w -X main.version=$VERSION -X main.buildDate=$DATE"

echo build Docker image on `date`
docker build -t rudderlabs/develop-rudder-server:$VERSION -f build/Dockerfile-aws .
echo build Docker image complete `date`
echo push latest Docker images to docker hub...
docker push rudderlabs/develop-rudder-server:$VERSION

