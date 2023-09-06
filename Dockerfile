
# syntax=docker/dockerfile:1
ARG GO_VERSION=1.21.0
ARG ALPINE_VERSION=3.18
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG VERSION
ARG REVISION
ARG COMMIT_HASH
ARG ENTERPRISE_TOKEN
ARG RACE_ENABLED=false
ARG CGO_ENABLED=0
ARG PKG_NAME=github.com/rudderlabs/release-demo

RUN apk add --update make tzdata ca-certificates

WORKDIR /rudder-server

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN BUILD_DATE=$(date "+%F,%T") \
    LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT_HASH} -X main.buildDate=$BUILD_DATE -X main.builtBy=${REVISION} -X main.builtBy=${REVISION} -X main.enterpriseToken=${ENTERPRISE_TOKEN} " \
    make build

RUN go build -o devtool ./cmd/devtool/


FROM alpine:${ALPINE_VERSION}

RUN apk update && apk add tzdata
RUN apk -U --no-cache upgrade && \
    apk add --no-cache ca-certificates postgresql-client curl bash

COPY --from=builder rudder-server/rudder-server .
COPY --from=builder rudder-server/build/wait-for-go/wait-for-go .
COPY --from=builder rudder-server/build/regulation-worker .
COPY --from=builder rudder-server/devtool .

COPY build/docker-entrypoint.sh /
COPY build/wait-for /
COPY ./rudder-cli/rudder-cli.linux.x86_64 /usr/bin/rudder-cli
COPY scripts/generate-event /scripts/generate-event
COPY scripts/batch.json /scripts/batch.json

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/rudder-server"]
