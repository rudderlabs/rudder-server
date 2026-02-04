
# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.25.6
ARG GO_VERSION_SHA256=sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92
ARG ALPINE_VERSION=3.23
ARG ALPINE_VERSION_SHA256=sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_VERSION_SHA256} AS builder 
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
    LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT_HASH} -X main.buildDate=$BUILD_DATE -X main.builtBy=${REVISION} -X main.enterpriseToken=${ENTERPRISE_TOKEN} " \
    make build

RUN go build -o devtool ./cmd/devtool/
RUN go build -o rudder-cli ./cmd/rudder-cli/

FROM alpine:${ALPINE_VERSION}@${ALPINE_VERSION_SHA256}

RUN apk --no-cache upgrade && \
    apk --no-cache add tzdata ca-certificates postgresql-client curl bash

COPY --from=builder rudder-server/rudder-server .
COPY --from=builder rudder-server/build/wait-for-go/wait-for-go .
COPY --from=builder rudder-server/build/regulation-worker .
COPY --from=builder rudder-server/devtool .
COPY --from=builder rudder-server/rudder-cli /usr/bin/rudder-cli

COPY build/docker-entrypoint.sh /
COPY build/wait-for /
COPY scripts/generate-event /scripts/generate-event
COPY scripts/batch.json /scripts/batch.json

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/rudder-server"]
