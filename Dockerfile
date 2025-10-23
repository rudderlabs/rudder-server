
# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.25.3
ARG GO_VERSION_SHA256=sha256:aee43c3ccbf24fdffb7295693b6e33b21e01baec1b2a55acc351fde345e9ec34
ARG ALPINE_VERSION=3.22
ARG ALPINE_VERSION_SHA256=sha256:8a1f59ffb675680d47db6337b49d22281a139e9d709335b492be023728e11715
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
