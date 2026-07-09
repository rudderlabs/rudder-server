
# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.26.3
ARG GO_VERSION_SHA256=sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d
ARG ALPINE_VERSION=3.23
ARG ALPINE_VERSION_SHA256=sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11
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

RUN for attempt in 1 2 3; do \
        go mod download && break; \
        if [ "$attempt" = "3" ]; then exit 1; fi; \
        echo "go mod download failed; retrying attempt $((attempt + 1)) of 3" >&2; \
        sleep $((attempt * 5)); \
    done

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
