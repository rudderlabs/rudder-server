# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.26.0
ARG GO_VERSION_SHA256=sha256:d4c4845f5d60c6a974c6000ce58ae079328d03ab7f721a0734277e69905473e5

ARG ALPINE_VERSION=3.23
ARG ALPINE_VERSION_SHA256=sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_VERSION_SHA256} AS builder
RUN mkdir /app
WORKDIR /app
COPY . .
RUN go mod download && go build -o main ./suppression-backup-service/cmd/main.go

FROM alpine:${ALPINE_VERSION}@${ALPINE_VERSION_SHA256}
WORKDIR /root/
COPY --from=builder /app/main ./
CMD ["./main"]
