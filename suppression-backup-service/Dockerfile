# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.27.3
ARG GO_VERSION_SHA256=sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd

ARG ALPINE_VERSION=3.24
ARG ALPINE_VERSION_SHA256=sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_VERSION_SHA256} AS builder
RUN mkdir /app
WORKDIR /app
COPY . .
RUN go mod download && go build -o main ./suppression-backup-service/cmd/main.go

FROM alpine:${ALPINE_VERSION}@${ALPINE_VERSION_SHA256}
WORKDIR /root/
COPY --from=builder /app/main ./
CMD ["./main"]
