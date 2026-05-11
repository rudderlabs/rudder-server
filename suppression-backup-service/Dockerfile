# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.26.3
ARG GO_VERSION_SHA256=sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d

ARG ALPINE_VERSION=3.23
ARG ALPINE_VERSION_SHA256=sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_VERSION_SHA256} AS builder
RUN mkdir /app
WORKDIR /app
COPY . .
RUN go mod download && go build -o main ./suppression-backup-service/cmd/main.go

FROM alpine:${ALPINE_VERSION}@${ALPINE_VERSION_SHA256}
WORKDIR /root/
COPY --from=builder /app/main ./
CMD ["./main"]
