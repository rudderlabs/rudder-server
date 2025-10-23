# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.25.3
ARG GO_VERSION_SHA256=sha256:aee43c3ccbf24fdffb7295693b6e33b21e01baec1b2a55acc351fde345e9ec34
ARG ALPINE_VERSION=3.22
ARG ALPINE_VERSION_SHA256=sha256:8a1f59ffb675680d47db6337b49d22281a139e9d709335b492be023728e11715

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_VERSION_SHA256} AS builder
RUN mkdir /app
WORKDIR /app
COPY . .
RUN go mod download && go build -o main ./suppression-backup-service/cmd/main.go

FROM alpine:${ALPINE_VERSION}@${ALPINE_VERSION_SHA256}
WORKDIR /root/
COPY --from=builder /app/main ./
CMD ["./main"]
