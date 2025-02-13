# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.23.5
ARG ALPINE_VERSION=3.20
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
RUN mkdir /app
WORKDIR /app
COPY . .
RUN go mod download && go build -o main ./suppression-backup-service/cmd/main.go

FROM alpine:${ALPINE_VERSION}
WORKDIR /root/
COPY --from=builder /app/main ./
CMD ["./main"]
