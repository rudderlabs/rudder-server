# syntax=docker/dockerfile:1
FROM golang:1.21.0-alpine3.18 AS builder

RUN mkdir workdir
WORKDIR /workdir

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o throttling ./cmd/benchmark/throttling/

FROM alpine:3.21.0

COPY --from=builder workdir/throttling .

CMD ["./throttling"]
