# syntax=docker/dockerfile:1
FROM golang:1.19.2-alpine3.16 AS builder

RUN mkdir workdir
WORKDIR /workdir

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o throttling ./cmd/benchmark/throttling/

FROM alpine:3.16

COPY --from=builder workdir/throttling .

CMD ["./throttling"]
