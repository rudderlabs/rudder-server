# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine3.24@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder

RUN mkdir workdir
WORKDIR /workdir

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o throttling ./cmd/benchmark/throttling/

FROM alpine:3.24@sha256:71e9b7bae160ba167fa17414401b94cbcdaf3c32feeab9298bd794a2a0614c35

COPY --from=builder workdir/throttling .

CMD ["./throttling"]
