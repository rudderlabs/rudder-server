# syntax=docker/dockerfile:1
FROM golang:1.21.0-alpine3.18@sha256:445f34008a77b0b98bf1821bf7ef5e37bb63cc42d22ee7c21cc17041070d134f AS builder

RUN mkdir workdir
WORKDIR /workdir

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o throttling ./cmd/benchmark/throttling/

FROM alpine:3.21.3@sha256:a8560b36e8b8210634f77d9f7f9efd7ffa463e380b75e2e74aff4511df3ef88c

COPY --from=builder workdir/throttling .

CMD ["./throttling"]
