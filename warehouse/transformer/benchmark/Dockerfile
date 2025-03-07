FROM --platform=$BUILDPLATFORM golang:1.23.6-alpine3.20 AS builder
ARG COMMIT_ID="unknown"
WORKDIR /app
COPY . .
RUN go mod download && LDFLAGS="-swX" go build -o warehouse-transformer-benchmark ./warehouse/transformer/benchmark/main.go

FROM alpine:3.21.0
WORKDIR /app
COPY --from=builder /app/warehouse-transformer-benchmark .
RUN chmod +x warehouse-transformer-benchmark
ENTRYPOINT ["/app/warehouse-transformer-benchmark"]
