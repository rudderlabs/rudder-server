FROM golang:1.23.6-alpine3.20 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o warehouse-transformer-benchmark main.go

FROM alpine:3.21.0
WORKDIR /app
COPY --from=builder /app/warehouse-transformer-benchmark .
RUN chmod +x warehouse-transformer-benchmark
ENTRYPOINT ["/app/warehouse-transformer-benchmark"]
