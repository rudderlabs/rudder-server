FROM golang:1.25.5-alpine3.23@sha256:26111811bc967321e7b6f852e914d14bede324cd1accb7f81811929a6a57fea9
WORKDIR /app
COPY . .
RUN go mod download &&  \
    LDFLAGS="-swX" go build -o warehouse-transformer-benchmark ./warehouse/transformer/benchmark/main.go

FROM alpine:3.23@sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375
WORKDIR /app
COPY --from=builder /app/warehouse-transformer-benchmark .
ENTRYPOINT ["/app/warehouse-transformer-benchmark"]
