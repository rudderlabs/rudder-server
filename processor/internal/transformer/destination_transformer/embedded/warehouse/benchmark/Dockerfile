FROM golang:1.25.3-alpine3.22@sha256:aee43c3ccbf24fdffb7295693b6e33b21e01baec1b2a55acc351fde345e9ec34
WORKDIR /app
COPY . .
RUN go mod download &&  \
    LDFLAGS="-swX" go build -o warehouse-transformer-benchmark ./warehouse/transformer/benchmark/main.go

FROM alpine:3.22.0@sha256:8a1f59ffb675680d47db6337b49d22281a139e9d709335b492be023728e11715
WORKDIR /app
COPY --from=builder /app/warehouse-transformer-benchmark .
ENTRYPOINT ["/app/warehouse-transformer-benchmark"]
