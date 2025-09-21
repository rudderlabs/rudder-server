FROM golang:1.24.4-alpine3.22@sha256:68932fa6d4d4059845c8f40ad7e654e626f3ebd3706eef7846f319293ab5cb7a AS builder
WORKDIR /app
COPY . .
RUN go mod download &&  \
    LDFLAGS="-swX" go build -o warehouse-transformer-benchmark ./warehouse/transformer/benchmark/main.go

FROM alpine:3.21.0@sha256:21dc6063fd678b478f57c0e13f47560d0ea4eeba26dfc947b2a4f81f686b9f45
WORKDIR /app
COPY --from=builder /app/warehouse-transformer-benchmark .
ENTRYPOINT ["/app/warehouse-transformer-benchmark"]
