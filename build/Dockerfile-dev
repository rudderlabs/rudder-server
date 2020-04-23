FROM golang:1.13-alpine
ENV GO111MODULE=on
RUN apk add git
RUN go get github.com/onsi/ginkgo/ginkgo
RUN mkdir /app
ADD . /app
WORKDIR /app
ENTRYPOINT ["go", "run", "-mod", "vendor", "main.go"]
#ENTRYPOINT ["go", "run", "main.go"]
