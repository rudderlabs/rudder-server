FROM golang:1.15-alpine
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go install -mod vendor
ENTRYPOINT [ "rudder-server" ]
EXPOSE 8080
