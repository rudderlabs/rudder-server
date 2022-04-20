FROM golang:1.17.8-alpine3.15
RUN apk add --no-cache build-base \
&& mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go install
COPY scripts/generate-event /scripts
COPY scripts/batch.json /scripts
ENTRYPOINT [ "rudder-server" ]
EXPOSE 8080
