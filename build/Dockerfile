FROM ubuntu:20.04
ADD warehouse/scripts/init.sh /
RUN bash init.sh
ENV GOLANG_VERSION 1.16.8
RUN wget https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && rm -f go${GOLANG_VERSION}.linux-amd64.tar.gz
ENV PATH "$PATH:/usr/local/go/bin"
RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go install
ENTRYPOINT [ "rudder-server" ]
EXPOSE 8080
