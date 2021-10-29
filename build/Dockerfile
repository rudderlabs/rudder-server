FROM ubuntu:20.04
RUN apt-get update
RUN apt-get -y install build-essential unixodbc unixodbc-dev wget unzip libsasl2-modules-gssapi-mit netcat
RUN wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.18/SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip
RUN unzip SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip
RUN dpkg -i simbaspark_2.6.18.1030-2_amd64.deb
RUN rm -rf SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip simbaspark_2.6.18.1030-2_amd64.deb docs/
ENV GOLANG_VERSION 1.16.8
RUN wget https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz
RUN rm -f go${GOLANG_VERSION}.linux-amd64.tar.gz
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
