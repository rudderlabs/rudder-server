FROM ubuntu:14.04
COPY --from=golang:1.16.8-alpine /usr/local/go/ /usr/local/go/
ENV PATH="/usr/local/go/bin:${PATH}"
RUN apt-get install unixODBC unixODBC-dev wget unzip libsasl2-modules-gssapi-mit
RUN wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.18/SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip
RUN unzip SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip
RUN dpkg -i simbaspark_2.6.18.1030-2_amd64.deb
RUN rm -rf SimbaSparkODBC-2.6.18.1030-Debian-64bit.zip simbaspark_2.6.18.1030-2_amd64.deb docs/
RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go install
ENTRYPOINT [ "rudder-server" ]
EXPOSE 8080
