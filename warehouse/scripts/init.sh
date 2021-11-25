#!/bin/sh
apt-get -y update
apt-get -y install build-essential wget unzip libsasl2-modules-gssapi-mit netcat dpkg ca-certificates postgresql-client curl
wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.19/SimbaSparkODBC-2.6.19.1033-Debian-64bit.zip
wget ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.9.tar.gz
tar xvzf unixODBC-2.3.9.tar.gz
cd unixODBC-2.3.9/
./configure --prefix=/usr --sysconfdir=/etc/unixODBC
make
make install
cd ..
unzip SimbaSparkODBC-2.6.19.1033-Debian-64bit.zip
dpkg -i simbaspark_2.6.19.1033-2_amd64.deb
rm -rf SimbaSparkODBC-2.6.19.1033-Debian-64bit.zip simbaspark_2.6.19.1033-2_amd64.deb docs/ unixODBC-2.3.9 unixODBC-2.3.9.tar.gz

