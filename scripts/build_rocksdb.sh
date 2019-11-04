#!/bin/bash

sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
cd ~
wget https://github.com/facebook/rocksdb/archive/master.zip
unzip master.zip && cd rocksdb-master
make install
