#!/bin/bash

cd /home/ubuntu/rudder-server
GO111MODULE=on CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go build -mod vendor
