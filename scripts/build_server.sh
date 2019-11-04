#!/bin/bash

cd /home/ubuntu/rudder-server
GO111MODULE=on CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++" go build -mod vendor
