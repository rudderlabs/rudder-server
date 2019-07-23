#!/bin/bash

cd /home/ubuntu
chown -R ubuntu:ubuntu /home/ubuntu/rudder-server

# Stop all servers and start the server as a daemon
kill $(lsof -t -i:8080)
./rudder-server
