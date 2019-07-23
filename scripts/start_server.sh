#!/bin/bash

chown -R ubuntu:ubuntu /home/ubuntu/rudder-server
cd /home/ubuntu/rudder-server
cp /home/ubuntu/.env /home/ubuntu/rudder-server/.env

# Stop all servers and start the server as a daemon
kill $(lsof -t -i:8080) || true
nohup ./rudder-server &
