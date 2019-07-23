#!/bin/bash

chown -R ubuntu:ubuntu /home/ubuntu/rudder-server

# Copy the .env from home folder
cp /home/ubuntu/.env /home/ubuntu/rudder-server/.env

systemctl enable rudder.service
systemctl restart rudder.service
