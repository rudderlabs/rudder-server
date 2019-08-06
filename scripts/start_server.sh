#!/bin/bash

cp /home/ubuntu/.env /home/ubuntu/rudder-server/.env
chown -R ubuntu:ubuntu /home/ubuntu/rudder-server
systemctl enable rudder.service
systemctl restart rudder.service
