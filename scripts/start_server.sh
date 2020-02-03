#!/bin/bash

cp /home/ubuntu/.env /home/ubuntu/rudder-server/.env
chown -R ubuntu:ubuntu /home/ubuntu/rudder-server
systemctl enable rudder.service
timestamp=$(date +%s)
mv /tmp/recovery_data.json /tmp/recovery_data_"$timestamp".json
mv /tmp/error_store.json /tmp/error_store_"$timestamp".json
systemctl restart rudder.service
