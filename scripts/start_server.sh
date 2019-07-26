#!/bin/bash

chown -R ubuntu:ubuntu /home/ubuntu/rudder-server
systemctl enable rudder.service
systemctl restart rudder.service
