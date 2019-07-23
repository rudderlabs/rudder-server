#!/bin/bash

chown -R ubuntu:ubuntu /home/ubuntu/rudder-server
service rudder stop || true
service rudder start
