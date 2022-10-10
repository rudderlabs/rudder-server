# Rudder server devtool

This tool contains several commands to assist in running, debugging and manually testing rudder-server in a local environment.

## etcd

etcd is a crucial component when running rudder-server in normal mode. Using the script you can:
    - switch between normal and degraded mode of rudder-server
    - change the workspace the server is responsible for
    - wait for changes to be ack, or not using `--no-wait`
    - list all the keys of etcd for debugging
  
## event

You can send events to a running rudder server.

## WEBHOOK

Simulates a destination.
