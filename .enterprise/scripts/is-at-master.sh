#!/bin/sh

cd `dirname $0`/../..
source .enterprise/env

# Check enterprise repo is loaded
if [ ! -d "$ENTERPRISE_DIR" ]; then
  echo "ERROR: Enterprise version is not initialised. Please run 'make enterprise-init'"
  exit 1
fi

git fetch origin master

# Compute the distance from the current branch to the remote master branch
DISTANCE_ORIGIN_MASTER=`cd $ENTERPRISE_DIR; git rev-list --count HEAD..origin/master`

if [ "$DISTANCE_ORIGIN_MASTER" -gt 0 ]; then
  echo "ERROR: Enterprise repo is behind origin/master by $DISTANCE_ORIGIN_MASTER commit(s):"
  cd $ENTERPRISE_DIR; git log --color HEAD..origin/master | cat
  echo ""
  echo "Please, update .enterprise-commit file."
  exit 1
fi