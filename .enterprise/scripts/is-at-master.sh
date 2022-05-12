#!/bin/sh

cd `dirname $0`/../..
. .enterprise/env

# Compute the distance from the current branch to the remote master branch
ORIGIN_MASTER_COMMIT=`git ls-remote $ENTERPRISE_REPO master | cut -f 1`
LOCAL_ENTERPRISE_COMMIT=`cat .enterprise-commit`

if [ "$ORIGIN_MASTER_COMMIT" != "$LOCAL_ENTERPRISE_COMMIT" ]; then
  echo "ERROR: .enterprise-commit is not pointing to enterprise origin/master"
  echo "Update .enterprise-commit file:"
  echo ""
  echo "\t echo $ORIGIN_MASTER_COMMIT > .enterprise-commit "
  echo ""
  echo ""
  exit 1
fi