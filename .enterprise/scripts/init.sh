#!/bin/sh

cd `dirname $0`/../..
source .enterprise/env

if [ ! -d "$ENTERPRISE_DIR" ]; then
    echo "Cloning enterprise repo at '$ENTERPRISE_DIR'"
    git clone $ENTERPRISE_REPO $ENTERPRISE_DIR;
fi

if [ -f "$ENTERPRISE_COMMIT_FILE" ]; then
    ENTERPRISE_COMMIT=`cat $ENTERPRISE_COMMIT_FILE`
    echo "Checking out linked enterprise commit '$ENTERPRISE_COMMIT'"
    (cd ./$ENTERPRISE_DIR; git fetch; git checkout $ENTERPRISE_COMMIT);
fi
