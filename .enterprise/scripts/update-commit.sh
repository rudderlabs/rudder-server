#!/bin/sh

cd `dirname $0`/../..
source .enterprise/env

# Check enterprise repo is loaded
if [ ! -d "$ENTERPRISE_DIR" ]; then
  echo "ERROR: Enterprise version is not initialised. Please run 'make enterprise-init'"
  exit 1
fi

# Check if commit needs updating
STORED_COMMIT=`cat $ENTERPRISE_COMMIT_FILE`
CURRENT_COMMIT=`cd $ENTERPRISE_DIR; git rev-parse HEAD`

# check if enterprise repo has any unstaged changes
ENTERPRISE_UNSTAGED=`cd $ENTERPRISE_DIR; git status -s -uall`
if [ ! -z "$ENTERPRISE_UNSTAGED" ]; then
  echo "WARNING: There are unstaged changes in enterprise repo."
  HAS_WARNING=true
fi

# check if enterprise repo has no remote
ENTERPRISE_BRANCH=`cd $ENTERPRISE_DIR; git name-rev --name-only HEAD`
ENTERPRISE_REMOTE=`cd $ENTERPRISE_DIR; git config branch.$ENTERPRISE_BRANCH.remote`
if [ -z "$ENTERPRISE_REMOTE" ]; then
  echo "WARNING: Current enterprise repo branch '$ENTERPRISE_BRANCH' has no remote."
  HAS_WARNING=true
fi

# check if enterprise remote repo exists
if [ ! -z "$ENTERPRISE_REMOTE" ]; then
  ENTERPRISE_REMOTE_EXISTS=`cd $ENTERPRISE_DIR; git branch -a | grep remotes/$ENTERPRISE_REMOTE/$ENTERPRISE_BRANCH`
  if [ -z "$ENTERPRISE_REMOTE_EXISTS" ]; then
    echo "WARNING: Enterprise repo remote branch '$ENTERPRISE_REMOTE/$ENTERPRISE_BRANCH' does not yet exist."
      HAS_WARNING=true
  fi
fi


# check if enterprise repo has unpushed commits
if [ ! -z "$ENTERPRISE_REMOTE" ]; then
  ENTERPRISE_DIFF=`cd $ENTERPRISE_DIR; git rev-list --left-right $ENTERPRISE_BRANCH...$ENTERPRISE_REMOTE/$ENTERPRISE_BRANCH`
  if [ ! -z "$ENTERPRISE_DIFF" ]; then
    echo "WARNING: There are either unpushed changes, or current branch '$ENTERPRISE_BRANCH' is behind '$ENTERPRISE_REMOTE/$ENTERPRISE_BRANCH'"
      HAS_WARNING=true
  fi
fi

if [ "$STORED_COMMIT" = "$CURRENT_COMMIT" ]; then
  # Current commit already stored
  echo "Stored enterprise commit '$STORED_COMMIT' already matches commit in '$ENTERPRISE_DIR'"
  exit 0
fi

if [ ! -z "$HAS_WARNING" ]; then
  read -p "Would you like to continue updating to '$CURRENT_COMMIT'? (y/N) " -n 1 -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

echo $CURRENT_COMMIT > $ENTERPRISE_COMMIT_FILE

echo "Commit hash in '$ENTERPRISE_COMMIT_FILE' updated to '$CURRENT_COMMIT'"