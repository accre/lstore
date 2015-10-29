#!/bin/bash

# Helper tool to make forks for all lstore-related projects
# Usage: ./github-make-forks.sh

set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
load_github_token

set +e
GH_USER=$(git config --get user.github)
if [ $? -ne 0 ]; then
    GH_USER=$(ssh -T git@github.com | sed 's/Hi \(.*\)!.*/\1/')
fi
set -e
[ -z "$GH_USER" ] && \
    fatal "Either set up user.github with your github account name or set
up your SSH client to access github. (preferrably both)"

ALL_HEAD_BRANCHES="$LSTORE_HEAD_BRANCHES
release=master"
for VAL in $ALL_HEAD_BRANCHES; do
    REPO_NAME="lstore-${VAL%=*}"
    curl --request POST --fail \
        -H "Authorization: token $LSTORE_GITHUB_TOKEN" \
        https://api.github.com/repos/accre/${REPO_NAME}/forks
done
