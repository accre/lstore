#!/bin/bash

# Helper to see status of all subproject repositories

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
statuses=( )
cd $LSTORE_RELEASE_BASE
echo "lstore-release $(get_repo_status .)"
cd source
for REPO in apr-accre apr-util-accre jerasure toolbox gop ibp lio; do
    RET="$(get_repo_status $REPO)"
    GIT=${RET% *}
    CLEAN=${RET##* }
    echo "$REPO $GIT $CLEAN"
done
