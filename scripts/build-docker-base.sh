#!/bin/bash

# Builds all of the known docker images

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

note "Generating configs"
$LSTORE_SCRIPT_BASE/generate-docker-base.sh

cd $LSTORE_SCRIPT_BASE/docker/base
STATUS=""

# Parse comand line
DISTROS=( "$@" )
if [ ${#DISTROS[@]} -eq 0 ]; then
    DISTROS=( */ )
fi
DISTROS=( "${DISTROS[@]%/}" )

for DISTRO in "${DISTROS[@]}"; do
    note "Processing $(basename $DISTRO)"
    docker build --force-rm=true --rm=true \
        -t "lstore/builder:$(basename $DISTRO)" "$DISTRO" || \
        STATUS="${STATUS}"$'\n'"Failed to build $DISTRO" && \
        STATUS="${STATUS}"$'\n'"Successfully built $DISTRO"
done
if [ ! -z "$STATUS" ]; then
    note "$STATUS"
else
    fatal "Nothing was built?"
fi
