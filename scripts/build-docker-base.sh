#!/bin/bash

# Builds all of the known docker images

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

note "Generating configs"
$LSTORE_SCRIPT_BASE/generate-docker-base.sh

DIRECTORY_NAMES="builder buildslave"
STATUS=""
for DIR in $DIRECTORY_NAMES; do
    cd $LSTORE_SCRIPT_BASE/docker/$DIR

    # Parse comand line
    DISTROS=( "$@" )
    if [ ${#DISTROS[@]} -eq 0 ]; then
        DISTROS=( */ )
    fi
    DISTROS=( "${DISTROS[@]%/}" )
    SUCCESS=0

    for DISTRO in "${DISTROS[@]}"; do
        if [ ! -d $DISTRO ]; then
            continue
        fi
        note "Processing $(basename $DISTRO)"
        docker build --force-rm=true --rm=true \
            -t "lstore/$DIR:$(basename $DISTRO)" "$DISTRO"
        if [ $? -ne 0 ]; then
            SUCCESS=1
            STATUS="${STATUS}"$'\n'"Failed to build $DISTRO"
        else
            STATUS="${STATUS}"$'\n'"Successfully built $DISTRO"
        fi
    done
    if [ ! -z "$STATUS" ]; then
        if [ $SUCCESS -eq 0 ]; then
            note "$STATUS"
        else
            fatal "$STATUS"
        fi
    else
        fatal "Nothing was built?"
    fi
done
