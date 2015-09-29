#!/bin/bash

# Lists docker images for package-building in docker
# Usage: ./list-docker-base.sh [versions]
#    ie: ./list-docker-base.sh
#        Lists all Dockerfiles


set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

cd $LSTORE_RELEASE_BASE/scripts/docker/base

# Parse comand line
DISTROS=( "$@" )
if [ ${#DISTROS[@]} -eq 0 ]; then
    DISTROS=( */ )
fi
DISTROS=( "${DISTROS[@]%/}" )

for DISTRO in "${DISTROS[@]}"; do
    echo $DISTRO
done
