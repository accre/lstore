#!/usr/bin/env bash

# package.sh - Spin up docker, package sources into RPM/DEB/TGZ
# Usage: ./package.sh [versions]
#    ie: ./package.sh
#        Runs packaging for each supported distribution. The distributions
#        supported each have an entry in scripts/docker/base
#    or: ./package.sh centos-7
#        Runs packaging for centos-7


set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
PACKAGE_EPOCH=$(date +%F-%H-%M-%S)
DISTROS=( "$@" )
if [ ${#DISTROS[@]} -eq 0 ]; then
    pushd $LSTORE_RELEASE_BASE/scripts/docker/base
    DISTROS=( */ )
    popd
fi
DISTROS=( "${DISTROS[@]%/}" )


cd $LSTORE_RELEASE_BASE
EXTRA_ARGS=""

# if the host has proxies set, use them to try and decrease the download time
for PROXY in http_proxy HTTPS_PROXY; do
    if [ ! -z "${!PROXY+x}" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -e ${PROXY}=${!PROXY} "
    fi
done

for DISTRO in "${DISTROS[@]}"; do
    note "Starting docker container to package $DISTRO"
    set -x
    docker run --rm=true -v $(pwd):/tmp/source \
            $EXTRA_ARGS \
            lstore/builder:${DISTRO} \
            /tmp/source/scripts/package-internal.sh $DISTRO
    set +x
done
