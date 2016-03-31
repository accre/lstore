#!/usr/bin/env bash

# test-repo.sh - Spin up docker, install lstore from local repository
# Usage: ./test-repo.sh [distributions]
#    ie: ./test-repo.sh
#        Runs tests for each supported distribution. The distributions
#        supported each have an entry in scripts/docker/base
#    or: ./test-repo.sh centos-7
#        Runs tests for centos-7


set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

DISTROS=( "$@" )
if [ ${#DISTROS[@]} -eq 0 ]; then
    pushd $LSTORE_RELEASE_BASE/scripts/docker/base
    DISTROS=( */ )
    popd
fi
DISTROS=( "${DISTROS[@]%/}" )


cd $LSTORE_RELEASE_BASE
EXTRA_ARGS=""
# if the host has proxies set, use them to try and decrease the download
#    time
for PROXY in http_proxy HTTPS_PROXY; do
    if [ ! -z "${!PROXY+x}" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -e ${PROXY}=${!PROXY} "
    fi
done

if [[ ! -z "${HOST_VOLUME_PATH:-}" && ! -z "${CONTAINER_VOLUME_PATH:-}" ]]; then
    LSTORE_RELEASE_RELATIVE="${HOST_VOLUME_PATH}/$(realpath $(pwd) --relative-to "$CONTAINER_VOLUME_PATH")"
else
    LSTORE_RELEASE_RELATIVE="$LSTORE_RELEASE_BASE"
fi


for DISTRO in "${DISTROS[@]}"; do
    PARENT="${DISTRO%-*}"
    RELEASE="${DISTRO##*-}"
    BARE_DISTRO_IMAGE="${PARENT}:${RELEASE}"
    note "Starting docker container to test $DISTRO"
    set -x
    docker run --rm=true -v $LSTORE_RELEASE_RELATIVE:/tmp/source \
            $EXTRA_ARGS \
            $BARE_DISTRO_IMAGE \
            /tmp/source/scripts/test-repo-internal.sh $DISTRO
    set +x
done
