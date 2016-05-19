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
    pushd $LSTORE_RELEASE_BASE/scripts/docker/builder
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

# Set up ccache to speed multiple compilations
if [[ -z "${CCACHE_DIR:-}" ]]; then
    CCACHE_DIR=/tmp/source/build/ccache
    mkdir -p $CCACHE_DIR
else
    if [[ -e "$CCACHE_DIR" ]]; then
        EXTRA_ARGS="$EXTRA_ARGS -v $CCACHE_DIR:$CCACHE_DIR"
    fi
fi
EXTRA_ARGS="$EXTRA_ARGS -e CCACHE_DIR=$CCACHE_DIR"

if [[ ! -z "${HOST_VOLUME_PATH:-}" && ! -z "${CONTAINER_VOLUME_PATH:-}" ]]; then
    LSTORE_RELEASE_RELATIVE="${HOST_VOLUME_PATH}/$(realpath $(pwd) --relative-to "$CONTAINER_VOLUME_PATH")"
    CCACHE_DIR_RELATIVE="${HOST_VOLUME_PATH}/$(realpath "$CCACHE_DIR" --relative-to "$CONTAINER_VOLUME_PATH")"
else
    LSTORE_RELEASE_RELATIVE="$LSTORE_RELEASE_BASE"
    CCACHE_DIR_RELATIVE="$CCACHE_DIR"
fi


for DISTRO in "${DISTROS[@]}"; do
    note "Starting docker container to package $DISTRO"
    case $DISTRO in
        centos*)
            INTERNAL_CMD="/tmp/source/scripts/package-internal.sh $DISTRO"
            ;;
        debian*|ubuntu*)
            INTERNAL_CMD="/tmp/source/scripts/package-internal-separate.sh $DISTRO"
            ;;
    esac
    set -x
    docker run --rm=true -v $LSTORE_RELEASE_RELATIVE:/tmp/source \
            $EXTRA_ARGS \
            lstore/builder:${DISTRO} \
            $INTERNAL_CMD
    set +x
done
