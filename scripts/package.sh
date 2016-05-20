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

#
# Argument parsing
#
PACKAGE_ARGS=""
while getopts ":c:ht" opt; do
    case $opt in
        c)
            PACKAGE_ARGS="$PACKAGE_ARGS -c $OPTARG"
            ;;
        t)
            PACKAGE_ARGS="$PACKAGE_ARGS -t"
            ;;
        \?|h)
            1>&2 echo "Usage: $0 [-t] [-c ARGUMENT] [distribution ...]"
            1>&2 echo "       -t: Produce static tarballs only"
            1>&2 echo "       -c: Add ARGUMENT to cmake"
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

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
    CCACHE_DEFAULT=1
    CCACHE_DIR=/tmp/source/build/ccache
    mkdir -p $CCACHE_DIR
fi

if [[ ! -z "${HOST_VOLUME_PATH:-}" && ! -z "${CONTAINER_VOLUME_PATH:-}" ]]; then
    LSTORE_RELEASE_RELATIVE="${HOST_VOLUME_PATH}/$(realpath $(pwd) --relative-to "$CONTAINER_VOLUME_PATH")"
    CCACHE_DIR_RELATIVE="${HOST_VOLUME_PATH}/$(realpath "$CCACHE_DIR" --relative-to "$CONTAINER_VOLUME_PATH")"
else
    LSTORE_RELEASE_RELATIVE="$LSTORE_RELEASE_BASE"
    CCACHE_DIR_RELATIVE="$CCACHE_DIR"
fi

note "ccache default: ${CCACHE_DEFAULT:-} ccache_dir $CCACHE_DIR"
if [[ -z "${CCACHE_DEFAULT:-}" && -d "$CCACHE_DIR" ]]; then
    EXTRA_ARGS="$EXTRA_ARGS -e CCACHE_DIR=/tmp/ccache"
    EXTRA_ARGS="$EXTRA_ARGS -v $CCACHE_DIR_RELATIVE:/tmp/ccache"
else
    EXTRA_ARGS="$EXTRA_ARGS -e CCACHE_DIR=$CCACHE_DIR"
fi

for DISTRO in "${DISTROS[@]}"; do
    note "Starting docker container to package $DISTRO"
    set -x
    docker run --rm=true -v $LSTORE_RELEASE_RELATIVE:/tmp/source \
            $EXTRA_ARGS \
            lstore/builder:${DISTRO} \
            /tmp/source/scripts/package-internal.sh $PACKAGE_ARGS $DISTRO
    set +x
done
