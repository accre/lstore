#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

OPTIND=1
VOLUME_FROM=""
BASE_DIR=""
while getopts "v:d:" opt; do
    case $opt in
        v)
            VOLUME_FROM="--volumes-from $OPTARG"
            ;;
        d)
            BASE_DIR="-d $OPTARG"
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

if [[ ! -z "${HOST_VOLUME_PATH:-}" && ! -z "${CONTAINER_VOLUME_PATH:-}" ]]; then
    LSTORE_RELEASE_RELATIVE="${HOST_VOLUME_PATH}/$(realpath $(pwd) --relative-to "$CONTAINER_VOLUME_PATH")"
else
    LSTORE_RELEASE_RELATIVE="$LSTORE_RELEASE_BASE"
fi

cd $LSTORE_RELEASE_BASE

for DISTRO in "${DISTROS[@]}"; do
    if [ ! -e build/package/$DISTRO ]; then
        note "No binaries for distribution $DISTRO, skipping."
        continue
    fi

    PARENT="${DISTRO%-*}"
    RELEASE="${DISTRO##*-}"
    case $PARENT in
        centos)
            BARE_DISTRO_IMAGE=centos-7
            ;;
        ubuntu|debian)
            BARE_DISTRO_IMAGE=ubuntu-xenial
            ;;
        *)
            fatal "Unsupported distro"
            ;;
    esac
    mkdir -p build/repo/$PARENT/$RELEASE/packages
    note "Starting docker container to update $DISTRO"
    set -x
    docker run --rm=true -v $LSTORE_RELEASE_RELATIVE:/tmp/source \
            $VOLUME_FROM lstore/builder:$BARE_DISTRO_IMAGE \
            /tmp/source/scripts/update-repo-internal.sh $BASE_DIR $DISTRO
    set +x
done
