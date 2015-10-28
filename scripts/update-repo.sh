#!/usr/bin/env bash
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

for DISTRO in "${DISTROS[@]}"; do
    if [ ! -e package/$DISTRO ]; then
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
            BARE_DISTRO_IMAGE=debian-jessie
            ;;
        *)
            fatal "Unsupported distro"
            ;;
    esac
    mkdir -p repo/$PARENT/$RELEASE/packages
    note "Starting docker container to update $DISTRO"
    set -x
    docker run --rm=true -v $(pwd):/tmp/source \
            lstore/builder:$BARE_DISTRO_IMAGE \
            /tmp/source/scripts/update-repo-internal.sh $DISTRO
    set +x
done
