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
    mkdir -p repo/$PARENT/$RELEASE/packages
    find package/$DISTRO/ -name *.rpm | \
        xargs -I{} cp {} repo/$PARENT/$RELEASE/packages
    createrepo --retain-old-md 10 --deltas --num-deltas 5 -x '*-dirty.rpm' \
                repo/$PARENT/$RELEASE/
done
