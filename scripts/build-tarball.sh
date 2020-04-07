#!/usr/bin/env bash

#  Make the various tarballs for use in creating LServer, depot, and client containers
#  These tarballs can also be used for manual installs if needed.

set -e
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

if [ "${1}" == "" ]; then
    echo "Must provide at least one tarball to create:"
    echo
    echo "lserver        - Build the tarball containing LServer scripts"
    echo "                 Base: ${LSTORE_RELEASE_BASE}/lserver/files"
    echo "lfs             - LFS tarball"
    echo "                 Base: ${LSTORE_RELEASE_BASE}/lfs"
    echo "-------Still todo are below------"
    echo "depot          - Build the tarball containing IBP depot scripts"
    echo "                 Base: ${LSTORE_RELEASE_BASE}/depot/files"
    echo
    echo "All tarballs are stored in ${LSTORE_TARBALL_ROOT}"
    echo
    exit 1
fi

function make_tarball() {
    NAME=${1}
    BASE=${2}

    DEST=${LSTORE_TARBALL_ROOT}
    [ "${3}" != "" ] && DEST=${3}


    echo "################################ ${NAME} ###########################################"
    [ ! -e ${DEST} ] && mkdir ${DEST}
    cd ${BASE}/files
    tar -cvzf ${DEST}/${NAME}.tgz .

    #Check if there are Site extenstions
    if [ -e ${BASE}/site ]; then
        cd ${BASE}/site
        tar -cvzf ${DEST}/${NAME}.site.tgz .
    fi
}

for target in $*; do
    if [ "${target}" == "lserver" ]; then
        make_tarball ${target} ${LSTORE_RELEASE_BASE}/lserver
        make_tarball osfile ${LSTORE_RELEASE_BASE}/lserver/samples/osfile ${LSTORE_RELEASE_BASE}/samples
    elif [ "${target}" == "lfs" ]; then
        make_tarball ${target} ${LSTORE_RELEASE_BASE}/lfs
    else
        echo "Unknown target: ${target}"
        exit 1
    fi
done
