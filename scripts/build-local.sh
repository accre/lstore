#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

cd $LSTORE_RELEASE_BASE/build

PREFIX=$LSTORE_RELEASE_BASE/local

# TODO: Should make sure deps are installed first. On Centos7, you need
#     to install the "Development Tools" group, and openssl-devel, zlib-devel,
#     czmq-devel, zmq-devel and fuse-devel
for p in toolbox gop ibp lio; do
    get_lstore_source ${p}
done

for p in toolbox gop ibp lio; do
    BUILT_FLAG="${PREFIX}/built-${p}"
    if [ -e $BUILT_FLAG ]; then
        note "Not building ${p}, was already built. To change this behavior,"
        note "    remove $BUILT_FLAG"
        continue
    fi
    pushd ${p}
    build_lstore_package ${p} ${PREFIX} 2>&1 | tee $LSTORE_RELEASE_BASE/logs/${p}-build.log
    [ ${PIPESTATUS[0]} -eq 0 ] || fatal "Could not build ${p}"
    touch $BUILT_FLAG
    popd
done
