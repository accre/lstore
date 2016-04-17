#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

check_cmake
cd $LSTORE_RELEASE_BASE/build
cmake $LSTORE_RELEASE_BASE \
    -DCMAKE_INSTALL_PREFIX=$LSTORE_RELEASE_BASE/build/local
make

