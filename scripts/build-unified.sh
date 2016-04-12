#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

check_cmake
cmake . \
    -DCMAKE_INSTALL_PREFIX=$LSTORE_RELEASE_BASE/build/local \
    -DCMAKE_BINARY_DIR=$LSTORE_RELEASE_BASE
make

