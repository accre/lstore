#!/usr/bin/env bash
#
#  Builds the hwloc hardware locality libraries.  Should be run from inside the base directory
#

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

./configure --prefix=${PREFIX}${d} --enable-static --disable-shared
make $MAKE_ARGS
make $MAKE_ARGS install

