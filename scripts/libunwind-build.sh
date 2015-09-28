#!/usr/bin/env bash
#
#  Builds the libunwind libraries.  Should be run from inside the base directory
#

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

./configure --prefix=${PREFIX}${d}
make $MAKE_ARGS
make $MAKE_ARGS install
