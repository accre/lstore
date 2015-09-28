#!/usr/bin/env bash
#
#  Builds the ZeroMQ Runtime libraries.  Should be run from inside the base directory

#  ***NOTE: Need uuid-dev installed***

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

./configure --prefix=${PREFIX}${d} --enable-static --enable-shared --with-libzmq=${PREFIX} 
make $MAKE_ARGS
make $MAKE_ARGS install

