#!/usr/bin/env bash
#
#  Builds the Google gperftools libraries.  Should be run from inside the base directory
#

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PREFIX/lib
LDFLAGS="-L$PREFIX/lib" LIBS="-lunwind" CPPFLAGS="-I$PREFIX/include" ./configure --prefix=${PREFIX}${d}
make $MAKE_ARGS
make $MAKE_ARGS install
