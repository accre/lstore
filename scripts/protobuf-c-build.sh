#!/usr/bin/env bash
#
#  Builds the prtobuf-c compiler/libraries.  Should be run from the base directory.
#
#  Dependencies:
#      Google protobuf must be installed
#

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && export CXX="g++ -m32" d="32" && echo "Making 32-bit binaries"


[ "${PREFIX}" == "" ] && PREFIX=/usr/local

CXXFLAGS="-I${PREFIX}${d}/include" \
LDFLAGS="-L${PREFIX}${d}/lib -lpthread" \
./configure --prefix=${PREFIX}${d} --disable-shared
make $MAKE_ARGS
make $MAKE_ARGS install

