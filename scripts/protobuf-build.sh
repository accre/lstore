#!/usr/bin/env bash
#
#  Builds the Base Google protobuf compiler/libraries which support c++/Java/Python.  
#  Should be run from the base directory.
#
#  Dependencies:
#      Google protobuf must be installed
#

d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && export CXX="g++ -m32" d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

./configure --prefix=${PREFIX}${d} --disable-shared
make $MAKE_ARGS
make $MAKE_ARGS check
make $MAKE_ARGS install

