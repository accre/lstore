#!/usr/bin/env bash
#
#  Builds the Jerasure Runtime libraries.  Should be run from inside the base directory
#
d=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local
cat ../../scripts/bootstrap | sed -e "s|PREFIX=/usr/local|PREFIX=${PREFIX}|" > bootstrap
./bootstrap ${1}

make $MAKE_ARGS
make $MAKE_ARGS install

