#!/usr/bin/env bash
#
#  Builds the OpenSSL libraries.  Should be run from the base directory.
#
#  Dependencies:
#

d=""
target=""
[ "${1}" == "32" ] && export CC="gcc -m32" && export CXX="g++ -m32" && d="32" && \
   export target="linux-generic32" && export CFLAGS="-m32" && echo "Making 32-bit binaries"

[ "${PREFIX}" == "" ] && PREFIX=/usr/local

LDFLAGS="$LDFLAGS -L${PREFIX}${d}/lib" CPPFLAGS="$CPPFLAGS -I${PREFIX}${d}/include" ./config ${target} threads shared -fPIC --prefix=${PREFIX}${d}

# force sequential make build for this module, the make task dependencies are not fully defined so there are
# out-of-order task completion race conditions that lead to errors when built in parallel
# This is a well known issue with the upstream openssl source, although many distros use various workaround patches downstream.
# see http://openssl.6102.n7.nabble.com/parallel-make-broken-td45613.html http://stackoverflow.com/questions/28639207/why-cant-i-compile-openssl-with-multiple-threads-make-j3 https://github.com/openssl/openssl/issues/298 
MAKE_ARGS="$(sed 's/-j [0-9]*/-j 1/' <<<$MAKE_ARGS)"

make $MAKE_ARGS
make $MAKE_ARGS test
make $MAKE_ARGS install

