#!/usr/bin/env bash
#
#  Builds the APR Apache Portable Runtime  libraries.  Should be run from inside the base directory
#

d=""
PKG=""
[ "${1}" == "32" ] && export CC="gcc -m32" && d="32" && echo "Making 32-bit binaries" && shift
[ "${1}" == "-D" ] && export PKG="-D ${PKG}" && shift
[ "${1}" == "-R" ] && export PKG="-R ${PKG}" && shift
[ "${1}" == "-S" ] && export PKG="-S ${PKG}" && shift

if [ "${1}" == "-h" ] ; then
   echo "$0 [-D|-R|-S] [32]"
   echo "  -D  Build Debian packages"
   echo "  -R  Build RPM packages"
   echo "  -S  Build Slackware packages"
   echo "  32  Make 32-bit binaries"
   exit
fi

[ "${PREFIX}" == "" ] && PREFIX=/usr/local


if [ "${PKG}" == "" ]; then  #Just building if undefined
   #Apply the patch
   patch -p1 < ../../tarballs/apr.patch
   if [[ $? -ne 0 ]]; then
       grep apr-ACCRE configure >/dev/null 2>&1
       if [[ $? -eq 0 ]]; then
           echo "Patch failed, but it might've been applied before, ignoring."
       else
           echo "Patch failed"
           exit 1
       fi
   fi

# Force sequential make build for this module. The make task dependencies are not fully defined so there are
# out-of-order task completion race conditions that lead to errors when built in parallel.
# (at least as of 2015 July 9 on my 4-core AMD w/ -j 10 --MFH)
MAKE_ARGS="$(sed 's/-j [0-9]*/-j 1/' <<<$MAKE_ARGS)"

   ./configure --prefix=${PREFIX}${d} --enable-static --enable-shared --includedir=${PREFIX}${d}/include/apr-ACCRE-1 && \
   make $MAKE_ARGS && \
   make $MAKE_ARGS test && \
   make $MAKE_ARGS install
else #Building the packages
  VERSION=$(pwd | tr "-" "\n" | tail -n 1)
  ./configure --prefix=${PREFIX}${d} --enable-static --enable-shared --includedir=${PREFIX}${d}/include/apr-ACCRE-1
  for p in ${PKG}; do
    echo "Making ${p}"
    rm apr-*.spec >& /dev/null
    checkinstall.new ${p} --pkgname apr-accre --pkgversion ${VERSION} --pkglicense "Apache License, Version 2.0" \
        --maintainer "tacketar@accre.vanderbilt.edu" -y make install
  done
fi
