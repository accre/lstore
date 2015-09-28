#!/usr/bin/env bash

# To build RPMs, we have (basically) three kinds of packages
# 1) Externals that can/should come from system yum repositories
#        libunwind, gperftools, zlib, openssl, zeromq, czmq
# 2) Externals that must come from a private yum repo
#        libapr-ACCRE, libapr-util-ACCRE, Jerasure
# 3) Packages from ACCRE that must go to a private yum repo
#        gop, toolbox, IBP, LIO

# For building RPMS, we do *NOT* build case 1. Doing so makes RPM's automatic
# dependency solving logic go crazy. Ex: our privately-built openssl is called
# "libssl.so.1.0.0()(64bit)", but the one provided by yum is called
# "libssl.so.10()(64bit)". We assume that the build system has these installed

# run from base of lio-release
[ "${1}" == "" ] && echo "${0} /temporary/prefix [git user-id|tar]  **Default is to use tar**" && exit 1

from="tar"
if [ "${2}" == "git" ] ; then
   from=git
   [ "${3}" == "" ] && echo "Pulling project from GIT but missing user id!" && exit 1
   user=${3}
fi

export PREFIX=${1}

# probably needs a better test
for BADSO in libcrypto.so libczmq.so \
		libssl.so libzmq.so libz.so; do
    if [ -e $PREFIX/lib/${BADSO} ]; then
	echo "ERROR: The prefix shouldn't contain the output from"
	echo "       'build-external.sh'. This will cause your RPMS to have"
	echo "       unsolvable dependencies"
    fi
done

pushd build

# Case 2
sudo yum remove -y apr-ACCRE apr-util-ACCRE jerasure
set -x
mkdir -p RPM RPM/SOURCES
for x in apr-util; do
    case $x in
        apr-util)
            SOURCE="apr-util-1.5.3"
            TARGET="apr-util-ACCRE-1.5.3"
            PATCH="apr-util.patch"
            ;;
        Jerasure)
            SOURCE="Jerasure-1.2A"
            TARGET="Jerasure-1.2A"
            PATCH="jerasure.patch"
            ;;
        apr)
            SOURCE="apr-1.5.0"
            TARGET="apr-ACCRE-1.5.0"
            PATCH="apr.patch"
            ;;
        *)
            echo "ERROR: Undefined external: ${X}"
            exit 1
    esac
    if [[ "x$PATCH" != "x" && ! -e RPM/SOURCES/${x}-[0-9]* ]]; then
        rm -rf tarball-temp
        mkdir tarball-temp
        cd tarball-temp
        pwd
        tar --no-same-owner --no-same-permissions --exclude=CMakeCache.txt -zxf ../../tarballs/${x}-[0-9]*gz
        cd ${x}-[0-9]*
        pwd
        patch -p1 < ../../../tarballs/${PATCH}
        cd ..
        pwd
        tar --transform "s/${SOURCE}/${TARGET}/" -czf ../RPM/SOURCES/${TARGET}.tar.gz ${SOURCE}
        cd ..
        pwd
        rm -rf tarball-temp
    fi
    pushd RPM
    if [[ "x$2" != "x" ]]; then
        RELEASE=$2
        sed 's#Release.*##' ../../scripts/${x}.spec > ${x}.spec
    else
        RELEASE="UNTOUCHED"
        cp ../../scripts/${x}.spec ${x}.spec
    fi
    sudo rpmbuild \
            --define "_topdir `pwd`" \
            --define "release `date +%Y%m%d%H%M%S`" \
            -ba ${x}.spec | tee ../../logs-${x}.rpm.log
    EXIT_CODE=${PIPESTATUS[0]}
    if [ $EXIT_CODE -ne 0 ]; then
       echo "Failed to build ${x}: $EXIT_CODE"
       exit $EXIT_CODE
    fi
    sudo yum localinstall -y RPMS/*/$(echo ${x} | tr '[:upper:]' '[:lower:]')*
    popd
done

# Case 4
sudo yum remove -y accre-*
for p in toolbox gop ibp lio; do
  if [ ! -d ${p} ] ; then
     if [ "${from}" == "git" ]; then
        echo "Checking out repository.  May be asked for password..."
        git clone https://${user}@redmine.accre.vanderbilt.edu/git/${p}.git
     else
        tar -zxvf ../tarballs/${p}.tgz
     fi
  fi

  pushd ${p}
  ../../scripts/my-build.sh 2>&1 | tee ../../logs/${p}-rpm.log
  make package
  sudo yum localinstall -y accre-*.rpm
  popd
done
popd
