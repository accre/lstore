#!/bin/bash

#
#  package-internal.sh - Runs within docker container to package LStore
#

#
# Preliminary bootstrapping
#

set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
PACKAGE_DISTRO=${1:-unknown_distro}
PACKAGE_SUBDIR=$PACKAGE_DISTRO
umask 0000

case $PACKAGE_DISTRO in
undefined)
	# TODO Fail gracefully
    ;;
ubuntu-*|debian-*)
    # switch to gdebi if automatic dependency resolution is needed
    PACKAGE_INSTALL="dpkg -i"        
    PACKAGE_SUFFIX=deb
    CMAKE_ARGS="-DINSTALL_DEB_RELEASE=ON"
    ;;
centos-*)
    PACKAGE_INSTALL="rpm -i"
    PACKAGE_SUFFIX=rpm
    CMAKE_ARGS="-DINSTALL_YUM_RELEASE=ON"
    ;;
*)
    fatal "Unexpected distro name $DISTRO_NAME"
    ;;
esac

# todo could probe this from docker variables
REPO_BASE=$LSTORE_RELEASE_BASE/build/package/$PACKAGE_SUBDIR
PACKAGE_BASE=/tmp/lstore-package

note "Beginning packaging at $(date) for $PACKAGE_SUBDIR"

TAG_NAME=$(cd $LSTORE_RELEASE_BASE &&
            ( git update-index -q --refresh &>/dev/null || true ) && \
            git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                --match 'v*' | sed 's,^v,,')
if [ -z "$TAG_NAME" ]; then
    TAG_NAME="0.0.0-$(cd $LSTORE_RELEASE_BASE &&
            ( git update-index -q --refresh &>/dev/null || true ) && \
            git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                --match ROOT --always)"
fi

TAG_NAME=${TAG_NAME:-"0.0.0-undefined-tag"}

(cd $LSTORE_RELEASE_BASE && note "$(git status)")
PACKAGE_REPO=$REPO_BASE/$TAG_NAME

set -x
mkdir -p $PACKAGE_BASE/build
cp -r ${LSTORE_RELEASE_BASE}/{scripts,src,vendor,doc,debian,test,cmake,CMakeLists.txt,lstore.spec} \
        $PACKAGE_BASE

if [[ $PACKAGE_SUFFIX == deb ]]; then
    cd $PACKAGE_BASE
    dpkg-buildpackage -uc -us

(
    umask 000
    mkdir -p $PACKAGE_REPO
    cp -r ../lstore*.{deb,tar.*z,changes} $PACKAGE_REPO
    chmod -R u=rwX,g=rwX,o=rwX $PACKAGE_REPO/*
    # Update lstore-release if we built it
    if test -n "$(shopt -s nullglob; set +u; echo lstore-release*.deb)"; then
        cp lstore-release*.deb $REPO_BASE/lstore-release.deb
    fi
)

else
    cd $PACKAGE_BASE/build
    cmake ..
    make $PACKAGE_SUFFIX VERBOSE=1
(
    umask 000
    mkdir -p $PACKAGE_REPO
    cp -r {,s}rpm_output/ $PACKAGE_REPO
    chmod -R u=rwX,g=rwX,o=rwX $PACKAGE_REPO/*
    # Update lstore-release if we built it
    if test -n "$(shopt -s nullglob; set +u; echo lstore-release*.rpm)"; then
        cp lstore-release*.rpm $REPO_BASE/lstore-release.rpm
    fi
)
fi

set +x 

note "Done! The new packages can be found in ./build/package/$PACKAGE_SUBDIR"
