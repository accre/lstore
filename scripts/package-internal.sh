#!/bin/bash

#
#  package-internal.sh - Runs within docker container to package LStore
#

#
# Preliminary bootstrapping
#

set -eux
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
umask 0000
echo "Parsing $@"
#
# Argument parsing
#
TARBALL=0
CMAKE_ARGS=""
while getopts ":c:ht" opt; do
    case $opt in
        c)
            CMAKE_ARGS="$CMAKE_ARGS $OPTARG"
            ;;
        t)
            TARBALL=1
            ;;
        \?|h)
            1>&2 echo "OPTARG: ${OPTARG:-}"
            1>&2 echo "Usage: $0 [-t] [-c ARGUMENT] [distribution ...]"
            1>&2 echo "       -t: Produce static tarballs only"
            1>&2 echo "       -c: Add ARGUMENT to cmake"
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))
PACKAGE_DISTRO=${1:-unknown_distro}
PACKAGE_SUBDIR=$PACKAGE_DISTRO

if [[ $TARBALL -eq 0 ]]; then
    case $PACKAGE_DISTRO in
    undefined)
        # TODO Fail gracefully
        ;;
    ubuntu-*|debian-*)
        # switch to gdebi if automatic dependency resolution is needed
        PACKAGE_INSTALL="dpkg -i"
        PACKAGE_SUFFIX=deb
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_DEB_RELEASE=ON"
        ;;
    centos-*)
        PACKAGE_INSTALL="rpm -i"
        PACKAGE_SUFFIX=rpm
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_YUM_RELEASE=ON"
        ;;
    *)
        fatal "Unexpected distro name $PACKAGE_DISTRO"
        ;;
    esac
fi

# todo could probe this from docker variables
REPO_BASE=$LSTORE_RELEASE_BASE/build/package/$PACKAGE_SUBDIR
PACKAGE_BASE=/tmp/lstore-package

note "Beginning packaging at $(date) for $PACKAGE_SUBDIR"

TAG_NAME="$(cd $LSTORE_RELEASE_BASE && git describe --match 'v*' --exact-match 2>/dev/null || true)"

if [ ! -z "$TAG_NAME" ]; then
    IS_RELEASE=1
else
    IS_RELEASE=0
fi
if [ -z "$TAG_NAME" ]; then
    TAG_NAME="$(cd $LSTORE_RELEASE_BASE &&
                ( git update-index -q --refresh &>/dev/null || true ) && \
                git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                    --match 'v*' | sed 's,^v,,' || true)"
fi
if [ -z "$TAG_NAME" ]; then
    TAG_NAME="0.0.0-$(cd $LSTORE_RELEASE_BASE &&
            ( git update-index -q --refresh &>/dev/null || true ) && \
            git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                --match ROOT --always || true)"
fi

TAG_NAME=${TAG_NAME:-"0.0.0-undefined-tag"}

(cd $LSTORE_RELEASE_BASE && note "$(git status)")
PACKAGE_REPO=$REPO_BASE/$TAG_NAME

set -x
mkdir -p $PACKAGE_BASE/build
cp -r ${LSTORE_RELEASE_BASE}/{scripts,src,vendor,doc,debian,test,cmake,CMakeLists.txt,lstore.spec,VERSION} \
        $PACKAGE_BASE
ln -s ${LSTORE_RELEASE_BASE}/.git $PACKAGE_BASE/.git

if [[ "${TARBALL:-}" -eq 1 ]]; then
    cd $PACKAGE_BASE/build
    cmake $CMAKE_ARGS ..
    make package
    ls -lah
(
    umask 0000
    mkdir -p $PACKAGE_REPO
    cp *gz $PACKAGE_REPO
)
elif [[ $PACKAGE_SUFFIX == deb ]]; then
    cd $PACKAGE_BASE
    DISTANCE=$(git describe --match 'v*' --long | awk -F '-' '{ print $2 }')
    # Attempt to automatically bump the debian version
    if [ $IS_RELEASE -eq 1 ]; then
        gbp dch --auto --ignore-branch --id-length=8
    else
        gbp dch --auto --snapshot --snapshot-number "$DISTANCE" \
                    --ignore-branch --id-length=8
    fi
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
