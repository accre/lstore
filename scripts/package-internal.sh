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
    ;;
centos-*)
    PACKAGE_INSTALL="rpm -i"
    PACKAGE_SUFFIX=rpm
    ;;
*)
    fatal "Unexpected distro name $DISTRO_NAME"
    ;;
esac

# todo could probe this from docker variables
PACKAGE_BASE="/tmp/lstore-release"
REPO_BASE=$LSTORE_RELEASE_BASE/build/package/$PACKAGE_SUBDIR
# Here and elsewhere, we need to set the umask when we write to the host-mounted
#    paths. Otherwise users outside the container can't read/write files. But,
#    we don't want to just blindly set umask 0000, in case there's some
#    weirdness with how the installer works.
(
    umask 000
    mkdir -p $PACKAGE_BASE/{build,cmake} $REPO_BASE
    # That being said, umask appears to not work right...
    # FIXME: Why is umask so sad?
    chmod 777 $PACKAGE_BASE $PACKAGE_BASE/{build,cmake} $REPO_BASE
)
check_cmake $PACKAGE_BASE/cmake
note "Beginning packaging at $(date) for $PACKAGE_SUBDIR"

#
# Get weird. Build each package, then install the RPM. If this is 
#   configurable in the future, the order of packages matters.
#
cd $PACKAGE_BASE/build
for PACKAGE in apr-accre apr-util-accre jerasure czmq \
               toolbox gop ibp lio release meta; do
    SOURCE_BASE=$(get_repo_source_path ${PACKAGE})
    if [ "$PACKAGE" == "czmq" ];then
        if (ldconfig -p | grep -q libczmq); then
                echo "libczmq.so is available: skipping czmq package build.";
                continue
        else
                echo "libczmq.so not found, building a czmq package...";
        fi
    fi

    (
        umask 000
        mkdir -p $PACKAGE
        # See above.
        chmod 777 $PACKAGE
    )
    pushd $PACKAGE
    # NOTE: Can't do this with --git-dir. Git 1.8.3.1 considers the tag "dirty"
    #       if the CURRENT working directory is dirty...
    # NOTE: The git update-index is needed since the host and container git
    #       versions might be different.
    TAG_NAME=$(cd $SOURCE_BASE &&
                ( git update-index -q --refresh &>/dev/null || true ) && \
                git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                    --match 'ACCRE_*' | sed 's,^ACCRE_,,')
    if [ -z "$TAG_NAME" ]; then
        TAG_NAME="0.0.0-$(cd $PACKAGE_SOURCE &&
                ( git update-index -q --refresh &>/dev/null || true ) && \
                git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                    --match ROOT --always)"
    fi

    TAG_NAME=${TAG_NAME:-"0.0.0-undefined-tag"}
    (cd $SOURCE_BASE && note "$(git status)")
    PACKAGE_REPO=$REPO_BASE/$PACKAGE/$TAG_NAME
    if [ ! -e $PACKAGE_REPO ]; then
        set -x
        build_lstore_package $PACKAGE $SOURCE_BASE $TAG_NAME \
                             $PACKAGE_DISTRO
        mkdir -p $PACKAGE_REPO
        # Need to figure out what to do with these eventually.
        rm *.source.rpm || true
        (
            umask 000
            cp *.${PACKAGE_SUFFIX} $PACKAGE_REPO
            chmod 666 $PACKAGE_REPO/*
            # Update lstore-release if we built it
            if test -n "$(shopt -s nullglob; set +u; echo lstore-release*.rpm)"; then
                cp lstore-release*.rpm $REPO_BASE/lstore-release.rpm
            fi
        )
        set +x 
    else
        note "Tag $TAG_NAME of $PACKAGE already exists in:"
        note "    ${PACKAGE_REPO}"
        note "    Instead of building, we are installing from there."
    fi
    $PACKAGE_INSTALL $REPO_BASE/$PACKAGE/$TAG_NAME/*.${PACKAGE_SUFFIX}
    popd 
done

note "Done! The new packages can be found in ./package/$PACKAGE_SUBDIR"
