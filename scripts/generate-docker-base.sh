#!/bin/bash

# Makes base images for package-building in docker
# Usage: ./generate-docker-base.sh [versions]
#    ie: ./generate-docker-base.sh
#        updates all Dockerfiles
#    or: ./generate-docker-base.sh centos-7
#        updates the centos-7 dockerfile
#    or: ./gengerate-docker-base.sh centos-newversion
#        makes a new folder and puts a fresh dockerfile inside

# Inspired by https://github.com/docker/docker/blob/master/contrib/builder/rpm/generate.sh

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

cd $LSTORE_RELEASE_BASE/scripts/docker/builder

AFL_VERSION="2.32b"

# Parse comand line
DISTROS=( "$@" )
if [ ${#DISTROS[@]} -eq 0 ]; then
    DISTROS=( */ )
fi
DISTROS=( "${DISTROS[@]%/}" )

for DISTRO in "${DISTROS[@]}"; do
    PARENT="${DISTRO%-*}"
    RELEASE="${DISTRO##*-}"
    FROM="${PARENT}:${RELEASE}"
    
    mkdir -p $DISTRO

    GLOBAL_INSTALL=""
    case $PARENT in
        centos|fedora)
            # Fedora claims: 
            # Yum command has been deprecated, redirecting to 
            #                   '/usr/bin/dnf groupinstall -y Development Tools'
            # Should I rewrite this again to include dnf as a different packager
            # When does dnf first exist?
            PACKAGER="rpm"
            PACKAGE_PREFIX="RUN yum install -y"
            PACKAGE_POSTFIX="&& yum clean all"
            JAVA_INSTALL=""
            if [ $PARENT == "centos" ]; then
                GLOBAL_INSTALL="RUN yum groupinstall -y 'Development Tools' && yum install -y epel-release git && yum clean all"
            else
                # Fedora includes epel-releease already
                GLOBAL_INSTALL="RUN yum groupinstall -y 'Development Tools' && yum clean all"
            fi
            ;;
        ubuntu|debian)
            PACKAGER="deb"
            PACKAGE_PREFIX="RUN apt-get update && apt-get install -y"
            PACKAGE_POSTFIX=" --no-install-recommends --no-upgrade && apt-get clean"
            GLOBAL_INSTALL="RUN apt-get update && apt-get install -y build-essential fakeroot devscripts git ca-certificates --no-install-recommends --no-upgrade && apt-get clean"
            JAVA_INSTALL="RUN apt-get update && apt-get install -y clang-tidy cppcheck openjdk-8-jdk-headless lcov gcovr python-sphinx doxygen --no-install-recommends --no-upgrade && apt-get clean && mkdir /tmp/afl && cd /tmp/afl && wget http://lcamtuf.coredump.cx/afl/releases/afl-${AFL_VERSION}.tgz && tar -xzf afl-${AFL_VERSION}.tgz && cd afl-${AFL_VERSION} && make install && cd / && rm -rf /tmp/afl"
            ;;
        *)
            fatal "Unrecognized base image type: ${PARENT}"
            ;;
    esac
    case $PACKAGER in
        rpm)
            ADDITIONAL_PACKAGES=(
                                    apr-devel
                                    apr-util-devel
                                    autoconf
                                    ccache
                                    curl
                                    createrepo
                                    czmq-devel
                                    expat-devel
                                    fuse-devel
                                    leveldb-devel
                                    libdb-dev
                                    libtool
                                    openssl-devel
                                    python
                                    rsync
                                    tar
                                    wget
                                    which
                                    zlib-devel
                                )
            ;;
        deb)
            ADDITIONAL_PACKAGES=(
                                    autoconf
                                    ca-certificates
                                    ccache
                                    cmake
                                    curl
                                    debhelper
                                    dpkg-dev
                                    git-buildpackage
                                    git-core
                                    libapr1-dev
                                    libaprutil1-dev
                                    libdb-dev
                                    libdistro-info-perl
                                    libexpat1-dev
                                    libfuse-dev
                                    libleveldb-dev
                                    libssl-dev
                                    libtool
                                    libz-dev
                                    libzmq3-dev
                                    lsb-release
                                    python
                                    rsync
                                    wget
                                )
            ;;
        *)
            fatal "Unrecognized packaging system: ${PACKAGER}"
    esac
    if [ "$DISTRO" == "ubuntu-xenial" ]; then
        ADDITIONAL_PACKAGES+=( clang
                               libczmq-dev
                             )
    fi
    case $RELEASE in
        vivid|wily|xenial|yakkety|jessie)
            ADDITIONAL_PACKAGES+=( libtool-bin )
            ;;
    esac
    if [ "${#ADDITIONAL_PACKAGES[0]}" -ne 0 ]; then
        PACKAGE_INSTALL=$PACKAGE_PREFIX
        for VAL in ${ADDITIONAL_PACKAGES[@]}; do
            PACKAGE_INSTALL="$PACKAGE_INSTALL $VAL"
        done
        PACKAGE_INSTALL="$PACKAGE_INSTALL $PACKAGE_POSTFIX"
    else
        PACKAGE_INSTALL=""
    fi
    OUT="$DISTRO/Dockerfile"
    cat > $OUT <<-EOF
#
# Autogenerated by lstore-release/scripts/generate-docker-base.sh
#
FROM $FROM
MAINTAINER http://lstore.org
$GLOBAL_INSTALL
RUN cd /tmp && \
    git clone https://github.com/Kitware/CMake.git && \
    cd CMake && \
    git checkout v3.5.2 && \
    ./bootstrap && \
    make -j16 && \
    make install && \
    cd .. && \
    rm -rf CMake
$PACKAGE_INSTALL
EOF
    BUILDSLAVE_DIR=$LSTORE_RELEASE_BASE/scripts/docker/buildslave/$DISTRO
    if [[ -d "$BUILDSLAVE_DIR" && ! -z "$JAVA_INSTALL" ]]; then
        cat > $BUILDSLAVE_DIR/Dockerfile <<-EOF
#
# Autogenerated by lstore-release/scripts/generate-docker-base.sh
#
FROM lstore/builder:$DISTRO
MAINTAINER http://lstore.org
$JAVA_INSTALL
EOF
    fi
done
