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
            JAVA_INSTALL="RUN apt-get update && apt-get install -y clang clang-tidy cppcheck openjdk-8-jdk-headless lcov gcovr python-sphinx doxygen --no-install-recommends --no-upgrade && apt-get clean && mkdir /tmp/afl && cd /tmp/afl && wget http://lcamtuf.coredump.cx/afl/releases/afl-${AFL_VERSION}.tgz && tar -xzf afl-${AFL_VERSION}.tgz && cd afl-${AFL_VERSION} && make install && cd / && rm -rf /tmp/afl"
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
                                    zeromq3-devel
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
    if [ "$DISTRO" == "ubuntu-bionic" ]; then
        ADDITIONAL_PACKAGES+=( clang
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
ENV DEBIAN_FRONTEND=noninteractive
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
        cat > $BUILDSLAVE_DIR/jenkins_slave <<-'EOF'
#!/usr/bin/env sh

# The MIT License
#
#  Copyright (c) 2015, CloudBees, Inc.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.
echo "Args are $@"
set -x
set +e
sleep 10
echo "Args are $@"
# Usage jenkins-slave.sh [options] -url http://jenkins [SECRET] [AGENT_NAME]
# Optional environment variables :
# * JENKINS_TUNNEL : HOST:PORT for a tunnel to route TCP traffic to jenkins host, when jenkins can't be directly accessed over network
# * JENKINS_URL : alternate jenkins URL
# * JENKINS_SECRET : agent secret, if not set as an argument
# * JENKINS_AGENT_NAME : agent name, if not set as an argument
# * JENKINS_AGENT_WORKDIR : agent work directory, if not set by optional parameter -workDir

if [ $# -eq 1 ]; then

    # if `docker run` only has one arguments, we assume user is running alternate command like `bash` to inspect the image
    exec "$@"

else

    # if -tunnel is not provided try env vars
    case "$@" in
        *"-tunnel "*) ;;
        *)
        if [ ! -z "$JENKINS_TUNNEL" ]; then
            TUNNEL="-tunnel $JENKINS_TUNNEL"
        fi ;;
    esac

    # if -workDir is not provided try env vars
    if [ ! -z "$JENKINS_AGENT_WORKDIR" ]; then
        case "$@" in
            *"-workDir"*) echo "Warning: Work directory is defined twice in command-line arguments and the environment variable" ;;
            *)
            WORKDIR="-workDir $JENKINS_AGENT_WORKDIR" ;;
        esac
    fi

    if [ -n "$JENKINS_URL" ]; then
        URL="-url $JENKINS_URL"
    fi

    if [ -n "$JENKINS_NAME" ]; then
        JENKINS_AGENT_NAME="$JENKINS_NAME"
    fi  

    if [ -z "$JNLP_PROTOCOL_OPTS" ]; then
        echo "Warning: JnlpProtocol3 is disabled by default, use JNLP_PROTOCOL_OPTS to alter the behavior"
        JNLP_PROTOCOL_OPTS="-Dorg.jenkinsci.remoting.engine.JnlpProtocol3.disabled=true"
    fi

    # If both required options are defined, do not pass the parameters
    OPT_JENKINS_SECRET=""
    if [ -n "$JENKINS_SECRET" ]; then
        case "$@" in
            *"${JENKINS_SECRET}"*) echo "Warning: SECRET is defined twice in command-line arguments and the environment variable" ;;
            *)
            OPT_JENKINS_SECRET="${JENKINS_SECRET}" ;;
        esac
    fi
    
    OPT_JENKINS_AGENT_NAME=""
    if [ -n "$JENKINS_AGENT_NAME" ]; then
        case "$@" in
            *"${JENKINS_AGENT_NAME}"*) echo "Warning: AGENT_NAME is defined twice in command-line arguments and the environment variable" ;;
            *)
            OPT_JENKINS_AGENT_NAME="${JENKINS_AGENT_NAME}" ;;
        esac
    fi

    #TODO: Handle the case when the command-line and Environment variable contain different values.
    #It is fine it blows up for now since it should lead to an error anyway.
    
    exec java $JAVA_OPTS $JNLP_PROTOCOL_OPTS -cp /usr/share/jenkins/slave.jar hudson.remoting.jnlp.Main -headless $TUNNEL $URL $WORKDIR $OPT_JENKINS_SECRET $OPT_JENKINS_AGENT_NAME "$@"
fi

EOF
        cat > $BUILDSLAVE_DIR/Dockerfile <<-EOF
#
# Autogenerated by lstore-release/scripts/generate-docker-base.sh
#
FROM lstore/builder:$DISTRO
MAINTAINER http://lstore.org
$JAVA_INSTALL

# Get additional Jenkins slave things

ARG VERSION=3.16

RUN curl --create-dirs -sSLo /usr/share/jenkins/slave.jar https://repo.jenkins-ci.org/public/org/jenkins-ci/main/remoting/\${VERSION}/remoting-\${VERSION}.jar \
  && chmod 755 /usr/share/jenkins \
  && chmod 644 /usr/share/jenkins/slave.jar

ADD jenkins_slave /jenkins_slave
ENTRYPOINT ["/bin/sh", "/jenkins_slave"]
CMD ["-verbose"]
EOF
    fi
done
