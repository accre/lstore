#!/usr/bin/env bash

# Script run inside docker by test-repo.sh
set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

DISTRO=$1
PARENT="${DISTRO%-*}"                                                          
RELEASE="${DISTRO##*-}"

case $DISTRO in
    centos-*)
        note "Attempting to use yum to install."
        REPO_BASE=$LSTORE_RELEASE_BASE/repo/$PARENT/$RELEASE/
        cat > /etc/yum.repos.d/lstore.repo <<-EOF
# Autogetnerated from lstore-release/scripts/rest-repo-internal.sh
[lstore]
name=LStore-\$releasever - LStore packages for \$basearch
baseurl=file://${REPO_BASE}
enabled=1
gpgcheck=0
protect=1
EOF
        yum install -y epel-release
        yum install -y accre-lio
        yum clean all
        ;;
    *)
        fatal "Unknown distro ${DISTRO}."
        ;;
esac

# TODO: Need a better smoke test. If you don't know you have a server up, there
#       there isn't much that can be done. At the very least --version needs to
#       be added to the command line args for the tools.
note "$(ls -l /usr/lib64)"
note "Attempting ldd against lio_ls."
LD_DEBUG=all lio_ls
note "Attempting to execute lio_ls."
LD_DEBUG=all lio_ls && [ $? -eq 1 ]
