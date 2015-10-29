#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh


cd $LSTORE_RELEASE_BASE
DISTRO=$1

if [ ! -e package/$DISTRO ]; then
    note "No binaries for distribution $DISTRO, skipping."
    exit 0
fi
PARENT="${DISTRO%-*}"
RELEASE="${DISTRO##*-}"
case $PARENT in
    centos)
        mkdir -p repo/$PARENT/$RELEASE/packages
        find package/$DISTRO/ -name *.rpm | \
            xargs -I{} cp {} repo/$PARENT/$RELEASE/packages
        createrepo --retain-old-md 10 --deltas --num-deltas 5 -x '*-dev.rpm' \
                    repo/$PARENT/$RELEASE/
        ;;
    ubuntu|debian)
        mkdir -p repo/$PARENT/$RELEASE/packages
        find package/$DISTRO/ -name *.deb | \
            xargs -I{} cp {} repo/$PARENT/$RELEASE/packages
        pushd repo/$PARENT/$RELEASE/
        dpkg-scanpackages packages | gzip >packages/Packages.gz
        cat >lstore-local-repo.list \
<<EOF
# Update the path if needed, stick this /etc/apt/sources.list.d/, and apt-get udpate
deb file:///$(pwd)/  packages/
EOF
	# note that the trailing slash after 'packages' is crucial otherwise that field is treated as a distro release name
        popd
        exit 0
        ;;
    *)
        fatal "Unrecognized distribution"
        ;;
esac
