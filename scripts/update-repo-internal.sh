#!/usr/bin/env bash
set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
umask 0000

cd $LSTORE_RELEASE_BASE
DISTRO=$1

if [ ! -e build/package/$DISTRO ]; then
    note "No binaries for distribution $DISTRO, skipping."
    exit 0
fi
set -o pipefail
PARENT="${DISTRO%-*}"
RELEASE="${DISTRO##*-}"
case $PARENT in
    centos)
        mkdir -p build/repo/$PARENT/$RELEASE/packages
        find build/package/$DISTRO/ -name '*.rpm' | grep -v lstore-release.rpm | \
            xargs -I{} cp {} build/repo/$PARENT/$RELEASE/packages
        createrepo --retain-old-md 10 --deltas --num-deltas 5 -x '*-dev.rpm' \
                    build/repo/$PARENT/$RELEASE/
        if [[ $RELEASE -eq 6 && -e build/package/$DISTRO/lstore-release.rpm ]]; then
            cp build/package/$DISTRO/lstore-release.rpm build/repo/$PARENT
        fi
        ;;
    ubuntu|debian)
        mkdir -p build/repo/$PARENT/$RELEASE/packages
        find build/package/$DISTRO/ -name '*.deb' | \
            xargs -I{} cp {} build/repo/$PARENT/$RELEASE/packages
        pushd build/repo/$PARENT/$RELEASE/packages
        dpkg-scanpackages ./ | gzip >Packages.gz
        popd
        exit 0
        ;;
    *)
        fatal "Unrecognized distribution"
        ;;
esac
