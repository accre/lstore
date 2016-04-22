#!/usr/bin/env bash
set -eux
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
umask 0000

OPTIND=1
BASE_DIR=""
while getopts "v:d:" opt; do
    case $opt in
        d)
            BASE_DIR="$OPTARG"
            ;;
    esac
done
shift $((OPTIND-1))


cd $LSTORE_RELEASE_BASE
DISTRO=$1

if [ ! -e build/package/$DISTRO ]; then
    note "No binaries for distribution $DISTRO, skipping."
    exit 0
fi
set -o pipefail
PARENT="${DISTRO%-*}"
RELEASE="${DISTRO##*-}"
BASE_REPO_DIR="$BASE_DIR/$PARENT/$RELEASE"
if [ ! -z "${BASE_REPO_DIR}" ]; then
    mkdir -p "${BASE_REPO_DIR}"
fi
if [ -d "$BASE_REPO_DIR" ]; then
    mkdir -p build/repo/$PARENT
    cp -a "$BASE_REPO_DIR" build/repo/$PARENT
else
    mkdir -p build/repo/$PARENT/$RELEASE/packages
fi
case $PARENT in
    centos)
        find build/package/$DISTRO/ -name '*.rpm' | grep -v lstore-release.rpm | \
            xargs -I{} cp {} build/repo/$PARENT/$RELEASE/packages
        createrepo --retain-old-md 10 --deltas --num-deltas 5 -x '*-dev.rpm' \
                    build/repo/$PARENT/$RELEASE/
        if [[ $RELEASE -eq 6 && -e build/package/$DISTRO/lstore-release.rpm ]]; then
            cp build/package/$DISTRO/lstore-release.rpm build/repo/$PARENT
        fi
        ;;
    ubuntu|debian)
        find build/package/$DISTRO/ -name '*.deb' | \
            xargs -I{} cp {} build/repo/$PARENT/$RELEASE/packages
        pushd build/repo/$PARENT/$RELEASE/packages
        dpkg-scanpackages ./ | gzip >Packages.gz
        popd
        ;;
    *)
        fatal "Unrecognized distribution"
        ;;
esac
if [ -d "$BASE_REPO_DIR" ]; then
    rsync -avh build/repo/$PARENT/$RELEASE/ "$BASE_REPO_DIR"
fi
