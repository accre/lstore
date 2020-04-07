#!/usr/bin/env bash

#  Bundles up all the various bits needed for builingthe LServer docker container

if [ "$1" == "" ]; then
    echo "$0 [--quick | --quick-deb | --quick-mix-deb deb-distro] package_dir deb-distro"
    echo "--quick          Skip building everything.  Just use what's currently available"
    echo "--quick-deb      Just skip building the deb packages and use what exists"
    echo "--quick-mix-deb  Just skip building the deb packages and use what exists from the specified distro"
    echo "package_dir      Location to store container files"
    echo "deb-distro       Debian package distro to use"
    exit 1
fi

set -e
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

#Parse the quick options
enable_quick=0
skip_deb=0
DEB_DISTRO=

if [ "${1}" == "--quick" ]; then
    enable_quick=1
    skip_deb=1
    shift
fi
if [ "${1}" == "--quick-deb" ]; then
    skip_deb=1
    shift
fi

if [ "${1}" == "--quick-mix-deb" ]; then
    skip_deb=1
    shift
    DEB_DISTRO=${1}
    shift
fi

#I just have the fixed arguments left
PDIR=$( realpath ${1})
DISTRO=${2}

[ "${DEB_DISTRO}" == "" ] && DEB_DISTRO=${DISTRO}

#Make the output directory if needed
[ ! -e ${PDIR} ] && mkdir ${PDIR}
[ ! -e ${PDIR}/tarballs ] && mkdir ${PDIR}/tarballs
[ ! -e ${PDIR}/samples ] && mkdir ${PDIR}/samples
[ ! -e ${PDIR}/install ] && mkdir ${PDIR}/install
[ ! -e ${PDIR}/repo ] && mkdir ${PDIR}/repo
[ ! -e ${PDIR}/repo/packages ] && mkdir ${PDIR}/repo/packages

DEBDIR=${LSTORE_RELEASE_BASE}/build/package/${DEB_DISTRO}
# Verify the DEB distro exists
if [ ! -e ${DEBDIR} ]; then
    echo "${DISTRO} doesn't exist!"
    echo "Available options:"
    ls ${LSTORE_RELEASE_BASE}/build/package/
    exit 1
fi

#Make the deb package
if [ "${skip_deb}" != "1" ]; then
    ${LSTORE_SCRIPT_BASE}/package.sh ${DISTRO}
fi

#Copy it
ddir=${DEBDIR}/$(ls -t ${DEBDIR}/ | head -n 1)
#cp ${ddir}/lstore_*.deb ${ddir}/lstore-dbgsym*.deb ${PDIR}/repo/packages
cp ${ddir}/lstore_*.deb ${PDIR}/repo/packages
cd ${PDIR}/repo
dpkg-scanpackages packages . | gzip -9c > packages/Packages.gz

#Copy all the install scripts
cp -a ${LSTORE_RELEASE_BASE}/lserver/install/* ${PDIR}/install || echo "No LServer install scripts"
cp -a ${LSTORE_RELEASE_BASE}/lfs/install/* ${PDIR}/install || echo "No LFS install scripts"

#And make the run script
cd ${PDIR}/install
echo '#!/usr/bin/env bash' > ${PDIR}/install/run-install.sh
echo 'PATH="/install:${PATH}"' >> ${PDIR}/install/run-install.sh
ls *.sh | grep -v run-install.sh >> ${PDIR}/install/run-install.sh
chmod +x ${PDIR}/install/run-install.sh

#Make the lmgmt sdist and copy it
${LSTORE_SCRIPT_BASE}/package-lmgmt.sh
cp ${LSTORE_RELEASE_BASE}/lserver/lmgmt/dist/lmgmt*.tar.gz ${PDIR}/tarballs
cp ${LSTORE_RELEASE_BASE}/lserver/lmgmt/lmgmt.py ${PDIR}/tarballs

#Make all the tarballs
${LSTORE_SCRIPT_BASE}/build-tarball.sh lserver lfs

#and copy them along with the LServer DockerFile
cp ${LSTORE_TARBALL_ROOT}/{lserver,lfs}*.tgz ${PDIR}/tarballs
cp ${LSTORE_RELEASE_BASE}/samples/osfile*.tgz ${PDIR}/samples
cp ${LSTORE_RELEASE_BASE}/lserver/docker/* ${PDIR}/

#Get the base image
base=$(echo ${DISTRO} | sed 's/-/:/g')
echo "FROM ${base}" > ${PDIR}/Dockerfile
cat ${PDIR}/Dockerfile.base >> ${PDIR}/Dockerfile
