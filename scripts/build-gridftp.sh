#!/bin/bash
d=""

[ "${1}" == "" ] && echo "${0} /install/prefix" && exit 1

export PREFIX=${1}
export MAKE_ARGS="-j 10"

# need libxml2
cd build
[[ -d gridftp-lfs ]] || git clone https://github.com/PerilousApricot/gridftp-lfs.git
cd gridftp-lfs

[ -e CMakeCache.txt ] && rm CMakeCache.txt

if command -v cmake28 >/dev/null 2>&1; then
    LFS_CMAKE_COMMAND=cmake28
else
    LFS_CMAKE_COMMAND=cmake
fi

set -ex
CMAKE_PREFIX_PATH=${PREFIX}${d} \
   ${LFS_CMAKE_COMMAND} ${cmflags}  -D CMAKE_INSTALL_PREFIX=${PREFIX}${d} -D CMAKE_INCLUDE_CURRENT_DIR=on -D CMAKE_VERBOSE_MAKEFILE=on .
make
make install
