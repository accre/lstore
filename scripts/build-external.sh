#!/usr/bin/env bash

[ "${1}" == "" ] && echo "${0} /install/prefix" && exit 1

export PREFIX=${1}
export MAKE_ARGS="-j 10"

cd build

for x in libunwind gperftools zlib openssl Jerasure apr apr-util zeromq czmq; do
    [ -d ${x}-[0-9]* ] || tar --no-same-owner --no-same-permissions -zxvf ../tarballs/${x}-[0-9]*gz
    cd ${x}-[0-9]*
    cat ../../scripts/${x}-build.sh | sed -e "s|PREFIX=/usr/local|PREFIX=${PREFIX}|" > my-build.sh
    chmod +x my-build.sh
    ./my-build.sh 2>&1 | tee ../../logs/${x}-build.log
    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        echo "Building ${x} failed"
        exit ${PIPESTATUS[0]}
    fi
    cd ..
done
