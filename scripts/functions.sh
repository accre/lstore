# Helper functions for lstore-release

#
# Globals
#
LSTORE_SCRIPT_BASE=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
LSTORE_RELEASE_BASE=$(cd $(dirname "${LSTORE_SCRIPT_BASE}") && pwd)
LSTORE_HEAD_BRANCHES="apr-accre=accre-fork
                       apr-util-accre=accre-fork
                       jerasure=v1
                       lio=master
                       gop=redmine_pre-alok
                       toolbox=master
                       ibp=master"
#
# Informational messages
#
function lstore_message() {
    MESSAGE_TYPE=$1
    shift
    echo "$@" | >&2 sed -e "s,^,$MESSAGE_TYPE: ,g"
}
function fatal() {
    lstore_message FATAL "$@"
    exit 1
}
function note() {
    lstore_message NOTE "$@"
}

#
# Manipulating local repositories
#
function get_lstore_source() {
    TO_GET=$1
    BRANCH=""
    for VAL in $LSTORE_HEAD_BRANCHES; do
        if [[ $VAL == ${TO_GET}=* ]]; then
            BRANCH="${VAL#*=}"
        fi
    done
    if [ -z "$BRANCH" ]; then
        fatal "Invalid repository: $TO_GET"
    fi
    if [ ! -e ${TO_GET} ]; then
        git clone git@github.com:accre/lstore-${TO_GET}.git -b ${BRANCH} ${TO_GET}
    else
        note "Repository ${TO_GET} already exists, not checking out"
    fi
}

function build_lstore_package() {
    set -ex
    TO_BUILD=$1
    INSTALL_PREFIX=${2:-${LSTORE_RELEASE_BASE}/local}
    case $TO_BUILD in
        apr-accre)
            ./configure --prefix=${INSTALL_PREFIX}
            make
            make test
            make install
            ;;
        apr-util-accre)
            if [ -e ${INSTALL_PREFIX}/bin/apr-ACCRE-1-config ]; then
                OTHER_ARGS="--with-apr=${INSTALL_PREFIX}/bin/apr-ACCRE-1-config"
            fi
            ./configure --prefix=${INSTALL_PREFIX} $OTHER_ARGS
            make
            make test
            make install
            ;;
        jerasure|toolbox|gop|ibp|lio)
            cmake . -DCMAKE_PREFIX_PATH="${INSTALL_PREFIX};${INSTALL_PREFIX}/usr/local"
                            #-DPREFIX=${INSTALL_PREFIX}
                            #-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
            make DESTDIR=${INSTALL_PREFIX} install
            ;;
        *)
            fatal "Invalid package: $TO_BUILD"
            ;;
    esac

}

function check_cmake(){
    # Obnoxiously, we need cmake 2.8.12 to build RPM, and even Centos7 only
    #   packages 2.8.11
    CMAKE_VERSION=$(cmake --version | head -n 1 | awk '{ print $3 }')
    IFS='.' read -a VERSION_ARRAY <<< "$CMAKE_VERSION"
    if [ "${VERSION_ARRAY[0]}" -gt 2 ]; then
        # We're good if we're at cmake 3
        return
    fi
    if [[ "${VERSION_ARRAY[1]}" -lt 8 || "${VERSION_ARRAY[2]}" -lt 12 ]]; then
        note "Using bundled version of cmake - the system version is too old '$CMAKE_VERSION'"
        # Download cmake
        # https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz
        # https://cmake.org/files/v3.3/cmake-3.3.2-Linux-i386.tar.gz
        if [ ! -d $LSTORE_RELEASE_BASE/build/cmake ]; then
            pushd $LSTORE_RELEASE_BASE/build
            curl https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz | tar xz
            mv cmake-3.3.2-Linux-x86_64 cmake
            popd
        fi
        export PATH="$LSTORE_RELEASE_BASE/build/cmake/bin:${PATH}"
    fi
    hash -r
    CMAKE_VERSION=$(cmake --version | head -n 1 |  awk '{ print $3 }')
    note "Bundled version of cmake is version '$CMAKE_VERSION'"
    note "Bundled cmake can be found at $(which cmake)"
}

