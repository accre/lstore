# Helper functions for lstore-release

#
# Globals
#
LSTORE_SCRIPT_BASE=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
LSTORE_RELEASE_BASE=$(cd $(dirname "${LSTORE_SCRIPT_BASE}") && pwd)
LSTORE_TARBALL_ROOT=$LSTORE_RELEASE_BASE/tarballs/
LSTORE_HEAD_BRANCHES="apr-accre=accre-fork
                       apr-util-accre=accre-fork
                       jerasure=v1
                       lio=master
                       gop=master
                       toolbox=master
                       ibp=master
                       czmq=master"

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
# Additional helpers
#
function get_repo_master() {
    for VAR in $LSTORE_HEAD_BRANCHES; do
        if [ "${VAR%=*}" == "$1" ]; then
            echo "${VAR##*=}"
        fi
    done
}
#
# Manipulating local repositories
#
function get_lstore_source() {
    # TODO: Accept an additional argument allowing you to override the source
    #       repository/branch.
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
        # Try via SSH first and fall back to https otherwise
        git clone git@github.com:accre/lstore-${TO_GET}.git -b ${BRANCH} ${TO_GET} || \
            git clone https://github.com/accre/lstore-${TO_GET}.git -b ${BRANCH} ${TO_GET}
    else
        note "Repository ${TO_GET} already exists, not checking out"
    fi
}

function build_lstore_binary() {
    # In-tree builds (are for chumps)
    #     Make out-of-tree builds by default and then let someone force the
    #     build to the source tree if they're a masochist.
    build_lstore_binary_outof_tree $1 $(pwd) $2
}

function build_lstore_binary_outof_tree() {
    set -e
    TO_BUILD=$1
    SOURCE_PATH=$2
    INSTALL_PREFIX=${3:-${LSTORE_RELEASE_BASE}/local}
    case $TO_BUILD in
        apr-accre)
            # Keep this in sync with CPackConfig.cmake in our fork
            ${SOURCE_PATH}/configure \
                        --prefix=${INSTALL_PREFIX} \
                        --includedir=${INSTALL_PREFIX}/include/apr-ACCRE-1 \
                        --with-installbuilddir=${INSTALL_PREFIX}/lib/apr-ACCRE-1/build
            make
            make test
            make install
            ;;
        apr-util-accre)
            if [ -e ${INSTALL_PREFIX}/bin/apr-ACCRE-1-config ]; then
                OTHER_ARGS="--with-apr=${INSTALL_PREFIX}/bin/apr-ACCRE-1-config"
            fi
            # Keep this in sync with CPackConfig.cmake in our fork
            ${SOURCE_PATH}/configure --prefix=${INSTALL_PREFIX} $OTHER_ARGS \
                        --includedir=${INSTALL_PREFIX}/include/apr-util-ACCRE-1 \
                        --with-installbuilddir=${INSTALL_PREFIX}/lib/apr-util-ACCRE-1/build
            make
            make test
            make install
            ;;
        jerasure|toolbox|gop|ibp|lio|czmq)
            cmake ${SOURCE_PATH} -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX} -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
            make install
            ;;
        *)
            fatal "Invalid package: $TO_BUILD"
            ;;
    esac

}

function build_lstore_package() {
    set -e
    TO_BUILD=$1
    SOURCE_PATH=${2:-${LSTORE_RELEASE_BASE}/sources/${TO_BUILD}}
    TAG_NAME=${3:-test}
    DISTRO_NAME=${4:-undefined}
    case $DISTRO_NAME in
        undefined)
            CPACK_ARG=""
            CMAKE_ARG=""
            NATIVE_PKG=""
            ;;
        ubuntu-*|debian-*)
            CPACK_ARG="-G DEB"
            CMAKE_ARG="-DCPACK_GENERATOR=DEB -DCPACK_SOURCE_GENERATOR=DEB"
            NATIVE_PKG="cp -ra $SOURCE_BASE/$PACKAGE/ ./ ; pushd $PACKAGE ; dpkg-buildpackage -uc -us ; popd"
            ;;
        centos-*)
            CPACK_ARG="-G RPM"
            CMAKE_ARG="-DCPACK_GENERATOR=RPM -DCPACK_SOURCE_GENERATOR=RPM"
            NATIVE_PKG=""
            ;;
        *)
            fatal "Unexpected distro name $DISTRO_NAME"
            ;;
    esac
    case $TO_BUILD in
        apr-accre|apr-util-accre)
            ls -l $SOURCE_PATH/CPackConfig.cmake
            cpack $CPACK_ARG --config $SOURCE_PATH/CPackConfig.cmake \
                   --debug --verbose "-DCPACK_VERSION=$TAG_NAME" || \
                fatal "$(cat _CPack_Packages/*/InstallOutput.log)"
            ;;
        czmq)
            eval $NATIVE_PKG
            ;;
        jerasure|lio|ibp|gop|toolbox)
            # This is gross, but works for now..
            set -x
            cmake -DWANT_PACKAGE:BOOL=ON "-DLSTORE_PROJECT_VERSION=$TAG_NAME"\
                    $CMAKE_ARG --debug --verbose $SOURCE_PATH
            set +x
            make package
            ;;
        *)
            fatal "Invalid package: $TO_BUILD"
            ;;
    esac
}

function get_cmake_tarballs(){
    if [ ! -d ${LSTORE_TARBALL_ROOT} ]; then
        mkdir ${LSTORE_TARBALL_ROOT}
    fi
    curl https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz > \
            ${LSTORE_TARBALL_ROOT}/cmake-3.3.2-Linux-x86_64.tar.gz
}

function check_cmake(){
    # Obnoxiously, we need cmake 2.8.12 to build RPM, and even Centos7 only
    #   packages 2.8.11
    set +e
    # TODO: Detect architechture
    CMAKE_LOCAL_TARBALL=${LSTORE_TARBALL_ROOT}/cmake-3.3.2-Linux-x86_64.tar.gz
    CMAKE_VERSION=$(cmake --version 2>/dev/null | head -n 1 | awk '{ print $3 }')
    [ -z "$CMAKE_VERSION" ] && CMAKE_VERSION="0.0.0"
    set -e
    INSTALL_PATH=${1:-${LSTORE_RELEASE_BASE}/build}
    IFS="." read -a VERSION_ARRAY <<< "${CMAKE_VERSION}"
    
    if [ "${VERSION_ARRAY[0]}" -gt 2 ]; then
        # We're good if we're at cmake 3
        return
    fi
    if [[ "${VERSION_ARRAY[1]}" -lt 8 || "${VERSION_ARRAY[2]}" -lt 12 ]]; then
        [ $CMAKE_VERSION == "0.0.0" ] ||  \
            note "Using bundled version of cmake - the system version is too old '$CMAKE_VERSION'" &&
            note "Couldn't find cmake, pulling our own"
        
        # Download cmake
        if [ ! -d $INSTALL_PATH/cmake ]; then
            if [ ! -e $CMAKE_LOCAL_TARBALL ]; then
                get_cmake_tarballs
            fi
            pushd $INSTALL_PATH
            tar xzf $CMAKE_LOCAL_TARBALL
            mv cmake-3.3.2-Linux-x86_64 cmake
            popd
        fi
        export PATH="$INSTALL_PATH/cmake/bin:$PATH"
    fi
    hash -r
    CMAKE_VERSION=$(cmake --version | head -n 1 |  awk '{ print $3 }')
    note "Bundled version of cmake is version '$CMAKE_VERSION'"
    note "Bundled cmake can be found at $(which cmake)"
}

function build_helper() {
    # Don't copy/paste code twice for build-local and build-external
    set -e
    BUILD_BASE="$LSTORE_RELEASE_BASE/build"
    SOURCE_BASE="$LSTORE_RELEASE_BASE/source"


    PREFIX=$LSTORE_RELEASE_BASE/local
    check_cmake

    cd $SOURCE_BASE
    for p in "$@"; do
        get_lstore_source ${p}
    done

    cd $BUILD_BASE
    for p in $@; do
        BUILT_FLAG="${PREFIX}/built-${p}"
        if [ -e $BUILT_FLAG ]; then
            note "Not building ${p}, was already built. To change this behavior,"
            note "    remove $BUILT_FLAG"
            continue
        fi
        [ -d ${p} ] && rm -rf ${p}
        mkdir -p ${p}
        pushd ${p}
        build_lstore_binary_outof_tree ${p} $SOURCE_BASE/${p} ${PREFIX} 2>&1 | tee $LSTORE_RELEASE_BASE/logs/${p}-build.log
        [ ${PIPESTATUS[0]} -eq 0 ] || fatal "Could not build ${p}"
        touch $BUILT_FLAG
        popd
    done
}

function get_repo_status() {
    REPO_PATH=$1
    cd $REPO_PATH
    echo -n $(git rev-parse --abbrev-ref HEAD)
    [[ $(git diff --shortstat HEAD 2> /dev/null | tail -n1) != "" ]] && \
        echo " DIRTY" || echo " CLEAN"
    cd - &>/dev/null
}

function get_source() {
    set -e
    SOURCE_BASE="$LSTORE_RELEASE_BASE/source"

    cd $SOURCE_BASE
    for p in "$@"; do
        get_lstore_source ${p}
    done
}

function load_github_token() {
    if [ ! -z "${LSTORE_GITHUB_TOKEN+}" ]; then
        return
    elif [ -e $HOME/.lstore_release ]; then
        source $HOME/.lstore_release
    fi
    set +u
    [ -z "${LSTORE_GITHUB_TOKEN}" ] && \
        fatal "Need a github authentication token to perform this action. To get
a token, use the following FAQ (be sure to remove all scopes).

https://help.github.com/articles/creating-an-access-token-for-command-line-use/

This token should be set to \$LSTORE_GITHUB_TOKEN. Alternately, the file
\$HOME/.lstore_release can be used to store secrets. The following will set
your github token only when needed:

export LSTORE_GITHUB_TOKEN=<token from github page>"
    set -u
    return 0

}

