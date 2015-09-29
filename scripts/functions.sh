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

