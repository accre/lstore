#!/usr/bin/env bash

# Packages the Lserver Management python tools

set -e
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

${LSTORE_SCRIPT_BASE}/build-python3-venv.sh
[ "$?" != "0" ] && exit $?

source ${LSTORE_VENV}/bin/activate

cd ${LSTORE_RELEASE_BASE}/lserver/lmgmt
python3 setup.py sdist
ERR=$?
deactivate
exit ${ERR}

