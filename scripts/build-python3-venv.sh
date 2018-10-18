#!/usr/bin/env bash

#  Creates the python3 virtual environment for packaging lmgmt
#
#  The venv is created in the GIT repository root

ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

if [ -e ${LSTORE_VENV} ]; then
    echo "LStore Python 3 virtual environment already exists: ${LSTORE_VENV}"
    exit 0
fi

python3 -m venv ${LSTORE_VENV}

source ${LSTORE_VENV}/bin/activate
pip install --upgrade setuptools
ERR=$?

if [ "${ERR}" == "0" ]; then
    echo "LStore Python3 virtual environment created: ${LSTORE_VENV}"
else
    echo "ERROR creating LStore Python3 virtual environment: ${LSTORE_VENV}"
fi

deactivate
exit ${ERR}

