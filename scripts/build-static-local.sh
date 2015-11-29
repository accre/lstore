#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
export PATH=/usr/local//Cellar/llvm/3.6.2/bin/:$PATH
build_helper STATIC toolbox gop ibp lio
