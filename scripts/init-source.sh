#!/usr/bin/env bash
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

get_source apr-accre apr-util-accre jerasure czmq leveldb
get_source toolbox gop ibp lio
get_source gridftp
