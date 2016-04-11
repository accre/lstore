#!/bin/bash


ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)  
DEF_IP=$(hostname -I | cut -d ' ' -f 1)
IP=${1:-$DEF_IP}
BASE=${2:-$ABSOLUTE_PATH}


echo "Switching to LServer IP $IP"

sed -i.bak 's,\(.*\)address\s*=\s*[^:]*.[^:]*,\1address=tcp://'"$IP"',' \
            $BASE/lio-core.cfg
