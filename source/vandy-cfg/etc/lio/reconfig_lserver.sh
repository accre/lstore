#!/bin/bash

#BASE=/etc/lio
BASE=.
DEF_IP=$(hostname -I | cut -d ' ' -f 1)
IP=$1
IP=${IP:-$DEF_IP}

echo "Switching to LServer IP $IP"

sed -i.bak 's,\(.*\)address\s*=\s*.*,\1address=tcp://'"$IP"':6713,' $BASE/lio-core.cfg
