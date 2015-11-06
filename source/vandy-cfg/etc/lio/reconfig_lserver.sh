#!/bin/bash

#BASE=/etc/lio
BASE=.
DEF_IP=$(hostname -I | cut -d ' ' -f 1)
IP=$1
IP=${IP:-$DEF_IP}

echo "Switching to LServer IP $IP"

mv $BASE/lio-core.cfg{,.old} 2>/dev/null
sed "s/@LSERVER_IP@/$IP/" <$BASE/lio-core.cfg.in >$BASE/lio-core.cfg
