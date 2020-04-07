#!/usr/bin/env bash

source lio_helpers.sh

#THis sets the LD_PRELOAD for tcmalloc
eval $(get_ld_preload_tcmalloc)

ulimit -n 20480
ulimit -c unlimited
cp /usr/bin/lio_server /tmp/lio_server.$$
cd /tmp
/tmp/lio_server.$$ -d 1 -c /etc/lio/server.cfg
