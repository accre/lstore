#!/usr/bin/env /bin/bash

if [ "${1}" == "-h" ]; then
    echo "${0} [host]"
    exit
fi

DEF_HOST=$(hostname -f)
HOST=${1:-$DEF_HOST}

echo "host=tcp://${HOST}" > /etc/lio/host.cfg
echo "host=tcp://${HOST}" > /etc/lio/clients/host.cfg
echo "lstore://${HOST}:lio" > /etc/lio/default

