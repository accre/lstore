#!/usr/bin/env bash

echo "LSERVER_HOST=${LSERVER_HOST}" > /tmp/onstart
echo "OSFILE=${OSFILE}" >> /tmp/onstart

if [ "${OSFILE}" == "1" ]; then
    if [ -e /lio/osfile/file ]; then
        echo "/lio/osfile already exists. Ignoring sample install."
        echo "Please remove the directory contents to overwrite."
    else
        cd /lio
        tar -zxvf /samples/osfile.tgz .
    fi
fi

reconfigure_lserver.sh ${LSERVER_HOST}
launch_server.sh
cron
service ssh start
/etc/init.d/nullmailer start
echo "Ctrl-P Ctrl-Q to exit without stopping this container"
bash
