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

LHOST="--host ${LSERVER_HOST}"
[ "${LSERVER_HOST}" == "" ] && LHOST=

LPORT="--port ${LSERVER_PORT}"
[ "${LSERVER_PORT}" == "" ] && LPORT=

#Add the container name to the warmer environment for email notifications
sed -i -e "s/DOCKER_NAME=.*$/DOCKER_NAME=${DOCKER_NAME}/g" /etc/cron.d/warmer

#Make the hosts.cfg
reconfigure_lserver.sh ${LHOST} ${LPORT}

#Launch the LServer
launch_server.sh

#Startup the services
cron
service ssh start
/etc/init.d/nullmailer start

#Let them know how to kick out safely
echo "Ctrl-P Ctrl-Q to exit without stopping this container"
bash
