#!/usr/bin/env /bin/bash

HOST=$(hostname -f)
BPORT=6711

while (( "$#" )); do
    case "$1" in
        -h)
            echo "${0} [-h] [--host host] [--port base_port]"
            exit
            ;;
        --host)
            HOST=$2
            shift 2
            ;;
        --port)
            BPORT=$2
            shift 2
            ;;
        *)
            echo "Unsupported argument: $1"
            exit
            ;;
    esac
done

RC_PORT=${BPORT}
OS_PORT=$((BPORT+1))
RS_PORT=$((BPORT+2))

echo_info() {
    echo "host=tcp://${HOST}"
    echo "rc_port=${RC_PORT}"
    echo "os_port=${OS_PORT}"
    echo "rs_port=${RS_PORT}"
}

echo_info > /etc/lio/host.cfg
echo_info > /etc/lio/clients/host.cfg
echo "lstore://${HOST}:${RC_PORT}:lio" > /etc/lio/default

