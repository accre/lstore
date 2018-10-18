#!/usr/bin/env bash

verbose=0

function print_info {
    if [ "${verbose}" == "1" ]; then
        echo $*
    fi
}

function print_help {
  echo "lserver_mgmt.sh [-v] [start|stop|stop_soft|restart|status]"
  echo "    -v       Print more information"
}

function get_pid {
   pgrep lio_server
}

function print_status {
    pid=$(get_pid)
    if [ "${pid}" == "" ]; then
        echo "lio_server is NOT running!"
        return 1
    else
        echo "lio_server is running with PID ${pid}"
        return 0
    fi
}

function launch {
    ulimit -n 20480
    ulimit -c unlimited

    eval $(get_ld_preload_tcmalloc)
    TCMALLOC_RELEASE_RATE=5 lio_server -d 1 -c /etc/lio/lio-server.cfg
    [ "${verbose}" == "1" ] && print_status
}

function start {
    pid=$(get_pid)
    if [ "${pid}" == "" ]; then
        launch
    else
        echo "lio_server is already running with PID ${pid}!"
        return 1
    fi
}

function wait_for_shutdown {
    kick_out=$(date -d "now + ${1} seconds" +%s)
    while [ "$(get_pid)" != "" ]; do
        print_info "lio_server still running"
        sleep 1
        now=$(date -d now +%s)
        if [ ${now} -ge ${kick_out} ]; then
            return 1
        fi
    done

    return 0
}

function stop_soft {
    killall -QUIT lio_server
    wait_for_shutdown 60
    if [ $? -ne 0 ]; then
        echo "Shutdown failed!"
        return 1
    fi
    return 0
}

function stop_hard {
    stop_soft

    if [ $? -ne 0 ]; then
        echo "Forcing shutdown"
        killall -9 lio_server
        wait_for_shutdown 60
    fi
}


#---------------------------------------------------------
#---------------------------------------------------------

if [ "${1}" == "-v" ];then
    verbose=1
    shift
fi

source /usr/local/bin/lio_helpers.sh

case "${1}" in
    "start"                ) start ;;
    "stop" | "stop_hard"   ) stop_hard ;;
    "stop_soft"            ) stop_soft ;;
    "restart"              ) stop_hard && start ;;
    "status"               ) print_status ;;
    *                      ) print_help ;;
esac
