#!/bin/bash
#----------------------------------------------------------------------------------------------
#    lfs_service_manager.sh - LFS mount manager designed for use on the cluster
#----------------------------------------------------------------------------------------------

usage() {
    echo "Usage: $0 {start|stop|status|restart [-f]|cleanup [-f|-k] [older-than-date]|install <path> <user> [binaries-path]}"
    #echo "Usage: $0 {start|stop|status|restart|cleanup [-f] [older-than-date]}"
}

LIO_CFG_PREFIX=/etc/lio
LIO_CORE_PREFIX=/usr
LIO_CONFIG="-c 10.0.13.241:cluster-lfs"

#Determine where the binaries and config are stored
if [ "$LIO_CFG_PREFIX" == "" ] || [ "$LIO_CORE_PREFIX" == "" ]; then
    echo "Unable to determine LIO paths."
    exit 1
fi

# don't use trailing slashes for directories
BASE=${LFS_BASE:-/lfs_roots}
INSTANCE_ROOT="${BASE}/instances"
MOUNT_SYMLINK="${BASE}/mnt"
CURRENT_SYMLINK="${BASE}/current"
FUSE_BIN_REL_PATH="bin/lio_fuse"
FUSE_OPTS="-o big_writes,use_ino,kernel_cache,allow_other,fsname=lfs:lio,max_background=128,congestion_threshold=96"
#old fuse version opts: FUSE_OPTS="-o big_writes,use_ino,kernel_cache,allow_other,fsname=lfs:lio"
LIO_OPTS="-d 1"
if [ "${LIO_CONFIG}" == "" ]; then
    LIO_CONFIG="-c ini:///etc/lio/lio-fuse.cfg"
fi
RUN_AS_USER=lfs
#RUN_AS_USER=lfs
WDIR=/var/run/lfs
FORCE_UMOUNT="0"

TIMEOUT="timeout -s 9"

TCMLIB=$(ldconfig -p |grep libtcmalloc.so | sort | tail -n 1 | awk '{print $4}')
if [ "${TCMLIB}" != "" ]; then
    TCMOPT="TCMALLOC_RELEASE_RATE=5 LD_PRELOAD=${TCMLIB}"
else
    TCMOPT=""
    echo "**** WARN: Missing TCMALLOC ****"
fi


define(){ IFS='\n' read -r -d '' ${1} || true; }

# for cluster
# format: </absolute/path/of/src> <relative/dest>
define LFS_FILE_CPS <<EOF
        $LIO_CORE_PREFIX/bin/lio_fuse           bin/lio_fuse
        /usr/lib/x86_64-linux-gnu/libtoolbox.so* lib64/.
        /usr/lib/x86_64-linux-gnu/libgop.so* lib64/.
        /usr/lib/x86_64-linux-gnu/libibp.so* lib64/.
        /usr/lib/x86_64-linux-gnu/liblio.so* lib64/.
EOF

install() {
    if [ "$#" -lt "3" ]; then
        echo "The install sub-command requires arguements as follows:"
        echo "Usage: $0 install <path> <user> [binaries-path]"
        exit 2;
    fi

    script_path=$1
    install_target=$2
    user=$3
        master_version=$4

    new_script_path="${install_target}/$(basename $script_path)"
    echo "Installing to $install_target and setting up service to be run by user: $user"

    if ! id $user; then
        echo "User $user does not exist, creating the user as a system user that is a fuse group member..."
        useradd -r -s /bin/false -G fuse lfs
    fi

    echo "Note that the current permissions for 'fusermount' are:"
    ls -l $(which ${FUSE_PATH}/bin/fusermount)
    echo "if you experience 'fusermount' permission problems you may want to consider doing something like the following:"
    echo "   chown root:fuse ${FUSE_PATH}/bin/fusermount"
    echo "   chmod u+s ${FUSE_PATH}/bin/fusermount"
    echo

    echo "setting up file structure and copying service manager script"

    mkdir -p "${install_target}/masters"
#   chown "${user}:" "${install_target}/masters"
    mkdir -p "${install_target}/instances"
    chown "${user}:" "${install_target}/instances"
    cp $script_path $new_script_path
    sed -i 's@^BASE=.*$@BASE='"$install_target"'@' $new_script_path
    sed -i 's@^RUN_AS_USER=.*$@RUN_AS_USER='"$user"'@' $new_script_path
        mkdir $WDIR
        chown "${user}:" $WDIR

    if [ ! -f /etc/fuse.conf ]; then
        echo "/etc/fuse.conf doesn't exist, creating it..."
        echo "user_allow_other" >> /etc/fuse.conf
        chown root:lfs /etc/fuse.conf
    fi

    if [ -n "$master_version" ]; then
        echo "Master version provided, linking:"
        echo ln -s "$master_version" "$MASTER"
        ln -s "$master_version" "$MASTER"
    else
        echo "Next copy a version of lfs to $MASTER or update the 'MASTER' variable in $new_script_path to the path of a different master copy."
    fi

    echo "Then run $new_script_path to start the service."
    echo "done"
}

service_status() {
    if [ ! -e "$CURRENT_SYMLINK" ]; then
        PRIMARY_ID='service_is_stopped'
    else
        PRIMARY_ID=$(basename $(readlink -f $CURRENT_SYMLINK))
    fi

    if [ "$PRIMARY_ID" = 'service_is_stopped' ]; then
        echo "Service is stopped"
    else
        echo "Service is running"
    fi

        echo "Status of instances:"
        ls -r $INSTANCE_ROOT | while read i; do
            ID=$(basename $i)
            #echo "   $ID: $(check_instance_health $ID)"
            for i in $(echo $ID $(check_instance_health $ID)); do
                printf "%-24s" $i
            done | sed 's/ *$//'
            printf "\n"
        done
}

service_start() {
    PRIMARY_ID=$(basename $(readlink -f $CURRENT_SYMLINK))

    if [ "$PRIMARY_ID" = 'service_is_stopped' ] || check_instance_health $PRIMARY_ID | grep -q -E 'exists=NO|is_running=NO|mounted=NO'; then
        start_instance $(generate_instance_id)
    else
        echo "Service is already running and it appears to be operational, instance='${INSTANCE_ROOT}/$PRIMARY_ID'. Run the 'restart' command if you wish to force a new instance to be created."
    fi
}

service_stop() {
    echo "Preventing new activity..."
    update_symlink $MOUNT_SYMLINK 'service_is_stopped'
    echo "Waiting a bit so lfs is more likely to be idle" # TODO make this more sophisticated
    sleep 5
    stop_all
    echo "Retry again later if any instances were unable to be stopped"
}

service_restart() {
  if [ -f "/scratch/tacketar/lfs.dead" ]
  then
    echo "$(date) - LFS restart on ${HOSTNAME}" >> /scratch/tacketar/lfs.dead
  fi
    start_instance $(generate_instance_id) $1
}

service_cleanup() {
    if [ "${1}" == "-f" ]; then
        FORCE_UMOUNT="1"
        shift
    fi

    if [ "${1}" == "-k" ]; then
        FORCE_UMOUNT="1"
        shift
        stop_old "$1"
    else
        stop_inactive
    fi
    remove_old "$1"
}

stop_inactive() {
    echo "Stopping inactive instances ..."
    service_status | grep -E 'is_running=yes|mounted=yes' | grep 'files_in_use:0' | grep 'primary=NO' | while read id everything_else; do
        echo "Stopping inactive instance '$id'"
        stop_instance $id
    done
}

stop_old() {
    DEFAULT_THRESH="now"
    DATE_THRESH="$1"
    DATE_THRESH=${DATE_THRESH:-$DEFAULT_THRESH}
    ABS_DATE_EPOCH=$(date -d "$DATE_THRESH" +%s)
    ABS_DATE_HR="$(date -d @$ABS_DATE_EPOCH)"

    echo "Stopping all instances OLDER than '${DATE_THRESH}' or ${ABS_DATE_HR}..."
    LAST=$(date -d "now + 1 hour" +%s)
    service_status | grep 'primary' | while read id everything_else; do
        maybe=$(echo $everything_else | grep -E 'is_running=yes|mounted=yes' | grep 'primary=NO')
        if [[ "$LAST" -lt "$ABS_DATE_EPOCH" && "${maybe}" != "" ]]; then
            echo "Stopping Old instance '$id' (" $(date -d @${id}) ")"
            stop_instance $id
        fi
        LAST=${id}
    done
}

stop_all() {
    echo "Attempting to stop all instances ..."
    service_status | grep 'is_running=yes' | while read id everything_else; do
        echo "Stopping instance '$id' (" $(date -d @${id}) ")"
        stop_instance $id
    done
}

remove_old() {
    DEFAULT_THRESH="now - 7 days"
    DATE_THRESH="$1"
    DATE_THRESH=${DATE_THRESH:-$DEFAULT_THRESH}
    ABS_DATE_EPOCH=$(date -d "$DATE_THRESH" +%s)
    ABS_DATE_HR="$(date -d @$ABS_DATE_EPOCH)"
    echo "Removing files associated with stopped instances older than $ABS_DATE_HR ..."

    LAST=$(date -d "now + 1 hour" +%s)
    service_status | grep primary | while read id everything_else; do
        maybe=$(echo $everything_else | grep 'is_running=NO' | grep 'mounted=NO')
        echo "$id [$LAST] ?< $ABS_DATE_EPOCH"
        if [[ "$LAST" -lt "$ABS_DATE_EPOCH" && "${maybe}" != "" ]]; then
            echo "Removing files for instance '$id' (" $(date -d @${id}) ")"
            remove_instance $id
        fi
        LAST=${id}
    done
}

start_instance_attempt() {
    INSTANCE_ID=$1
    INSTANCE_PATH="${INSTANCE_ROOT}/${INSTANCE_ID}"
    INSTANCE_LOGS="${INSTANCE_PATH}/logs"
    INSTANCE_MNT="${INSTANCE_PATH}/mnt"

    mkdir -p "$INSTANCE_PATH"
    mkdir -p "$INSTANCE_LOGS"
    mkdir -p "$INSTANCE_MNT"

    while read src dest; do
        if [ -n "$src" ]; then
            destpath="${INSTANCE_PATH}/$dest"
            pdir="$(dirname $destpath)"
            if [ ! -d "$pdir" ]; then
                mkdir -p "$pdir"
            fi
            echo cp -r -a $src ${INSTANCE_PATH}/$dest
            cp -r -a $src ${INSTANCE_PATH}/$dest
        fi
    done <<<"$LFS_FILE_CPS"

    chown -R "${RUN_AS_USER}:" "$INSTANCE_PATH"

    # allow coredumps, set working dir
    #cd ${INSTANCE_PATH}
    cd $WDIR
    ulimit -c unlimited
    echo "Core dumps enabled (unlimited size), working directory is '$(pwd)' and the current destination/handler is '$(cat /proc/sys/kernel/core_pattern)' (set using /proc/sys/kernel/core_pattern)"

#NOTES on coredumps
# echo "/tmp/core" > /proc/sys/kernel/core_pattern
# mkdir -p /tmp/cores
# chmod a+rwx /tmp/cores
# echo "/tmp/cores/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern
# bash -c 'kill -s SIGSEGV $$'
# Segmenation fault (core dumped)
# ls /tmp/cores/core.bash.12494.brazil.accre.vanderbilt.edu.1392850395 ^C
# sleep 100
# ^\ Quit (core dumped)

    # ulimits are done twice, once as root to raise the hard limit and once after the sudo to raise user's soft limit
    ulimit -n 20480

    echo sudo -u ${RUN_AS_USER} bash -c "ulimit -c unlimited; ulimit -n 20480; LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}${INSTANCE_PATH:+:${INSTANCE_PATH}/lib} ${TCMOPT} ${INSTANCE_PATH}/${FUSE_BIN_REL_PATH} ${FUSE_OPTS} $INSTANCE_MNT --lio -C ${WDIR} -log ${INSTANCE_PATH}/logs/fuse.log $LIO_OPTS ${LIO_CONFIG}"
    sudo -u ${RUN_AS_USER} timeout -s 9 30 bash -c "ulimit -c unlimited; ulimit -n 20480; LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}${INSTANCE_PATH:+:${INSTANCE_PATH}/lib} ${TCMOPT} ${INSTANCE_PATH}/${FUSE_BIN_REL_PATH} ${FUSE_OPTS} $INSTANCE_MNT --lio -C ${WDIR} -log ${INSTANCE_PATH}/logs/fuse.log $LIO_OPTS ${LIO_CONFIG}"
#   sudo -u ${RUN_AS_USER} timeout -s 9 5 bash -c "ulimit -c unlimited; ulimit -n 20480; LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}${INSTANCE_PATH:+:${INSTANCE_PATH}/lib} ${TCMOPT} sleep 20"
    RESULT=$?
#echo "RETURN_CODE=${RESULT}"
    update_symlink $MOUNT_SYMLINK $INSTANCE_MNT
    update_symlink $CURRENT_SYMLINK $INSTANCE_PATH

    if [ "${RESULT}" == "0" ]; then
        echo "Instance with ID $INSTANCE_ID based in $INSTANCE_PATH started, link to mount: $MOUNT_SYMLINK"
    else
        echo "Instance with ID $INSTANCE_ID based in $INSTANCE_PATH FAILED to start with error ${RESULT}, link to mount: $MOUNT_SYMLINK"
    fi

    return ${RESULT}
}

start_instance() {
   start_instance_attempt $*
   if [[ ("${2}" == "-f") && ( "$?" != "0") ]]; then
       echo "Failed to start a new instance.  Killing all instances and trying again"
       killall lio_fuse
       echo "Attempting to restart new instance"
       start_instance_attempt $*
       echo "Cleaning up everything"
       service_cleanup -f
   fi
}

stop_instance() {
    INSTANCE_ID=$1
    INSTANCE_PATH="${INSTANCE_ROOT}/${INSTANCE_ID}"
    INSTANCE_LOGS="${INSTANCE_PATH}/logs"
    INSTANCE_MNT="${INSTANCE_PATH}/mnt"

    echo "DEBUG:  fusermount = ${FUSE_PATH}/bin/fusermount"

    ${FUSE_PATH}/bin/fusermount -u $INSTANCE_MNT
        status=$?

    if [ "${status}" != "0" ] && [ "${FORCE_UMOUNT}" == "1" ]; then
        ${FUSE_PATH}/bin/fusermount -u -z $INSTANCE_MNT
        #FPID=$(ps -eo pid,comm,args | grep ${INSTANCE_ID} | grep fuse | cut -f1 -d" ")
        FPID=$(ps -eo pid,comm,args | grep ${INSTANCE_ID} | grep fuse | awk '{print $1}')
        if [ "${FPID}" != "" ]; then
            kill -9 ${FPID}
        fi
    fi
}

remove_instance() {
    INSTANCE_ID=$1
    INSTANCE_PATH="${INSTANCE_ROOT}/${INSTANCE_ID}"
    INSTANCE_LOGS="${INSTANCE_PATH}/logs"
    INSTANCE_MNT="${INSTANCE_PATH}/mnt"

    # provide some protection against accidental deletion
    echo Command: sudo -u $RUN_AS_USER rm --one-file-system -rf "$INSTANCE_PATH"
    echo "Ctrl+C now to cancel (3 sec)"
    sleep 3

    sudo -u $RUN_AS_USER rm --one-file-system -rf $INSTANCE_PATH
}


check_instance_health() {
    INSTANCE_ID=$1
    INSTANCE_PATH="${INSTANCE_ROOT}/${INSTANCE_ID}"
    INSTANCE_LOGS="${INSTANCE_PATH}/logs"
    INSTANCE_MNT="${INSTANCE_PATH}/mnt"

    HEALTHY=true
    STATUS=""

    # primary instance?
    if [ "$INSTANCE_PATH" = "$(readlink -f $CURRENT_SYMLINK)" ]; then
                STATUS+=" primary=yes"
        else
                STATUS+=" primary=NO"
        fi

    # Does it exist?
    if [ -e "$INSTANCE_PATH" ]; then
        STATUS+=" exists=yes"
    else
        STATUS+=" exists=NO"
        echo $STATUS
        return
    fi
    # Is in mounted?
    if mount | grep -q "$INSTANCE_MNT"; then
        STATUS+=" mounted=yes"
    else
        STATUS+=" mounted=NO"
        HEALTHY=false
    fi
    # Is the filesystem process running?
    if pgrep -f "${INSTANCE_PATH}/${FUSE_BIN_REL_PATH}"'.*'"$INSTANCE_MNT" >/dev/null; then
        STATUS+=" is_running=yes"
    else
        STATUS+=" is_running=NO"
        HEALTHY=false
    fi

    if $HEALTHY; then
        # Is it in use?
        FILE_USE_COUNT=$($TIMEOUT 10 bash -c "lsof +f --  $INSTANCE_MNT 2>/dev/null | grep -Ev '^COMMAND ' | wc -l" || echo 'TIMEOUT')
        STATUS+=" files_in_use:$FILE_USE_COUNT"
        # Does a metadata operation work?
        # TODO
    else
        STATUS+=" files_in_use:0"
    fi

    echo $STATUS
}

generate_instance_id() {
    date +%s
}

update_symlink() {
    SYMLINK=$1
    SYMLINK_TMP="$1.new"
    NEW_TARGET=$2

    # This is commented and replaced since it can hang if the mount is borked because '-e' tests dereference links, we do not want to dereference to a hung mount point!
    #if ([ -e "$SYMLINK" ] && [ ! -L "$SYMLINK" ]) || ([ -e "$SYMLINK_TMP" ] && [ ! -L "$SYMLINK_TMP" ]); then
    if ( (! readlink "$SYMLINK" &>/dev/null) && ls -d "$SYMLINK" &>/dev/null) || ( (! readlink "$SYMLINK_TMP" &>/dev/null) && ls -d "$SYMLINK_TMP" &>/dev/null); then
        echo "WARN: something exists at $SYMLINK or $SYMLINK_TMP that it is not a symlink, skipping"
    else
        # make the change as atomic as possible, mv should be atomic on POSIX systems
        ln -snf "${NEW_TARGET}" "$SYMLINK_TMP"  # need -nf to overwrite with no dereference
        mv -T "$SYMLINK_TMP" "$SYMLINK"     # need -T to stop dereference
    fi
}


case "$1" in
    start)
        service_start
        ;;

    stop)
        service_stop
        ;;

    restart)
        service_restart "$2"
        ;;

    status)
        service_status
        ;;

    cleanup)
        service_cleanup "$2" "${3}"
        ;;

    install)
#       echo "'install' command is disabled"
#       disable on cluster
        install "$0" "$2" "$3" "$4"
        ;;

    *)
        usage;
        exit 1;
        ;;
esac

exit 0

