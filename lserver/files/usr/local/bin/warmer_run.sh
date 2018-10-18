#!/usr/bin/env bash

#Warms all the files for *almost* 30 days

#Set LD_PRELOAD for libtcmalloc
source lio_helpers.sh

LOCKFILE=/var/lock/warmer_run.lock

(
  flock -n 9
  if [ "$?" != "0" ]; then
     echo "Another Warmer is currently running!  Exiting."
     ps agux | grep -F warmer_run.sh | grep -v grep
     exit
  fi

  #Simple log rotation
  OUT=/lio/log/warmer_run.log
  mv ${OUT} ${OUT}.2

  #Shuffling the DBs is a little more complicated
  DB=/lio/log/warm
  DB2=${DB}.2
  rm -rf ${DB2}
  mv ${DB} ${DB2}
  mkdir ${DB} ${DB}/rid ${DB}/inode

  (
    echo "==================== START =========================="
    date
    echo "====================================================="
    echo

    ulimit -n 10240
    ulimit -c unlimited
    cd /tmp
    (
        eval $(get_ld_preload_tcmalloc)
        TCMALLOC_RELEASE_RATE=5 lio_warm -i 1 -t /etc/lio/tag.cfg -c /etc/lio/warmer.cfg -db ${DB} -sf -dt 2590000 -np 300 '@:/*'
        status=$?
    )
    echo
    echo "===================== END ==========================="
    date
    echo "warmer status: ${status}"
    echo "====================================================="
  ) > ${OUT}

  # Email the warmer report
  /usr/local/bin/email_warmer_summary.sh

) 9>${LOCKFILE}

