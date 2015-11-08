#!/bin/bash
#**********************************************************************
#
#  rs_gen.sh [-rk prefix] hostname rid_1 rid_N key=val...
#
#**********************************************************************

if [ "${1}" == "" ]; then
  echo "$0 [-rk prefix] hostname rid_1 rid_N key1=val1 ... keyN=valN"
  exit
fi


rk=
if [ "${1}" == "-rk" ]; then
  shift
  rk="${1}_"
  shift;
fi

host=${1}; shift
port=6714
rid1=${1}; shift
rid2=${1}; shift

for rid in `seq ${rid1} ${rid2}`; do
   echo "[rid]"
   echo "rid_key=${rk}${rid}"
   echo "ds_key=${host}:${port}/${rid}"
   echo "host=${host}"

   for a in ${*}; do
      echo "${a}"
   done
   echo ""
done

