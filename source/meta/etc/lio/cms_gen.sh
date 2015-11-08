#!/bin/bash
#**********************************************************************
#
#  cms_gen.sh depot_number key=val...
#
#**********************************************************************

if [ "${1}" == "" ]; then
  echo "$0 depot_number key1=val1 ... keyN=valN"
  exit
fi

rid_base=1501

dn=${1}; shift

(( dstart = rid_base + (dn-1)*36 ))
(( dend = dstart + 35 ))

./rs_gen.sh cms-depot${dn}.vampire ${dstart} ${dend} $*
