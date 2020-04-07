#!/bin/bash

if [ "${2}" == "" ]; then
   echo "dedup.sh fname base"
   echo "   Remove from fname entries in common with both fname and base and print to stdout"
   exit 1
fi

fname=${1}
base=${2}

cat ${fname} ${base} | sort | uniq -u > /tmp/dedup.$$
cat /tmp/dedup.$$ ${fname} | sort | uniq -d
rm /tmp/dedup.$$

