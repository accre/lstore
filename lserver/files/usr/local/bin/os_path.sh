#!/bin/bash

fname=$(echo ${1} | sed 's/@://g')

bname=$(basename ${fname})
dname=$(dirname ${fname})

fpath="/lio/osfile/file${dname}/_^FA^_/_^FA^_${bname}"
echo ${fpath}
