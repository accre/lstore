#!/bin/bash

if [ "${2}" == "" ]; then
    echo "${0} dXX rid_1 .. rid_N"
    exit 1
fi

depot=${1}
shift

fname=""
for i in $*; do
    echo "Processing RID ${i}"
    ./warmer_query.py --fname --prefix "@:" --rid ${i} > /tmp/${depot}r${i}.$$
    echo "      $(wc -l /tmp/${depot}r${i}.$$ | cut -f1 -d\  ) files"
    if [ "${fname}" == "" ]; then
        fname="${depot}r${i}"
    else
        fname="${fname}_${depot}r${i}"
    fi
done

fname="${fname}"
echo "Composite name: ${fname}"

cat /tmp/*.$$ | sort | uniq > /tmp/${fname}.w
rm /tmp/*.$$

echo "Total files: $(wc -l /tmp/${fname}.w | cut -f1 -d\  )"

