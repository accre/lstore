#!/bin/bash

if [ "${1}" == "" ]; then
    echo "${0} _depot_number_"
    exit 1
fi

depot=d${1}
rstart=$(( 1501 + (${1}-1)*36 ))
rend=$(( ${rstart} + 35 ))
range={${rstart}..${rend}}
echo "range=${range}"
shift

fname=""
for i in $(seq ${rstart} ${rend}); do
    echo "Processing RID ${i}"
    ./warmer_query.py --fname --prefix "@:" --rid ${i} > /tmp/${depot}r${i}.$$
    echo "      $(wc -l /tmp/${depot}r${i}.$$ | cut -f1 -d\  ) files"
done

echo "Composite name: ${depot}.w"

cat /tmp/*.$$ | sort | uniq > /tmp/${depot}.w
rm /tmp/*.$$

echo "Total files: $(wc -l /tmp/${depot}.w | cut -f1 -d\  )"

