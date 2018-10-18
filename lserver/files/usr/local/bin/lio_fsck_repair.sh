#!/bin/bash

OPT=""
echo "****************************************************"
if [ "${1}" == "--delete" ]; then
    OPT="-ex delete"
    echo "***************REMOVING FILES***********"
else
    echo "***************DRY RUN***********"
fi
echo "****************************************************"
echo

#grep err:11 /tmp/fsck.log | awk '{print $3}' | cut -f2 -d: | awk '{print "@:"$1}' | lio_fsck -c /etc/lio/lio-warmer.cfg -i 20 ${OPT} -
grep ^err: /tmp/fsck.log | awk '{print $3}' | cut -f2 -d: | awk '{print "@:"$1}' | cut -f1-7 -d/ | sort | uniq | lio_fsck -c /etc/lio/lio-warmer.cfg -i 20 ${OPT} -
