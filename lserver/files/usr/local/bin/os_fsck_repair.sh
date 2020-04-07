#!/bin/bash

OPT=""
echo "****************************************************"
if [ "${1}" == "--delete" ]; then
    OPT="-fix delete"
    echo "***************REMOVING FILES***********"
else
    echo "***************DRY RUN***********"
fi
echo "****************************************************"
echo

grep err: /tmp/os.fsck | awk '{print $3}' | cut -f2 -d: | os_fsck -c /etc/lio/lio-warmer.cfg -i 20 ${OPT} -

