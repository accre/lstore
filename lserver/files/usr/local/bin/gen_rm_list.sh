#!/bin/bash

lio_getattr -rd 100 -c /etc/lio/lio-warmer.cfg -al system.create -single '@:/cms/store/temp/' | sed -e 's/system.create=//' | cut -f 1 -d\| | awk "{ if (\$2<$(date -d 'now - 28 days' +%s)) {print \"@:\"\$1} }" > /tmp/temp.old

