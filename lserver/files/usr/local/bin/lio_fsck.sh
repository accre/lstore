#!/bin/bash

cp /tmp/fsck.log /tmp/fsck.log.2
lio_fsck -c /etc/lio/lio-warmer.cfg -i 20 @:/* |& tee /tmp/fsck.log

