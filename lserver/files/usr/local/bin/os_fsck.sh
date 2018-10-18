#!/bin/bash
os_fsck -c /etc/lio/lio-warmer.cfg '/*' |& tee /tmp/os.fsck

