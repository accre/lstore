#!/bin/bash

warmer_errors.py -w > /tmp/w
grep ERROR /lio/log/warmer_run.log | awk '{print "@:"$2}' | sort | uniq > /tmp/e
cat /tmp/{w,e} | sort | uniq > /tmp/x
wc -l /tmp/{e,w,x}

