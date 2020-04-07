#!/usr/bin/env bash

sed -i.docker-orig -e 's@^root:.*$@root:$6$BrGZ4Oi6$AxPcX.zbzOMGE1RzTf6SqGYhwdic7w0PINypcgEctXf45h8G4TKrRR1bX8iSD20eOlDIjphTuTnarP3tVdki8/:17000:0:99999:7:::@' /etc/shadow