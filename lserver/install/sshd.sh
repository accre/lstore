#!/usr/bin/env bash

sed -i.pkg-orig -e 's/Port 22/Port 2222/' -e 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
