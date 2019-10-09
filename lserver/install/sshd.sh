#!/usr/bin/env bash

#This line removes the '#'
sed -i.pkg-v1 -e 's/#Port 22/Port 22/' -e 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
#and make the substitution
sed -i.pkg-v2 -e 's/Port 22/Port 4444/' -e 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
