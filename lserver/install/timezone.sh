#!/usr/bin/env bash

set -e
echo "America/Chicago" > /etc/timezone
ln -snf /usr/share/zoneinfo/America/Chicago /etc/localtime
sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
echo 'LANG="en_US.UTF-8"'>/etc/default/locale
dpkg-reconfigure --frontend=noninteractive locales
update-locale LANG=en_US.UTF-8
