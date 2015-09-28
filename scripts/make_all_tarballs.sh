#!/usr/bin/env bash

[ "${1}" == "" ] && echo "${0} redmine-user" && exit 1

user=${1}

cd tarballs

../scripts/make_tarball.sh ${user} toolbox
../scripts/make_tarball.sh ${user} gop
../scripts/make_tarball.sh ${user} ibp
../scripts/make_tarball.sh ${user} lio


