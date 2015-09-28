#!/usr/bin/env bash

[ "$2" == "" ] && echo "$0 user-id repository-name" && exit 1

user=${1}
repo=${2}

now=$(pwd)

echo "Building tarball for repository ${repo} using Redmine user ${user}"

cd /tmp
mkdir tarball.$$
cd tarball.$$

echo "Checking out repository.  May be asked for password..."
git clone https://${user}@redmine.accre.vanderbilt.edu/git/${repo}.git

echo "Generating some info about the repository"
cd ${repo}

echo "Tarball generated on: $(date)" > HISTORY
echo " " >> HISTORY
echo "Branch information:" >> HISTORY
echo "----------------------------------------------" >> HISTORY
git branch >> HISTORY
echo " " >> HISTORY
echo "              Log history" >> HISTORY
echo "----------------------------------------------" >> HISTORY
git log --pretty=oneline >> HISTORY
echo "----------------------------------------------" >> HISTORY

cd ..

tar --exclude=.git -zvcf ${repo}.tgz ${repo} ${repo}/.gitignore
cp ${repo}.tgz ${now}/

cd ..
rm -rf tarball.$$

