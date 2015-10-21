#!/bin/bash

# Given a GH pull request URL, cut a new release tag

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
load_github_token

RELEASE_URL=$1
note "Attempting to tag and merge $RELEASE_URL"
# TODO: config this
UPSTREAM_REMOTE=origin
ORIGIN_REMOTE=origin
TARGET_BRANCH=accre-release

GITHUB_PULL_INFO=$(curl --request GET --fail --silent \
                    -H "Authorization: token $LSTORE_GITHUB_TOKEN" \
                    $RELEASE_URL)
GITHUB_BASE_REPO=$(echo "$GITHUB_PULL_INFO" | \
                    $LSTORE_RELEASE_BASE/scripts/extract-pull-base-repo.py)
GITHUB_HEAD_REPO=$(echo "$GITHUB_PULL_INFO" | \
                    $LSTORE_RELEASE_BASE/scripts/extract-pull-head-repo.py)
GITHUB_BASE_REF=$(echo "$GITHUB_PULL_INFO" | \
                    $LSTORE_RELEASE_BASE/scripts/extract-pull-base-ref.py)
GITHUB_HEAD_REF=$(echo "$GITHUB_PULL_INFO" | \
                    $LSTORE_RELEASE_BASE/scripts/extract-pull-head-ref.py)
GITHUB_REPO_NAME=$(echo "$GITHUB_PULL_INFO" | \
                    $LSTORE_RELEASE_BASE/scripts/extract-pull-repo-name.py)
GITHUB_MASTER_REF=master

# Our repository name (gop, ibp, etc..)
LSTORE_REPO=${GITHUB_REPO_NAME#lstore-}
# Our targeted tag name (ACCRE_0.0.1)
BARE_TAG="${GITHUB_HEAD_REF#release-proposed-}"
LSTORE_TAG="ACCRE_$BARE_TAG"

# At this point, we want to do 3 things:
#   - Merge the proposed release into accre-release
#   - Merge the accre-release into master
#   - Tag the just-created release commits with our chosen tag
#
# If those three things go smoothly, commit all three atomicly back into
#   the upstream, comment on the GH PR and delete the proposed release
#   branch (which automagically closes the PR)
set -x
TEMP_CHECKOUT=$(mktemp -d)
#trap "rm -rf $TEMP_CHECKOUT" EXIT
cd $TEMP_CHECKOUT

# Load the remotes
git init
git remote add head $GITHUB_HEAD_REPO
git remote add base $GITHUB_BASE_REPO
git fetch --all
git fetch head refs/heads/$GITHUB_HEAD_REF
git fetch base refs/heads/$GITHUB_BASE_REF refs/heads/$GITHUB_MASTER_REF

# Merge proposed into accre-release
git checkout -t remotes/base/$GITHUB_BASE_REF
git merge --no-ff -m "Release $BARE_TAG" head/$GITHUB_HEAD_REF

# Merge accre-release into master
git checkout -t base/$GITHUB_MASTER_REF
git merge --no-ff -m "Release $BARE_TAG" head/$GITHUB_HEAD_REF

# Tag the release. The '-u' option requires a GPG key to sign the release
# Note: git config --global user.signingkey <gpg-key-id> is your friend. You
# want to add the 'pub' key from 'gpg --list-keys'
note "If entering this passphrase hangs, you need to get gpg-agent working"
git tag -a -s $LSTORE_TAG -m "Release $LSTORE_TAG"

# Push everything back. Atomic is (unfortunately) a newer git feature. If this
# fails, update git
git push --atomic base $GITHUB_MASTER_REF $GITHUB_BASE_REF $LSTORE_TAG || \
    fatal "If git complains about --atomic, you need a newer git"
