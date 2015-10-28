#!/bin/bash

# Given a GH pull request, cut a new release tag, merge to the release branch
#   and the master branch
# Usage:
set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
note "Attempting to tag and merge $RELEASE_URL"
load_github_token
# TODO: test that GPG is set up properly

# TODO: config this
GH_ORGANIZATION=accre
RELEASE_URL=$1

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
#   - Tag the just-created release commits with our chosen tag
#   - Merge the accre-release into master
#
# If those three things go smoothly, commit all three atomicly back into
#   the upstream, comment on the GH PR and delete the proposed release
#   branch (which automagically closes the PR)
# In GH the ref names are defined as follows:
#   - HEAD: PR branch we're wanting to merge FROM
#   - MASTER: Master branch we're wanting to merge TO
#   - BASE: Release branch we're wanting to merge TO

TEMP_CHECKOUT=$(mktemp -d)
note "Checked out to $TEMP_CHECKOUT"
trap "rm -rf $TEMP_CHECKOUT" EXIT
cd $TEMP_CHECKOUT

# Load the remotes
git init
git remote add head $GITHUB_HEAD_REPO
git remote add base $GITHUB_BASE_REPO
git fetch head refs/heads/$GITHUB_HEAD_REF
git fetch base refs/heads/$GITHUB_BASE_REF refs/heads/$GITHUB_MASTER_REF

# Merge proposed into accre-release
note "Merging proposed release into accre-release"
git checkout -t remotes/base/$GITHUB_BASE_REF -b accre-release
COMMIT_HASH=$(git rev-parse HEAD)
COMMIT_MESSAGE=$(git log -1 --pretty=%B ${COMMIT_HASH})

# Tag the release. The '-u' option requires a GPG key to sign the release
#   Note: git config --global user.signingkey <gpg-key-id> is your friend. You
#   want to add the 'pub' key from 'gpg --list-keys'
note "If entering this passphrase hangs, you need to get gpg-agent working"
# FIXME: Needs a recipe for OSX to reenable -s instead of -a
git tag -a $LSTORE_TAG -m "Release $LSTORE_TAG"

# Merge accre-release into master
note "Merging master into accre-release"
git checkout -t base/$GITHUB_MASTER_REF -b master
git merge --no-ff -e -m "Release $LSTORE_TAG"  head/$GITHUB_HEAD_REF
echo -n "$COMMIT_MESSAGE" | \
    git merge --no-ff -F - head/$GITHUB_HEAD_REF

# Push everything back. Atomic is (unfortunately) a newer git feature. If this
# fails, update git
git push --atomic base $GITHUB_MASTER_REF $GITHUB_BASE_REF $LSTORE_TAG || \
        fatal "If git complains about --atomic, you need a newer git"
note "Checked out to $TEMP_CHECKOUT"
