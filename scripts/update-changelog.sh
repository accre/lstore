#!/bin/bash

# Usage: update-changelog.sh <proposed tag> <prevous tag> [current commit]

# Updates changelog with the difference between two tags, where the latter tag
#   may not be tagged yet. Current commit

set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

PROPOSED_TAG=$1
PREVIOUS_TAG=$2
CURRENT_TAG=${3:-$(git rev-parse HEAD)}

# TODO: config this
UPSTREAM_REMOTE=origin
ORIGIN_REMOTE=origin
TARGET_BRANCH=accre-release

REPO=$(basename $(pwd))

PROPOSED_URL=https://github.com/accre/lstore-${REPO}/tree/ACCRE_${PROPOSED_TAG}
PROPOSED_DIFF=https://github.com/accre/lstore-${REPO}/compare/${PREVIOUS_TAG}...ACCRE_${PROPOSED_TAG}

# Update CHANGELOG.md
echo -n "# **[$PROPOSED_TAG]($PROPOSED_URL)** $(date '+(%F)')

## Changes ([full changelog]($PROPOSED_DIFF))
$(git log --oneline --no-merges  ${PREVIOUS_TAG}..${CURRENT_TAG} | \
sed 's/^/*  /')

" > CHANGELOG.md
git show HEAD:CHANGELOG.md >> CHANGELOG.md
