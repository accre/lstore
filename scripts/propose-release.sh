#!/bin/bash

# Proposes a local repository be turned into a new release

set -eu 
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
load_github_token

PROPOSED_TAG=$1
shift
PACKAGES="$@"

# TODO: config this
UPSTREAM_REMOTE=origin
ORIGIN_REMOTE=origin
TARGET_BRANCH=accre-release

cd $LSTORE_RELEASE_BASE

# TODO: Add ability to make releases for the lstore-release package as well
RELEASE=$(get_repo_status .)
RELEASE_GIT=${RELEASE% *}
RELEASE_CLEAN=${RELEASE##* }

cd $LSTORE_RELEASE_BASE/source
for REPO in $PACKAGES; do
    note "Examining $REPO"
    RET="$(get_repo_status $REPO)"
    cd $LSTORE_RELEASE_BASE/source/$REPO
    git fetch ${UPSTREAM_REMOTE}
    git fetch ${ORIGIN_REMOTE}
    PREVIOUS_TAG=$(git describe --tags --match 'ACCRE_*' ${UPSTREAM_REMOTE}/${TARGET_BRANCH} --abbrev=0)
    PROPOSED_URL=https://github.com/accre/lstore-${REPO}/tree/ACCRE_${PROPOSED_TAG}
    PROPOSED_DIFF=https://github.com/accre/lstore-${REPO}/compare/${PREVIOUS_TAG}...ACCRE_${PROPOSED_TAG}

    # Sanity check things look okay.
    GIT=${RET% *}
    CLEAN=${RET##* }
    if [ $CLEAN != "CLEAN" ]; then
        fatal "Package $REPO isn't clean."
    fi
    git show-ref "ACCRE_${PROPOSED_TAG}" &>/dev/null && \
            fatal "The release ${PROPOSED_TAG} already exists"
    
    PROPOSED_BRANCH="release-proposed-${PROPOSED_TAG}"
    git checkout $(get_repo_master $REPO)
    git branch -D $PROPOSED_BRANCH || true
    git checkout -b $PROPOSED_BRANCH
    
    # Update CHANGELOG.md
    NEW_CHANGELOG="# **[$PROPOSED_TAG]($PROPOSED_URL)** $(date '+(%F)')

## Changes ([full changelog]($PROPOSED_DIFF))
$(git log --oneline --no-merges  ${UPSTREAM_REMOTE}/${TARGET_BRANCH}..HEAD | \
    sed 's/^/*  /')


"
    echo -n "$NEW_CHANGELOG" > CHANGELOG.md
    CHANGELOG_JSON="$(cat CHANGELOG.md | \
                        $LSTORE_RELEASE_BASE/scripts/convert-to-json-string.py)"
    git show HEAD:CHANGELOG.md >> CHANGELOG.md
    git add CHANGELOG.md
    git commit CHANGELOG.md -m "Incrementing to $PROPOSED_TAG"
    git push $UPSTREAM_REMOTE $PROPOSED_BRANCH
    HEAD_REF=$(git show-ref refs/heads/$GIT)

    # TODO: Make repo changable
    GITHUB_POST='{"title":"[RELEASE] '$PROPOSED_TAG'", 
                  "head": "'$PROPOSED_BRANCH'",
                  "base": "'$TARGET_BRANCH'",
                  "body":'"$CHANGELOG_JSON"'}'
    GITHUB_REQUEST=$(echo -n "$GITHUB_POST" | \
        curl --request POST  \
        -H "Authorization: token $LSTORE_GITHUB_TOKEN"\
        --data-binary '@-' \
        https://api.github.com/repos/accre/lstore-${REPO}/pulls)
    GITHUB_URL=$(echo $GITHUB_REQUEST | \
                            $LSTORE_RELEASE_BASE/scripts/extract-pull-url.py)
    note "Pull request created. It can be found at
$GITHUB_URL"
    # TODO: Apply GH labels to release requests
    # TODO: Use GH status API to block 'merge button' and force our
    #       accept-release.sh script
done
