#!/bin/bash

# Usage: propose-release.sh [opts] <branch name> <repository> [<repository ...]
#   Begins a new release branch for the given repository and opens a pull
#   request to track the progress of this proposed release.
#
# Options:
#   -o, --offline
#       Runs the script in 'offline' mode, which prevents interacting with
#       GitHub.


set -eu
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh

OFFLINE_MODE=false
OPTS=$(getopt -o o --long offline --name "$0" -- "$@")
eval set -- "$OPTS"

while true; do
    case "$1" in
        -o|--offline)
            OFFLINE_MODE=true
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            fatal "Unknown option: $1"
            break
            ;;
    esac
done

if [ "$OFFLINE_MODE" = false ]; then
    load_github_token
else
    note "Offline mode: not updating Github"
fi

PROPOSED_TAG=$1
shift
PACKAGES="$@"

# TODO: config this
UPSTREAM_REMOTE=origin
ORIGIN_REMOTE=origin
TARGET_BRANCH=accre-release

#
# To make a new release branch, we would like to do the following:
#
# * Pull the upstreams
# * Sanity check the local repo
# * Make a branch
# * Figure out the previous tag
# * Update the changelog
# * Commit the changelog and tag it
# * Push to GitHub
# * Open a Pull Request
#
# This should be nearly identical to producing a release candidate. Try and
# share functionality.
#
for REPO in $PACKAGES; do
    note "Examining $REPO"
    cd $LSTORE_RELEASE_BASE/source/$REPO
    RET="$(get_repo_status $REPO)"
    if [ "$OFFLINE_MODE" = false ]; then
        git fetch ${UPSTREAM_REMOTE}
        git fetch ${ORIGIN_REMOTE}
    fi
    PREVIOUS_TAG=$(git describe --tags --match 'ACCRE_*' --abbrev=0 \
                                        ${UPSTREAM_REMOTE}/${TARGET_BRANCH})
    PROPOSED_BRANCH="release-proposed-${PROPOSED_TAG}"
    if [ ! -z "$(git branch --list  $PROPOSED_BRANCH)" ]; then
        fatal "The requested release branch $PROPOSED_BRANCH already exists"
    else
        git checkout ${UPSTREAM_REMOTE}/$(get_repo_master $REPO) \
                            -b $PROPOSED_BRANCH
    fi

    create_release_candidate $PROPOSED_TAG $PREVIOUS_TAG $PROPOSED_BRANCH
    if [ "$OFFLINE_MODE" = false ]; then
        git push --atomic  ${ORIGIN_REMOTE}  \
                                "ACCRE_${PROPOSED_TAG}-rc1" $PROPOSED_BRANCH
        HEAD_REF=$(git show-ref refs/heads/$GIT)
        PREV_CHANGELOG_REF=$(git log --pretty=oneline -n 1 --skip 1 HEAD^ CHANGELOG.md \
                                    | awk '{ print $1 }')
        NEW_CHANGELOG=$(git diff --unified=0 CHANGELOG.md | grep '^[+]' | \
                                    grep -Ev '^(--- a/|\+\+\+ b/)' | sed 's/^\+//')
        CHANGELOG_JSON="$(echo -n "${NEW_CHANGELOG}" | \
                            $LSTORE_RELEASE_BASE/scripts/convert-to-json-string.py)"
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
        note "Pull request created. It can be found at"
        note "$GITHUB_URL"
        # TODO: Apply GH labels to release requests
        # TODO: Use GH status API to block 'merge button' and force our
        #       accept-release.sh script
    else
        note "New release configured locally"
    fi
done
