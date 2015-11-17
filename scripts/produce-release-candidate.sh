#!/bin/bash

# Proposes a local repository be turned into a new release, beginning with -rc1

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


PROPOSED_TAG=$1
shift
PACKAGES="$@"

# TODO: config this
UPSTREAM_REMOTE=origin
ORIGIN_REMOTE=origin
TARGET_BRANCH=accre-release

for REPO in $PACKAGES; do
    note "Examining $REPO"
    cd $LSTORE_RELEASE_BASE/source/$REPO
    if [ "$OFFLINE_MODE" = false ]; then
        git fetch ${UPSTREAM_REMOTE}
        git fetch ${ORIGIN_REMOTE}
    fi
    TAG_STRIPPED=${PROPOSED_TAG%-*}
    PROPOSED_BRANCH="release-proposed-${TAG_STRIPPED}"
    PREVIOUS_TAG=$(git describe --tags --match 'ACCRE_*' \
                                                ${PROPOSED_BRANCH} --abbrev=0)
    create_release_candidate $PROPOSED_TAG $PREVIOUS_TAG $PROPOSED_BRANCH
    if [ "$OFFLINE_MODE" = false ]; then
        git push $UPSTREAM_REMOTE $PROPOSED_BRANCH
    fi
done
