#!/bin/bash

# @see https://stubbisms.wordpress.com/2009/07/10/git-script-to-show-largest-pack-objects-and-trim-your-waist-line/
# @author Antony Stubbs

# Sentinel
PATCH_OK=0

# set the internal field spereator to line break, so that we can iterate easily over the verify-pack output
IFS=$'\n';

# list all objects including their size, sort by size, take top 10
objects=`git verify-pack -v .git/objects/pack/pack-*.idx | grep -v chain | sort -k3nr | head -n 30`

# Big blobs to ignore -- from externals
IGNORE_HASHES="58346973a5f2b4894cb8cd3fc95702ef1b49f882
               030e77766ab1ac10f4eaebb8786b7604e76b7d75
               44e0ecff11e3a16ca7656be45268d0bd597afc51
               63ae69dc6fecaf83c52fba2ad334f4b1369fb1cd
               1d7024e733f9360ae6b71cc48c2d461e5541744d
               2873e1cb3ef751c4c620df02cf4e49e2ca3f5105
               8ab7eab67fe3b40e3cae2ba658f01e40e5bde6b6"

output="size pack SHA location"
allObjects=`git rev-list --objects HEAD`
for y in $objects
do
    # extract the size in bytes
    size=$((`echo $y | cut -f 5 -d ' '`/1024))
    # extract the compressed size in bytes
    compressedSize=$((`echo $y | cut -f 6 -d ' '`/1024))
    # extract the SHA
    sha=`echo $y | cut -f 1 -d ' '`
    # Ignore old giant ref
    if [[ "$IGNORE_HASHES" =~ "$sha" ]]; then
        continue
    fi
    # find the objects location in the repository tree
    other=$(echo "${allObjects}" | grep "$sha")
    if [ -z "$other" ]; then
        continue
    fi
    # Numbers stolen from initial commit
    if [[ "$size" -gt 175 || "$compressedSize" -gt 100 ]]; then
        output="${output}\n${size} ${compressedSize} ${other}"
        PATCH_OK=1
    fi
done

if [ $PATCH_OK -ne 0 ]; then
    echo "Error! The following objects are too large"
    echo -e $output
    exit 1
fi
