#!/bin/bash

OWNER="root"

mkdir -p osfile/file/_^FA^_  osfile/hardlink/{{0..255},_^FA^_}/_^FA^_

echo $OWNER > osfile/file/_^FA^_/system.owner
echo $OWNER > osfile/hardlink/_^FA^_/system.owner

cp system.exnode.sample osfile/file/_^FA^_/system.exnode
cp system.exnode.sample osfile/hardlink/_^FA^_/system.exnode


