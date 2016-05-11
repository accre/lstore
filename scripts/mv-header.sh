#!/bin/bash

for HEADER in toolbox/*.h; do
    HEADER=$(basename $HEADER)
    ( cd toolbox
        sed -i '' 's,#include "'$HEADER'",#include "tbx/'$HEADER'",' *.{c,h}
        sed -i '' 's,'$HEADER',tbx/'$HEADER',' CMakeLists.txt
    )
    for DIR in ibp gop lio; do
        ( cd $DIR
            sed -i '' 's,#include "'$HEADER'",#include <tbx/'$HEADER'>,' *.{c,h}
        )
    done
done
#for HEADER in toolbox/*.h; do
#    HEADER=$(basename $HEADER)
#    (cd toolbox
#        git mv $HEADER tbx/$HEADER
#    )
#done
