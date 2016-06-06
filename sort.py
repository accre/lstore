#!/usr/bin/env python

import re
import sys

sort = {}
regex = re.compile(r'TBX_API \w* \*?(\w*)\(.*')
for line in sys.stdin.readlines():
    result = regex.match(line)
    if not result:
        sort[line] = line
    else:
        sort[result.group(1)] = line

for k in sorted(sort.keys()):
    sys.stdout.write(sort[k])
