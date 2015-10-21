#!/usr/bin/env python

import json
import sys
to_convert = sys.stdin.read()
print json.dumps(to_convert, separators=(',',':'))
