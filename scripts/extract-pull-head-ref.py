#!/usr/bin/env python

import json
import sys

reply = json.loads(sys.stdin.read())
print reply['head']['ref']
