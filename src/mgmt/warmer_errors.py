#!/usr/bin/env python

import lmgmt
import argparse

parser = argparse.ArgumentParser(description="Print warmer errors. The default is to print a summary table of RID errors")
lmgmt.options_add(parser)

parser.add_argument("-w", "--write", help="Print Files containing write errors instead of RID errors.", action="store_true", dest='mode')

args = parser.parse_args()

lmgmt.options_process(args)

if (args.mode):
    lmgmt.warmer_errors_write_print('@:')
else:
    lmgmt.warmer_errors_rid_print()

