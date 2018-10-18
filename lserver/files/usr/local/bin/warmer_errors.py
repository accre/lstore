#!/usr/bin/env python3
"""
Print warmer errors. The default is to print a summary table of RID errors.
"""

import lmgmt
import argparse

parser = argparse.ArgumentParser(description="Print warmer errors. The default is to print a summary table of RID errors")
lmgmt.options_add(parser)

group = parser.add_mutually_exclusive_group(required=False)
group.add_argument("-w", "--write", help="Print files containing write errors instead of RID errors.", action="store_true", dest='write_mode')
group.add_argument("-r", "--rid", help="Print filenames containing  RID errors.", action="store_true", dest='rid_mode')

args = parser.parse_args()

lmgmt.options_process(args)

if (args.write_mode):
    lmgmt.warmer_errors_write_print('@:')
elif (args.rid_mode):
    lmgmt.warmer_errors_rid_fname_print('@:')
else:
    lmgmt.warmer_errors_rid_table_print()

