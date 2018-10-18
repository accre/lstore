#!/usr/bin/env python3

import lmgmt
import argparse
import sys

parser = argparse.ArgumentParser(description="Generate file lists.")
lmgmt.options_add(parser)

parser.add_argument("--fname", help="Just output the filename", action="store_true", dest='fname')
parser.add_argument("--prefix", help="Filname prefix to add", action="store", dest='prefix')

exg = parser.add_mutually_exclusive_group()
exg.add_argument("--rid", help="List files that use the RID", action="store", dest='rid')
exg.add_argument("--rebalance", help="Rebalance data across all RIDs", action="store_true", dest='rebalance')

args = parser.parse_args()
lmgmt.options_process(args)

if (args.rebalance):
    rt = lmgmt.rid_rebalance_table()
    files = lmgmt.rid_rebalance_file_list(rt)
    lmgmt.rid_rebalance_file_list_print(files)
elif (args.rid != None):
    lmgmt.warmer_query_print(args.rid, lmgmt.WQ_ALL, 0, args.fname, args.prefix)
else:
    parser.print_help()
