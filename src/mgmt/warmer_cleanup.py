#!/usr/bin/env python

import lmgmt
import argparse
import sys

parser = argparse.ArgumentParser(description="Remove unrecoverable files.")
lmgmt.options_add(parser)

parser.add_argument("-y", "--yes", help="Don't ask first to remove the files.  Just do it.", action="store_true", dest='mode')
parser.add_argument("--test", help="Don't actually remove the files. Just print them.", action="store_true", dest='debug')

args = parser.parse_args()
lmgmt.options_process(args)

if ((args.mode == False) & (args.debug == False)):
    response = raw_input('Remove unrecoverable files(y/n)? ')
    if (response != "y"):
        sys.exit()

lmgmt.warmer_rm_cleanup(args.debug)

if (args.debug == False):
	print "\n"
