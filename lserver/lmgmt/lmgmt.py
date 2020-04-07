#!/usr/bin/env python3
"""
    LServer helper routines for performing file repairs and space rebalancing

    A config file is used to specify the locations for logs and
    the warmer database along with the location for LStore binaries.
    The default configuration file is /etc/lio/lmgmt.cfg.  The file format
    and defaults are as follows:

    [config]
    bin=/usr/bin

    [current]  #Most recent warmer output.  Could still be runnng
    log=/lio/log/warmer_run.log
    db=/lio/log/warm

    [previous] #Previously completed warmer outout
    log=/lio/log/warmer_run.log.2
    db=/lio/log/warm.2
"""

import re
import operator
import sys
import subprocess
import tempfile
import configparser

#Prefix to binaries if needed
BPREFIX = None

#Path to the warmer logs
path_warmer_log = None

#Path to the warmer RID DBs
path_warmer_db = None

#LMgmt config
cfg = configparser.ConfigParser()

#Path selection constants
WPATH_CURR = 1
WPATH_PREV = 2
WPATH_AUTO = 3

#Warmer Query constants
WQ_GOOD  = 1   # Only return files that have no missing allocations
WQ_BAD   = 2   # Only return files that have missing allocations
WQ_WRITE = 4   # Just return files with write errors
WQ_ALL   = 3   # Return everything

#***********************************************************************************

def convert_string2float(vstr):
    """
    Converts the string with optional units to bytes
    """

    match = re.search(r"(\d*\.\d+|\d+)(.*)", vstr)

    val = float(match.group(1))
    if (match.group(2) == None):
        return(val)

    base = 1000
    units = match.group(2).lower()

    if (len(units) == 2):
        if (match.group(2)[1] == "i"):
            base = 1024

    if (units[0] == "b"):
        val = val * 1
    elif (units[0] == "k"):
        val = val * base
    elif (units[0] == "m"):
        val = val * base * base
    elif (units[0] == "g"):
        val = val * base * base * base
    elif (units[0] == "t"):
        val = val * base * base * base * base

    return(val)

#***********************************************************************************

def options_add(parser):
    """
    Adds the lmgmt options to the args parser
    """

    exg = parser.add_mutually_exclusive_group()
    exg.add_argument("--current", help="Use the most recent warmer data", action="store_const", const=WPATH_CURR, dest='location')
    exg.add_argument("--prev", help="Use the previous warmer data", action="store_const", const=WPATH_PREV, dest='location')
    exg.add_argument("--config", help="Config file to use", action="store", dest='config_file')

#***********************************************************************************

def options_process(args):
    """
    Processes the lmgmt options. Loads the lmgmt config
    and sets the binary prefix

    :param argparse args: Arguments
    """

    global BPREFIX, cfg

    if (args.config_file):
        cfg.read(args.config_file)
    else:
        cfg.read("/etc/lio/lmgmt.cfg")

    BPREFIX = cfg['config'].get('bin', "/usr/bin") + '/'

    if (args.location != None):
        warmer_paths_set(args.location)
    else:
        warmer_paths_set(WPATH_AUTO)

#***********************************************************************************

def warmer_running():
    """
    Determines if the warmer is running

    :returns: True if a warmer is running and False otherwise
    """

    p = subprocess.run(["pgrep", "lio_warm"], stdout=subprocess.DEVNULL)
    if p.returncode == 0:
     	return(True)

    return(False)

#***********************************************************************************

def warmer_paths_set(mode):
    """
    Sets the wamrer log and db paths

    :param int mode:  WPATH_PREV, WPATH_CURR, or WPATH_AUTO for the previous, current,
                      or automatically pick the most recently completed run.
    """

    global path_warmer_log, path_warmer_db, cfg

    if mode == WPATH_AUTO:
        if warmer_running():
            mode = WPATH_PREV
        else:
            mode = WPATH_CURR

    if mode == WPATH_CURR:
        path_warmer_log = cfg['current'].get('log', "/lio/log/warmer_run.log")
        path_warmer_db = cfg['current'].get('db', "/lio/log/warm")
    else:
        path_warmer_log = cfg['previous'].get('log', "/lio/log/warmer_run.log.2")
        path_warmer_db = cfg['previous'].get('db', "/lio/log/warm.2")

    return

#***********************************************************************************

def warmer_rm_cleanup(debug):
    """
    Remove files that are unrecoverable.

    :param bool debug:  If True just cat the files list to the stdout and don't
                        actually delete any files.
    """

    #Form the task and launch it
    task = [BPREFIX + "lio_rm", "-"]

    #If debug just echo the text
    if (debug):
        task = ["cat"]

    p = subprocess.Popen(task, stdin=subprocess.PIPE)

    rm_match = ['on 9 out of 9',   'on 8 out of 9',
                'on 18 out of 18', 'on 17 out of 18',
                'on 27 out of 27', 'on 26 out of 27' ]

    with open(path_warmer_log, "r") as f:
        for line in f:
            if 'Failed' in line:
                for rm in rm_match:
                    if rm in line:
                        fname = line.split()[3] + "\n"
                        p.stdin.write(fname)
                        break

    return

#***********************************************************************************

def warmer_errors_rid_table():
    """
    Create a table containing all the RIDs with errors and the number of bad files for each RID

    :returns: A dict where each key is the RID and the value is the number of bad files
    """

    rid_table = dict()
    regex = re.compile(r'cap=ibp://[\w.\-]*:\d*/(\w*)#')

    with open(path_warmer_log, "r") as f:
        for line in f:
            if 'ERROR:' in line:
                rid = regex.search(line)
                if rid:
                    if rid.group(1) in rid_table:
                        rid_table[rid.group(1)] += 1
                    else:
                        rid_table[rid.group(1)] = 1

    return(rid_table)

#***********************************************************************************

def warmer_errors_rid_table_print():
    """
    Prints the RID error table to stdout (RID, #files)
    """

    rids = warmer_errors_rid_table()
    sorted_rids = sorted(rids.items(), key=operator.itemgetter(1))

    for r in sorted_rids:
        print(r[0], r[1])
    return

#***********************************************************************************

def warmer_errors_write():
    """
    Generate a list of files with write errors

    :returns: List of files with write errors
    """

    w = []
    regex = re.compile(r'WRITE_ERROR for file (.*)')

    with open(path_warmer_log, "r") as f:
        for line in f:
            if 'WRITE_ERROR for file' in line:
                fname = regex.search(line)
                w.append(fname.group(1))
    return(w)

#***********************************************************************************

def warmer_errors_write_print(prefix):
    """
    Print all the files with write errors to stdout, optionally prepending the supplied prefix

    :param str prefix:  Optional prefix to prepend to each name
    """
    w = warmer_errors_write()
    for fname in w:
        print("{}{}".format(prefix,fname))
    return


#***********************************************************************************

def warmer_errors_rid_fname():
    """
    Generate a list of files with RID errors

    :returns: List of files with RID errors
    """

    flist = dict()
    regex = re.compile(r'^ERROR: (.*) cap=ibp')

    with open(path_warmer_log, "r") as f:
        for line in f:
            fname = regex.search(line)
            if (fname):
                flist[fname.group(1)] = 1
    return(flist)

#***********************************************************************************

def warmer_errors_rid_fname_print(prefix):
    """
    Print all the files with RID errors to stdout, optionally prepending the supplied prefix

    :param str prefix:  Optional prefix to prepend to each name
    """
    f = warmer_errors_rid_fname()
    for fname in f.keys():
        print("{}{}".format(prefix,fname))
    return

#***********************************************************************************

def warmer_query(rid, mode, total_bytes):
    """
    Wrapper around the LStore warmer_query command for a RID

    :param str rid:  RID to use
    :param int mode: Selection mode: WQ_GOOD, WQ_BAD, WQ_WRITE, or WQ_ALL
    :param int total_bytes:  If > 0 then stop when the total bytes found
                     exceed this amount.  Otherwise return everything.

    :returns: The supprocess handle to the task
    """

    #Form the base task
    task = [ BPREFIX + "warmer_query", "-db", path_warmer_db, "-r", rid]

    #add the mode if needed
    if ((mode & WQ_ALL) == 0):
        if (mode & WQ_GOOD):
            task.append("-s")
        if (mode & WQ_BAD):
            task.append("-f")
        if (mode & WQ_WRITE):
            task.append("-w")

    #Add the size if needed
    if (total_bytes > 0):
        task.extend(["-b", str(total_bytes)])

    #Launch the process
    p = subprocess.Popen(task, stdout=subprocess.PIPE)

    return(p)

#***********************************************************************************

def warmer_query_print(rid, mode, total_bytes, fname_only, prefix):
    """
    Prints the RID warmer query to stdout

    :param str rid:  RID to use
    :param int mode: Selection mode: WQ_GOOD, WQ_BAD, WQ_WRITE, or WQ_ALL
    :param int total_bytes:  If > 0 then stop when the total bytes found
                     exceed this amount.  Otherwise return everything.
    :param str prefix: Prepend this to each filename
    """

    if prefix == None:
         prefix = ""
    p = warmer_query(rid, mode, total_bytes)
    while True:
        line = p.stdout.readline().decode("utf-8")
        if line != '':
            if fname_only:
                print("{}{}".format(prefix, line.split("|")[0]))
            else:
                print(line.rstrip())
        else:
            break
    return

#***********************************************************************************

def rid_rebalance_table():
    """
    Generate a list of RIDs and the space on each that needs to be shuffled off
    to have all RIDs using the same fraction of space

    :returns: List containing [RID, size] tuples.
    """

    regex = re.compile(r'RID:[\w.\-]*:\d*/(\w*) *DELTA: *-([\w.]*) ')

    p = None  #Signifies an ERROR occured

    #Make the rebalance config.  We don't use the temporary file routines
    #because they can't reliably pass the file to a sub process.
    with open("/tmp/inspect.cfg", "w") as fd:
        fd.write("[group-shuffle]\n _delta = auto\n _tolerance = 1%\n _unspecified\n\n")
        fd.write("[pool-redo]\n _group=shuffle\n")
        fd.flush();

        task = [ BPREFIX + "lio_inspect", "-pp", "1", "-h", "-pc", fd.name]

        #Launch the process
        p = subprocess.Popen(task, stdout=subprocess.PIPE)
        rid = []
        for line in p.stdout:
            line = line.decode("utf-8")
            if "STATE: 0" in line:
                #print "1:line=", line
                info = regex.search(line)
                if info != None:
                    rid.append([info.group(1), convert_string2float(info.group(2))]);

    return(rid)

#***********************************************************************************

def warmer_query_add(rid, size, files):
    """
    Add files using the RID that total up to 'size'

    :param str rid: RID to use
    :param int size:  Max number of bytes from RID to add
    :param list files: File list to add entries
    """

    p = warmer_query(rid, WQ_ALL, size)
    while True:
        line = p.stdout.readline().decode("utf-8")
        if line != '':
            files[line.split("|")[0]] = 1
        else:
            break
    return

#***********************************************************************************

def rid_rebalance_file_list(rid_table):
    """
    Generate a list of files to use for rebalancing based on the supplied RID table

    :param list rid_table: List or [RID, size] tuples to use in generating the files
    :returns: File list
    """

    files = dict()

    for r in rid_table:
        warmer_query_add(r[0], r[1], files)

    return(files)

#***********************************************************************************

def rid_rebalance_file_list_print(files):
    """
    Print the files used for rebalancing

    :param dict files: List of files
    """

    for fname in files:
        print(fname)
    return

