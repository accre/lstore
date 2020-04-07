#!/usr/bin/env python

import psutil
import re
import operator
import sys
import subprocess
import tempfile

#Prefix to binaries if needed
BPREFIX = "/usr/bin/"

#Path to the warmer logs
path_warmer_log_curr = "/vols/log/warmer_run.log"
path_warmer_log_prev = "/vols/log/warmer_run.log.2"
path_warmer_log = None

#Path to the warmer RID DBs
path_warmer_db_curr = "/vols/log/warm"
path_warmer_db_prev = "/vols/log/warm.2"
path_warmer_db = None

#Path selection constants
WPATH_CURR = 1
WPATH_PREV = 2
WPATH_AUTO = 3

#Warmer Query constants
WQ_GOOD  = 1   # Only return files that have no missing allocations
WQ_BAD   = 2   # Only return files that have missing allocations
WQ_WRITE = 4   # Just return files with write errors
WQ_ALL   = 3   # Return everything

def options_add(parser):
    exg = parser.add_mutually_exclusive_group()
    exg.add_argument("--current", help="Use the most recent warmer data", action="store_const", const=WPATH_CURR, dest='location')
    exg.add_argument("--prev", help="Use the previous warmer data", action="store_const", const=WPATH_PREV, dest='location')

def options_process(args):
    if (args.location != None):
        warmer_paths_set(args.location)

def warmer_running():
    for p in psutil.process_iter():
        try:
            pinfo = p.as_dict(attrs=['pid', 'name'])
        except psutil.NoSuchProcess:
            pass
        else:
            if 'lio_warm' in pinfo['name']:
                return(True)

    return(False)


def warmer_paths_set(mode):
    global path_warmer_log, path_warmer_db

    if mode == WPATH_AUTO:
        if warmer_running():
            mode = WPATH_PREV
        else:
            mode = WPATH_CURR

    if mode == WPATH_CURR:
        path_warmer_log = path_warmer_log_curr
        path_warmer_db = path_warmer_db_curr
    else:
        path_warmer_log = path_warmer_log_prev
        path_warmer_db = path_warmer_db_prev

    return

def warmer_rm_cleanup(debug):
    #Form the task and launch it
    task = [BPREFIX + "lio_rm", "-"]

    #If debug just echo the text
    if (debug == 1):
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

def warmer_errors_rid():
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

def warmer_errors_rid_print():
    rids = warmer_errors_rid()
    sorted_rids = sorted(rids.items(), key=operator.itemgetter(1))

    for r in sorted_rids:
        print r[1], r[0]
    return

def warmer_errors_write():
    w = []
    regex = re.compile(r'WRITE_ERROR for file (.*)')

    with open(path_warmer_log, "r") as f:
        for line in f:
            if 'WRITE_ERROR for file' in line:
                fname = regex.search(line)
                w.append(fname.group(1))
    return(w)

def warmer_errors_write_print(prefix):
    w = warmer_errors_write()
    for fname in w:
        print prefix + fname
    return

def warmer_query(rid, mode, total_bytes):
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
        task.extend(["-b", total_bytes])

    #Launch the process
    p = subprocess.Popen(task, stdout=subprocess.PIPE)

    return(p)

def warmer_query_print(rid, mode, total_bytes, fname_only, prefix):
    if prefix == None:
         prefix = ""
    p = warmer_query(rid, mode, total_bytes)
    while True:
        line = p.stdout.readline()
        if line != '':
            if fname_only:
                print("%s%s" % (prefix, line.split("|")[0]))
            else:
                print line.rstrip()
        else:
            break
    return

def rid_rebalance_table():
    regex = re.compile(r'RID:[\w.\-]*:\d*/(\w*) *DELTA: *-([\w.]*) ')

    p = None  #Signifies an ERROR occured

    #Make the rebalance config.  We don't use the temporary file routines
    #because they can't reliably pass the file to a sub process.
    with open("/tmp/inspect.log", "w") as fd:
        fd.write("[group-shuffle]\n _delta = auto\n _tolerance = 1%\n _unspecified\n\n")
        fd.write("[pool-redo]\n _group=shuffle\n")
        fd.flush();

        task = [ BPREFIX + "lio_inspect", "-pp", "1", "-h", "-pc", fd.name]

        #Launch the process
        p = subprocess.Popen(task, stdout=subprocess.PIPE)
        rid = []
        for line in p.stdout:
            if "STATE: 0" in line:
                #print "1:line=", line
                info = regex.search(line)
                if info != None:
                    rid.append([info.group(1), info.group(2)]);

    return(rid)


def warmer_query_add(rid, size, files):
    p = warmer_query(rid, WQ_ALL, size)
    while True:
        line = p.stdout.readline()
        if line != '':
            files[line.split("|")[0]] = 1
        else:
            break
    return

def rid_rebalance_file_list(rid_table):
    files = dict()

    for r in rid_table:
        warmer_query_add(r[0], r[1], files)

    return(files)

def rid_rebalance_file_list_print(files):
    for fname in files:
        print fname
    return

#-----------------------
warmer_paths_set(WPATH_AUTO)
#print "curr";
#print "log:", path_warmer_log
#print "db:", path_warmer_log

#warmer_errors_rid_print()
#warmer_errors_write_print("@:")#Form the base task
#warmer_query_print("3099", WQ_ALL, "1024000")
#rt = rid_rebalance_table()
#files = rid_rebalance_file_list(rt)
#rid_rebalance_file_list_print(files)

