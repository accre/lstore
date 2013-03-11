/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/ 

//*****************************************************
// ibp_tool - IBP utility to get various IBP constants
//      and also manually execute individual IBP commands
//*****************************************************

#define _log_module_index 141

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include "network.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "ibp.h"

#define table_len(t) (sizeof(t) / sizeof(char *))

const char *_ibp_error_map[IBP_MAX_ERROR];
const char *_ibp_subcmd_map[45+1];
const char *_ibp_st_map[6];
const char *_ibp_rel_map[3];
const char *_ibp_type_map[5];
const char *_ibp_ctype_map[3];
const char *_ibp_captype_map[4];
const char *_ibp_command_map[IBP_MAX_NUM_CMDS+1];

ibp_context_t *ic = NULL;

//*************************************************************************
// init_tables - Initialize the constatnt tables
//*************************************************************************

void init_tables()
{
   memset(_ibp_error_map, 0, sizeof(_ibp_error_map));
   memset(_ibp_subcmd_map, 0, sizeof(_ibp_subcmd_map));
   memset(_ibp_st_map, 0, sizeof(_ibp_st_map));
   memset(_ibp_type_map, 0, sizeof(_ibp_type_map));
   memset(_ibp_ctype_map, 0, sizeof(_ibp_ctype_map));
   memset(_ibp_rel_map, 0, sizeof(_ibp_rel_map));
   memset(_ibp_captype_map, 0, sizeof(_ibp_rel_map));
   memset(_ibp_command_map, 0, sizeof(_ibp_command_map));

   _ibp_error_map[0] = "IBP_OK";
   _ibp_error_map[-IBP_E_GENERIC] = "IBP_E_GENERIC";
   _ibp_error_map[-IBP_E_SOCK_READ] = "IBP_E_SOCK_READ";
   _ibp_error_map[-IBP_E_SOCK_WRITE] = "IBP_E_SOCK_WRITE";
   _ibp_error_map[-IBP_E_CAP_NOT_FOUND] = "IBP_E_CAP_NOT_FOUND";
   _ibp_error_map[-IBP_E_CAP_NOT_WRITE] = "IBP_E_CAP_NOT_WRITE";
   _ibp_error_map[-IBP_E_CAP_NOT_READ] = "IBP_E_CAP_NOT_READ";
   _ibp_error_map[-IBP_E_CAP_NOT_MANAGE] = "IBP_E_CAP_NOT_MANAGE";
   _ibp_error_map[-IBP_E_INVALID_WRITE_CAP] = "IBP_E_INVALID_WRITE_CAP";
   _ibp_error_map[-IBP_E_INVALID_READ_CAP] = "IBP_E_INVALID_READ_CAP";
   _ibp_error_map[-IBP_E_INVALID_MANAGE_CAP] = "IBP_E_INVALID_MANAGE_CAP";
   _ibp_error_map[-IBP_E_WRONG_CAP_FORMAT] = "IBP_E_WRONG_CAP_FORMAT";
   _ibp_error_map[-IBP_E_CAP_ACCESS_DENIED] = "IBP_E_CAP_ACCESS_DENIED";
   _ibp_error_map[-IBP_E_CONNECTION] = "IBP_E_CONNECTION";
   _ibp_error_map[-IBP_E_FILE_OPEN] = "IBP_E_FILE_OPEN";
   _ibp_error_map[-IBP_E_FILE_READ] = "IBP_E_FILE_READ";
   _ibp_error_map[-IBP_E_FILE_WRITE] = "IBP_E_FILE_WRITE";
   _ibp_error_map[-IBP_E_FILE_ACCESS] = "IBP_E_FILE_ACCESS";
   _ibp_error_map[-IBP_E_FILE_SEEK_ERROR] = "IBP_E_FILE_SEEK_ERROR";
   _ibp_error_map[-IBP_E_WOULD_EXCEED_LIMIT] = "IBP_E_WOULD_EXCEED_LIMIT";
   _ibp_error_map[-IBP_E_WOULD_DAMAGE_DATA] = "IBP_E_WOULD_DAMAGE_DATA";
   _ibp_error_map[-IBP_E_BAD_FORMAT] = "IBP_E_BAD_FORMAT";
   _ibp_error_map[-IBP_E_TYPE_NOT_SUPPORTED] = "IBP_E_TYPE_NOT_SUPPORTED";
   _ibp_error_map[-IBP_E_RSRC_UNAVAIL] = "IBP_E_RSRC_UNAVAIL";
   _ibp_error_map[-IBP_E_INTERNAL] = "IBP_E_INTERNAL";
   _ibp_error_map[-IBP_E_INVALID_CMD] = "IBP_E_INVALID_CMD";
   _ibp_error_map[-IBP_E_WOULD_BLOCK] = "IBP_E_WOULD_BLOCK";
   _ibp_error_map[-IBP_E_PROT_VERS] = "IBP_E_PROT_VERS";
   _ibp_error_map[-IBP_E_LONG_DURATION] = "IBP_E_LONG_DURATION";
   _ibp_error_map[-IBP_E_WRONG_PASSWD] = "IBP_E_WRONG_PASSWD";
   _ibp_error_map[-IBP_E_INVALID_PARAMETER] = "IBP_E_INVALID_PARAMETER";
   _ibp_error_map[-IBP_E_INV_PAR_HOST] = "IBP_E_INV_PAR_HOST";
   _ibp_error_map[-IBP_E_INV_PAR_PORT] = "IBP_E_INV_PAR_PORT";
   _ibp_error_map[-IBP_E_INV_PAR_ATDR] = "IBP_E_INV_PAR_ATDR";
   _ibp_error_map[-IBP_E_INV_PAR_ATRL] = "IBP_E_INV_PAR_ATRL";
   _ibp_error_map[-IBP_E_INV_PAR_ATTP] = "IBP_E_INV_PAR_ATTP";
   _ibp_error_map[-IBP_E_INV_PAR_SIZE] = "IBP_E_INV_PAR_SIZE";
   _ibp_error_map[-IBP_E_INV_PAR_PTR]= "IBP_E_INV_PAR_PTR";
   _ibp_error_map[-IBP_E_ALLOC_FAILED] = "IBP_E_ALLOC_FAILED";
   _ibp_error_map[-IBP_E_TOO_MANY_UNITS] = "IBP_E_TOO_MANY_UNITS";
   _ibp_error_map[-IBP_E_SET_SOCK_ATTR] = "IBP_E_SET_SOCK_ATTR";
   _ibp_error_map[-IBP_E_GET_SOCK_ATTR] = "IBP_E_GET_SOCK_ATTR";
   _ibp_error_map[-IBP_E_CLIENT_TIMEOUT] = "IBP_E_CLIENT_TIMEOUT";
   _ibp_error_map[-IBP_E_UNKNOWN_FUNCTION] = "IBP_E_UNKNOWN_FUNCTION";
   _ibp_error_map[-IBP_E_INV_IP_ADDR] = "IBP_E_INV_IP_ADDR";
   _ibp_error_map[-IBP_E_WOULD_EXCEED_POLICY] = "IBP_E_WOULD_EXCEED_POLICY";
   _ibp_error_map[-IBP_E_SERVER_TIMEOUT] = "IBP_E_SERVER_TIMEOUT";
   _ibp_error_map[-IBP_E_SERVER_RECOVERING] = "IBP_E_SERVER_RECOVERING";
   _ibp_error_map[-IBP_E_CAP_DELETING] = "IBP_E_CAP_DELETING";
   _ibp_error_map[-IBP_E_UNKNOWN_RS] = "IBP_E_UNKNOWN_RS";
   _ibp_error_map[-IBP_E_INVALID_RID] = "IBP_E_INVALID_RID";
   _ibp_error_map[-IBP_E_NFU_UNKNOWN] = "IBP_E_NFU_UNKNOWN";
   _ibp_error_map[-IBP_E_NFU_DUP_PARA] = "IBP_E_NFU_DUP_PARA";
   _ibp_error_map[-IBP_E_QUEUE_FULL] = "IBP_E_QUEUE_FULL";
   _ibp_error_map[-IBP_E_CRT_AUTH_FAIL] = "IBP_E_CRT_AUTH_FAIL";
   _ibp_error_map[-IBP_E_INVALID_CERT_FILE] = "IBP_E_INVALID_CERT_FILE";
   _ibp_error_map[-IBP_E_INVALID_PRIVATE_KEY_PASSWD] = "IBP_E_INVALID_PRIVATE_KEY_PASSWD";
   _ibp_error_map[-IBP_E_INVALID_PRIVATE_KEY_FILE] = "IBP_E_INVALID_PRIVATE_KEY_FILE";
   _ibp_error_map[-IBP_E_AUTHEN_NOT_SUPPORT] = "IBP_E_AUTHEN_NOT_SUPPORT";
   _ibp_error_map[-IBP_E_AUTHENTICATION_FAILED] = "IBP_E_AUTHENTICATION_FAILED";
   _ibp_error_map[-IBP_E_INVALID_HOST] = "IBP_E_INVALID_HOST";
   _ibp_error_map[-IBP_E_CANT_CONNECT] = "IBP_E_CANT_CONNECT";
   _ibp_error_map[-IBP_E_CHKSUM] = "IBP_E_CHKSUM";
   _ibp_error_map[-IBP_E_CHKSUM_TYPE] = "IBP_E_CHKSUM_TYPE";
   _ibp_error_map[-IBP_E_CHKSUM_BLOCKSIZE] = "IBP_E_CHKSUM_BLOCKSIZE";
   _ibp_error_map[-IBP_E_OUT_OF_SOCKETS] = "IBP_E_OUT_OF_SOCKETS";

   _ibp_subcmd_map[IBP_PROBE] = "IBP_PROBE";
   _ibp_subcmd_map[IBP_INCR] = "IBP_INCR";
   _ibp_subcmd_map[IBP_DECR] = "IBP_DECR";
   _ibp_subcmd_map[IBP_CHNG] = "IBP_CHNG";
   _ibp_subcmd_map[IBP_CONFIG] = "IBP_CONFIG";

   _ibp_st_map[IBP_ST_INQ] = "IBP_ST_INQ";
   _ibp_st_map[IBP_ST_CHANGE] = "IBP_ST_CHANGE";
   _ibp_st_map[IBP_ST_RES] = "IBP_ST_RES";
   _ibp_st_map[IBP_ST_STATS] = "IBP_ST_STATS";
   _ibp_st_map[IBP_ST_VERSION] = "IBP_ST_VERSION";

   _ibp_rel_map[IBP_SOFT] = "IBP_SOFT";
   _ibp_rel_map[IBP_HARD] = "IBP_HARD";

   _ibp_type_map[IBP_BYTEARRAY] = "IBP_BYTEARRAY";
   _ibp_type_map[IBP_BUFFER] = "IBP_BUFFER";
   _ibp_type_map[IBP_FIFO] = "IBP_FIFO";
   _ibp_type_map[IBP_CIRQ] = "IBP_CIRQ";

   _ibp_ctype_map[IBP_TCP] = "IBP_TCP";
   _ibp_ctype_map[IBP_PHOEBUS] = "IBP_PHOEBUS";

   _ibp_captype_map[IBP_READCAP] = "IBP_READCAP";
   _ibp_captype_map[IBP_WRITECAP] = "IBP_WRITECAP";
   _ibp_captype_map[IBP_MANAGECAP] = "IBP_MANAGECAP";

   _ibp_command_map[IBP_ALLOCATE] = "IBP_ALLOCATE";
   _ibp_command_map[IBP_STORE] = "IBP_STORE";
   _ibp_command_map[IBP_STATUS] = "IBP_STATUS";
   _ibp_command_map[IBP_SEND] = "IBP_SEND";
   _ibp_command_map[IBP_LOAD] = "IBP_LOAD";
   _ibp_command_map[IBP_MANAGE] = "IBP_MANAGE";
   _ibp_command_map[IBP_WRITE] = "IBP_WRITE";
   _ibp_command_map[IBP_ALIAS_ALLOCATE] = "IBP_ALIAS_ALLOCATE";
   _ibp_command_map[IBP_ALIAS_MANAGE] = "IBP_ALIAS_MANAGE";
   _ibp_command_map[IBP_RENAME] = "IBP_RENAME";
   _ibp_command_map[IBP_PHOEBUS_SEND] = "IBP_PHOEBUS_SEND";
   _ibp_command_map[IBP_SPLIT_ALLOCATE] = "IBP_SPLIT_ALLOCATE";
   _ibp_command_map[IBP_MERGE_ALLOCATE] = "IBP_MERGE_ALLOCATE";
   _ibp_command_map[IBP_PUSH] = "IBP_PUSH";
   _ibp_command_map[IBP_PULL] = "IBP_PULL";
   _ibp_command_map[IBP_PUSH_CHKSUM] = "IBP_PUSH_CHKSUM";
   _ibp_command_map[IBP_PULL_CHKSUM] = "IBP_PULL_CHKSUM";
   _ibp_command_map[IBP_LOAD_CHKSUM] = "IBP_LOAD_CHKSUM";
   _ibp_command_map[IBP_SEND_CHKSUM] = "IBP_SEND_CHKSUM";
   _ibp_command_map[IBP_PHOEBUS_SEND_CHKSUM] = "IBP_PHOEBUS_SEND_CHKSUM";
   _ibp_command_map[IBP_WRITE_CHKSUM] = "IBP_WRITE_CHKSUM";
   _ibp_command_map[IBP_STORE_CHKSUM] = "IBP_STORE_CHKSUM";
   _ibp_command_map[IBP_ALLOCATE_CHKSUM] = "IBP_ALLOCATE_CHKSUM";
   _ibp_command_map[IBP_SPLIT_ALLOCATE_CHKSUM] = "IBP_SPLIT_ALLOCATE_CHKSUM";
   _ibp_command_map[IBP_GET_CHKSUM] = "IBP_GET_CHKSUM";
   _ibp_command_map[IBP_VALIDATE_CHKSUM] = "IBP_VALIDATE_CHKSUM";
   _ibp_command_map[IBP_VEC_WRITE] = "IBP_VEC_WRITE";
   _ibp_command_map[IBP_VEC_WRITE_CHKSUM] = "IBP_VEC_WRITE_CHKSUM";
   _ibp_command_map[IBP_VEC_READ] = "IBP_VEC_READ";
   _ibp_command_map[IBP_VEC_READ_CHKSUM] = "IBP_VEC_READ_CHKSUM";
}

//*************************************************************************
// print_table - Prints the given table
//*************************************************************************

void print_table(const char **table, int scale, int n)
{
  int i;
  
  for (i=0; i<n; i++) {
     if (table[i] != NULL) printf("%3d %s\n", scale*i, table[i]);
  }
}

//*************************************************************************
// print_ibp_tables - Prints the various IBP constants
//*************************************************************************

void print_ibp_tables()
{
  printf("sizeof(map)=" ST " sizeof(char *)=" ST " table_len=" ST "\n", sizeof(_ibp_command_map), sizeof(char *), table_len(_ibp_command_map));
  printf("\n");
  printf("IBP Commands\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_command_map, 1, table_len(_ibp_command_map));
  printf("\n");

  printf("Options for IBP_MANAGE\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_subcmd_map, 1, table_len(_ibp_subcmd_map));
  printf("\n");

  printf("Options for IBP_STATUS\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_st_map, 1, table_len(_ibp_st_map));
  printf("\n");

  printf("Capability types\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_captype_map, 1, table_len(_ibp_captype_map));
  printf("\n");

  printf("Allocation reliability\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_rel_map, 1, table_len(_ibp_rel_map));
  printf("\n");

  printf("Allocation types\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_type_map, 1, table_len(_ibp_type_map));
  printf("\n");

  printf("Connection types\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_ctype_map, 1, table_len(_ibp_ctype_map));
  printf("\n");

  printf("Error messages\n");  
  printf("------------------------------------------\n");  
  print_table(_ibp_error_map, -1, table_len(_ibp_error_map));
  printf("\n");
}

//*************************************************************************
// scan_map - Scans a table for a match
//*************************************************************************

int scan_map(const char *str, const char **table, int n)
{
  int i;
  
  for (i=0; i<n; i++) {
//printf("scan_map: str=%s table[%d]=%s\n", str, i, table[i]);
     if (table[i] != NULL) {
        if (strcasecmp(table[i], str) == 0) return(i);
     }
  }

  return(-1);
}

//*************************************************************************
//  store_depot - Fills a depot data structure
//*************************************************************************

void store_depot(ibp_depot_t *depot, char **argv, int skip_rid)
{
  int port;
  rid_t rid;

  port = atoi(argv[1]);
  if (skip_rid == 1) {
    ibp_empty_rid(&rid);
  } else {
    rid = ibp_str2rid(argv[2]);
  }
  set_ibp_depot(depot, argv[0], port, rid);
}

//*************************************************************************
//  store_depot - Fills a depot data structure
//*************************************************************************

void store_attr(ibp_attributes_t *attr, char **argv)
{
  int type, rel, duration;

  rel = scan_map(argv[0], _ibp_rel_map, table_len(_ibp_rel_map));
  type = scan_map(argv[1], _ibp_type_map, table_len(_ibp_type_map));
  duration = atoi(argv[2]);
  if (rel < 0) { printf("store_attr: Bad rel type: %s\n", argv[0]); exit(1); }
  if (type < 0) { printf("store_attr: Bad type type: %s\n", argv[1]); exit(1); }

  set_ibp_attributes(attr, time(NULL) + duration, rel, type);
}

//*************************************************************************
//  cmd_validate_chksum - VAlidates the given allocation
//*************************************************************************

int cmd_validate_chksum(char **argv, int argc)
{
  ibp_cap_t *cap;
  ibp_op_t op;
  int correct_errors, n_bad;
  int err, timeout;
  
  if (argc < 3) { printf("cmd_validate_chksum: Not enough parameters.  Received %d need 3\n", argc); return(0); }

  cap = argv[0];
  correct_errors = atoi(argv[1]);
  timeout = atoi(argv[2]);

  init_ibp_op(ic, &op);  
  set_ibp_validate_chksum_op(&op, cap, correct_errors, &n_bad, timeout);
  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_validate_chksum: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  printf("Bad block count: %d\n", n_bad);

  return(0);
}

//*************************************************************************
//  cmd_get_chksum - Retreives the allocation chksum information
//*************************************************************************

int cmd_get_chksum(char **argv, int argc)
{
  ibp_cap_t *cap;
  ibp_op_t op;
  int info_only, cs_type, cs_size;
  ibp_off_t blocksize, nblocks, n_chksumbytes;
  int err, timeout;
  chksum_t cs;

  if (argc < 1) { printf("cmd_validate_chksum: Not enough parameters.  Received %d need 1\n", argc); return(0); }

  cap = argv[0];
  info_only = 1;
  timeout = atoi(argv[1]);

  init_ibp_op(ic, &op);  
  set_ibp_get_chksum_op(&op, cap, info_only, &cs_type, &cs_size, &blocksize, &nblocks, &n_chksumbytes, NULL, 0, timeout);
  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_get_chksum: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  chksum_set(&cs, cs_type);
  printf("Chksum type: %s (%d)  Size: %d  Block Size: " I64T "\n", chksum_name(&cs), cs_type, cs_size, blocksize);
  printf("Total blocks: " I64T " Chksum bytes: " I64T "\n", nblocks, n_chksumbytes);

  return(0);
}


//*************************************************************************
// cmd_allocate - Executes an allocate command
//*************************************************************************

int cmd_allocate(char **argv, int argc)
{
  ibp_depot_t depot;
  ibp_attributes_t attr;
  ibp_capset_t caps;
  ibp_op_t op;
  uint64_t size;
  int err, timeout;

  if (argc < 8) { printf("cmd_allocate: Not enough parameters.  Received %d need 8\n", argc); return(0); }

  store_depot(&depot, argv, 0);
  store_attr(&attr, &(argv[3]));
//  size = atol(argv[6]);
  sscanf(argv[6], LU, &size);
  timeout = atoi(argv[7]);

  init_ibp_op(ic, &op);  
  set_ibp_alloc_op(&op, &caps, size, &depot, &attr, 0, 0, timeout);

  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_allocate: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  printf("Read cap: %s\n", get_ibp_cap(&caps, IBP_READCAP));
  printf("Write cap: %s\n", get_ibp_cap(&caps, IBP_WRITECAP));
  printf("Manage cap: %s\n", get_ibp_cap(&caps, IBP_MANAGECAP));
   
  return(0);
}

//*************************************************************************
// cmd_split_allocate - Executes a split allocate command
//*************************************************************************

int cmd_split_allocate(char **argv, int argc)
{
  ibp_attributes_t attr;
  ibp_capset_t caps;
  ibp_cap_t *mcap;
  ibp_op_t op;
  int size;
  int err, timeout;

  if (argc < 6) { printf("cmd_split_allocate: Not enough parameters.  Received %d need 6\n", argc); return(0); }

  mcap = argv[0];
  store_attr(&attr, &(argv[1]));
  size = atol(argv[4]);
  timeout = atoi(argv[5]);

  init_ibp_op(ic, &op);  
  set_ibp_split_alloc_op(&op, mcap, &caps, size, &attr, 0, 0, timeout);

  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_split_allocate: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  printf("Read cap: %s\n", get_ibp_cap(&caps, IBP_READCAP));
  printf("Write cap: %s\n", get_ibp_cap(&caps, IBP_WRITECAP));
  printf("Manage cap: %s\n", get_ibp_cap(&caps, IBP_MANAGECAP));
   
  return(0);
}

//*************************************************************************
// cmd_merge_allocate - Executes a merge allocate command
//*************************************************************************

int cmd_merge_allocate(char **argv, int argc)
{
  ibp_cap_t *mcap;
  ibp_cap_t *ccap;
  ibp_op_t op;
  int err, timeout;

  if (argc < 3) { printf("cmd_merge_allocate: Not enough parameters.  Received %d need 3\n", argc); return(0); }

  mcap = argv[0];
  ccap = argv[1];
  timeout = atoi(argv[2]);

  init_ibp_op(ic, &op);  
  set_ibp_merge_alloc_op(&op, mcap, ccap, timeout);

  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_merge_allocate: Error %s(%d)\n", _ibp_error_map[-err], err);
  }

  return(0);
}

//*************************************************************************
// cmd_probe - Executes an IBP_MANAGE/PROBE command
//*************************************************************************

int cmd_probe(char **argv, int argc)
{
  ibp_attributes_t attr;
  ibp_capstatus_t probe;
  ibp_cap_t *cap;
  ibp_op_t op;
  int rcnt, wcnt, csize, msize, rel, type;
  char print_time[128];
  apr_time_t dt, duration;
  time_t d;
  int err, timeout;
  
  if (argc < 2) { printf("cmd_probe: Not enough parameters.  Received %d need 2\n", argc); return(0); }

  cap = argv[0];
  timeout = atoi(argv[1]);

  init_ibp_op(ic, &op);  
  set_ibp_probe_op(&op, cap, &probe, timeout);
  err = ibp_sync_command(&op);

  if (err != IBP_OK) {
     printf("cmd_probe: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  get_ibp_capstatus(&probe, &rcnt, &wcnt, &csize, &msize, &attr); 
  get_ibp_attributes(&attr, &d, &rel, &type); 
  duration = d;
  ctime_r(&d, print_time); print_time[strlen(print_time)-1] = '\0';

  printf("Probing Capability: %s\n", cap);
  printf("Read ref count: %d    Write ref count: %d\n", rcnt, wcnt);
  printf("Current Size: %d      Max Size: %d\n", csize, msize);
  printf("Reliability: %s(%d)\n", _ibp_rel_map[rel], rel);
  printf("Type: %s(%d)\n", _ibp_type_map[type], type);
  if (duration > time(NULL)) {
     dt = duration - time(NULL);  //** This is in sec NOT APR time
     printf("Expiration: %s (" TT ") " TT " sec left\n",print_time, duration, dt);
  } else {
     dt = time(NULL) - duration;  //** This is in sec NOT APR time
     printf("Expiration: %s (" TT ") EXPIRED " TT " secs ago\n",print_time, duration, dt);
  }

  return(0);
}

//*************************************************************************
// cmd_modify_count - Executes an IBP_MANAGE/(IBP_INC|IBP_DEC) command
//*************************************************************************

int cmd_modify_count(int mode, char **argv, int argc)
{
  ibp_cap_t *cap;
  ibp_op_t op;
  int captype;
  int err, timeout;
  
  if (argc < 3) { printf("cmd_modify_count: Not enough parameters.  Received %d need 3\n", argc); return(0); }

  
  cap = argv[0];
  captype = scan_map(argv[1], _ibp_captype_map, table_len(_ibp_captype_map));
  timeout = atoi(argv[2]);

  if (captype < 0) { printf("cmd_modify_count: Bad captype: %s\n", argv[1]); exit(1); }

  init_ibp_op(ic, &op);  
  set_ibp_modify_count_op(&op, cap, mode, captype, timeout);
  err = ibp_sync_command(&op);
  if (err != IBP_OK) {
     printf("cmd_modify_count: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  return(0);
}

//*************************************************************************
// cmd_modify_alloc - Executes an IBP_MANAGE/IBP_CHNG command
//*************************************************************************

int cmd_modify_alloc(char **argv, int argc)
{
  ibp_cap_t *cap;
  ibp_op_t op;
  int rel;
  size_t size;
  time_t duration;
  int err, timeout;
  
  if (argc < 5) { printf("cmd_modify_alloc: Not enough parameters.  Received %d need 5\n", argc); return(0); }
  
  cap = argv[0];
  size = atol(argv[1]);
  duration = atol(argv[2]);
  rel = scan_map(argv[3], _ibp_rel_map, table_len(_ibp_rel_map));
  timeout = atoi(argv[4]);

  if (rel < 0) { printf("cmd_modify_alloc: Bad reliability: %s\n", argv[3]); exit(1); }
  
  timeout = atoi(argv[4]);

  init_ibp_op(ic, &op);  
  set_ibp_modify_alloc_op(&op, cap, size, duration, rel, timeout);
  err = ibp_sync_command(&op);
  if (err != IBP_OK) {
     printf("cmd_modify_alloc: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  return(0);
}

//*************************************************************************
// cmd_version - Executes an IBP_STATUS/IBP_ST_VERSION command
//*************************************************************************

int cmd_version(char **argv, int argc)
{
  ibp_depot_t depot;
  ibp_op_t op;
  char buffer[4096];
  int err, timeout;
  
  if (argc < 2) { printf("cmd_version: Not enough parameters.  Received %d need 3\n", argc); return(0); }

  store_depot(&depot, argv, 1);
  timeout = atoi(argv[2]);
 
  init_ibp_op(ic, &op);  
  set_ibp_version_op(&op, &depot, buffer, sizeof(buffer), timeout);

  err = ibp_sync_command(&op);
  if (err != IBP_OK) {
     printf("cmd_probe: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  printf("%s\n", buffer);

  return(0);
}

//*************************************************************************
// cmd_ridlist - Executes an IBP_STATUS/IBP_ST_RES command
//*************************************************************************

int cmd_ridlist(char **argv, int argc)
{
  ibp_depot_t depot;
  ibp_ridlist_t ridlist;
  char srid[1024];
  int i;
  ibp_op_t op;
  int err, timeout;
  
  if (argc < 2) { printf("cmd_ridlist: Not enough parameters.  Received %d need 3\n", argc); return(0); }

  store_depot(&depot, argv, 1);
  timeout = atoi(argv[2]);
 
  init_ibp_op(ic, &op);  
  set_ibp_query_resources_op(&op, &depot, &ridlist, timeout);

  err = ibp_sync_command(&op);
  if (err != IBP_OK) {
     printf("cmd_ridlist: Error %s(%d)\n", _ibp_error_map[-err], err);
     return(0);
  }

  printf("Resource list for %s:%s  (%d resources)\n", argv[0], argv[1], ridlist_get_size(&ridlist));
  for (i=0; i<ridlist_get_size(&ridlist); i++) {
     ibp_rid2str(ridlist_get_element(&ridlist, i), srid);
     printf("%s\n", srid);
  }  

  return(0);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, tcpsize;
  phoebus_t pcc;
  char *ppath = NULL;
  char **cmd_args;
  int  cmd_count;
  ibp_connect_context_t *cc = NULL;

  if (argc < 2) {
     printf("\n");
     printf("IBP Client Library information\n");
     printf("--------------------------------------------------\n");
     printf("%s", ibp_client_version());
     printf("--------------------------------------------------\n");
     printf("\n");
     printf("ibp_tool -t\n");
     printf("ibp_tool [-d debug_level] [-config ibp.cfg] [-phoebus ppath] [-tcpsize] -c ibp_command\n");
     printf("\n");
     printf("-t                  - Print out the various IBP constants table\n");
     printf("-d debug_level      - Enable debug output.  debug_level=0..20\n");
     printf("-config ibp.cfg     - Use the IBP configuration defined in file ibp.cfg.\n");
     printf("-phoebus            - Use Phoebus protocol for data transfers.\n");
     printf("   gateway_list     - Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321\n");
     printf("-tcpsize tcpbufsize - Use this value, in KB, for the TCP send/recv buffer sizes\n");
     printf("-c ibp_command      - Execute an ibp_command defined below\n");
     printf("   ibp_allocate host port rid reliability type duration(sec) size(bytes) timeout(sec)\n");
     printf("   ibp_split_allocate master_cap reliability type duration(sec) size(bytes) timeout(sec)\n");
     printf("   ibp_merge_allocate master_cap child_cap timeout(sec)\n");
     printf("   ibp_manage IBP_PROBE manage_cap timeout(sec)\n");
     printf("              IBP_INCR|IBP_DECR manage_cap cap_type timeout(sec)\n");
     printf("              IBP_CHNG manage_cap size(bytes) duration(sec) reliability timeout(sec)\n");
     printf("   ibp_status IBP_ST_VERSION host port timeout(sec)\n");
     printf("              IBP_ST_RES host port timeout(sec)\n");
     printf("   ibp_validate_chksum master_cap regenerate_bad_chksums timeout(sec)\n");
     printf("   ibp_get_chksum master_cap timeout(sec)\n");
     printf("\n");
     return(-1);
  }

  init_tables();

  i = 1;
  if (strcmp(argv[i], "-t") == 0) {
     print_ibp_tables();
     return(0);
  }

  ic = ibp_create_context();  //** Initialize IBP

  if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
     i++;
     set_log_level(atoi(argv[i]));
     i++;
  }

  if (strcmp(argv[i], "-config") == 0) { //** Read the config file
     i++;
     ibp_load_config_file(ic, argv[i], NULL);
     i++;
  }

  if (strcmp(argv[i], "-phoebus") == 0) { //** Check if we want Phoebus transfers
     cc = (ibp_connect_context_t *)malloc(sizeof(ibp_connect_context_t));
     cc->type = NS_TYPE_PHOEBUS;
     i++;

     ppath = argv[i];
     phoebus_path_set(&pcc, ppath);
     cc->data = &pcc;

     ibp_set_read_cc(ic, cc);
     ibp_set_write_cc(ic, cc);

     i++;
  }

  if (strcmp(argv[i], "-tcpsize") == 0) { //** Check if we want sync tests
     i++;
     tcpsize = atoi(argv[i]) * 1024;
     ibp_set_tcpsize(ic, tcpsize);
     i++;
  }


  if (strcmp(argv[i], "-c") == 0) {
     i++;
     cmd_args = &(argv[i+1]);
     cmd_count = argc - (i+1);
     if (strcasecmp(argv[i], _ibp_command_map[IBP_ALLOCATE]) == 0) {
        cmd_allocate(cmd_args, cmd_count);
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_SPLIT_ALLOCATE]) == 0) {
        cmd_split_allocate(cmd_args, cmd_count);
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_MERGE_ALLOCATE]) == 0) {
        cmd_merge_allocate(cmd_args, cmd_count);
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_MANAGE]) == 0) {
        i++;
        cmd_args = &(argv[i+1]);
        cmd_count = argc - (i+1);
        if (strcasecmp(argv[i], _ibp_subcmd_map[IBP_PROBE]) == 0) {
           cmd_probe(cmd_args, cmd_count);
        } else if (strcasecmp(argv[i], _ibp_subcmd_map[IBP_INCR]) == 0) {
           cmd_modify_count(IBP_INCR, cmd_args, cmd_count);
        } else if (strcasecmp(argv[i], _ibp_subcmd_map[IBP_DECR]) == 0) {
           cmd_modify_count(IBP_DECR, cmd_args, cmd_count);
        } else if (strcasecmp(argv[i], _ibp_subcmd_map[IBP_CHNG]) == 0) {
           cmd_modify_alloc(cmd_args, cmd_count);
        } else {
          printf("unknown ibp_manage sub-command: %s\n", argv[i]);
        }        
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_STATUS]) == 0) {
        i++;
        cmd_args = &(argv[i+1]);
        cmd_count = argc - (i+1);
        if (strcasecmp(argv[i], _ibp_st_map[IBP_ST_VERSION]) == 0) {
           cmd_version(cmd_args, cmd_count);
        } else if (strcasecmp(argv[i], _ibp_st_map[IBP_ST_RES]) == 0) {
           cmd_ridlist(cmd_args, cmd_count);        
        } else {
          printf("unknown ibp_status sub-command: %s\n", argv[i]);
        }        
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_GET_CHKSUM]) == 0) {
        cmd_get_chksum(cmd_args, cmd_count);        
     } else if (strcasecmp(argv[i], _ibp_command_map[IBP_VALIDATE_CHKSUM]) == 0) {
        cmd_validate_chksum(cmd_args, cmd_count);        
     } else {
        printf("unknown command: %s\n", argv[i]);
     }
  }

//  printf("Final network connection counter: %d\n", network_counter(NULL));

  ibp_destroy_context(ic);  //** Shutdown IBP

  return(0);
}


