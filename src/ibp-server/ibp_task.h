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

//************************************************************************
//************************************************************************


#ifndef __IBP_TASK_H_
#define __IBP_TASK_H_

#include <apr_thread_mutex.h>
#include <tbx/network.h>
#include "allocation.h"
#include "resource.h"
#include "transfer_stats.h"

#define COMMAND_TABLE_MAX 100   //** Size of static command table

#define CMD_STATE_NONE      0   //** No command currently being processed
#define CMD_STATE_CMD       1   //** Still getting input for the command itself
#define CMD_STATE_WRITE     2   //** Sending data to client
#define CMD_STATE_READ      3   //** Reading data from the client
#define CMD_STATE_READWRITE 4   //** Performing both read and write operations simultaneously
#define CMD_STATE_RESULT    5   //** Sending the result to the client
#define CMD_STATE_WAITING   6   //** Command is in a holding pattern. Used for FIFO/BUFFER/CIRQ transfers
#define CMD_STATE_FINISHED  7   //** Command completed
#define CMD_STATE_NEW       8   //** New command
#define CMD_STATE_PARENT    9   //** This command has a 2ndary command which handles the parent connection
#define CMD_STATE_CLOSED   10   //** Command's network connection is already closed

#define PARENT_RETURN      1000 //** Used in worker_task to signify the return value is fro ma parent task so ignore ns

#define IOVEC_MAX 4096

typedef struct {
  ibp_off_t off;
  ibp_off_t len;
  ibp_off_t cumulative_len;
} iovec_ele_t;

typedef struct {
  int n;
//  int curr_index;
  ibp_off_t total_len;
  ibp_off_t transfer_total;
//  ibp_off_t curr_offset;
//  ibp_off_t curr_nleft;  
  iovec_ele_t vec[IOVEC_MAX];
}  ibp_iovec_t;

typedef struct {  // date_free args
  rid_t rid;
  uint64_t size;
  char    crid[128];        //** Character version of the RID for querying
} Cmd_internal_date_free_t;

typedef struct {  // expire_log args
  rid_t rid;
  apr_time_t start_time;
  int direction;
  int mode;
  int max_rec;
  char    crid[128];        //** Character version of the RID for querying
} Cmd_internal_expire_log_t;


typedef struct {          //**Allocate args
   rid_t     rid;            //RID to use (0=don't care)
   Cap_t     master_cap;     //Master cap for IBP_SPLIT_ALLOCATE
   char      crid[128];      //** Character version of the RID for querying
   int       cs_type;        //** disk Chksum type 
   ibp_off_t cs_blocksize;   //** disk Chksum blocksize
   Allocation_t a;           //Allocation being created
} Cmd_allocate_t;

typedef struct {          //**merge allocation args
   rid_t     rid;            //RID for both allocations
   Cap_t     mkey;           //Master cap
   Cap_t     ckey;           //Child cap
   char      crid[128];      //** Character version of the RID for querying
} Cmd_merge_t;

#define PASSLEN  32
typedef struct {          //** Status Args
  rid_t rid;               //** RID for querying
  char    crid[128];        //** Character version of the RID for querying
  ibp_off_t new_size[2];     //** New sizes
  int   start_time;         //** Start time used by status call
  int   subcmd;            //** Subcommand
  long int new_duration;      //** New max duration for allocation
  char password[PASSLEN];  //** Password  
} Cmd_status_t;

typedef struct {
  rid_t   rid;             //** RID for querying
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Cap_t   cap;             //** Manage cap of original allocation
  ibp_off_t   offset;          //** Offset into original allocation 
  ibp_off_t   len;             //** Length in original alloc if offset=len=0 then full range is given  
  uint32_t expiration;       //** Duration of alias allocation
} Cmd_alias_alloc_t;

typedef struct {
  rid_t rid;               //** RID for querying
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];        //** Character version of the ID for querying
  Cap_t   cap;             //** Key
  Cap_t   master_cap;      //** Master manage key for ALIAS_MANAGE
  ibp_off_t new_size;        //** New size
  ibp_off_t offset;
  int   subcmd;            //** Subcommand
  int   captype;           //** Capability type
  int  new_reliability;    //** New reliability
  long int new_duration;   //** New max duration for allocation
  Allocation_t a;          //** Allocation for command
} Cmd_manage_t;

typedef struct {
  int      sending;        //** Write state
  rid_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];        //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Resource_t *r;           //** Resource being used
  Cap_t   cap;             //** Key
//  ibp_off_t   offset;          //** Offset into allocation to start writing
//  ibp_off_t   len;             //** Length of write
//  ibp_off_t   pos;             //** Current buf pos
//  ibp_off_t   left;            //** Bytes left to copy
  Allocation_t a;          //** Allocation for command
  ibp_iovec_t      iovec;      //** USed only for iovec operations > 1
} Cmd_write_t;

typedef struct {
  int      recving;        //** read state
  int      retry;          //** Used only for IBP_copy commands
  int      transfer_dir;   //** either IBP_PULL or IBP_PUSH
  int      ctype;          //** Connection type
  int      write_mode;     //** Write mode, 0=use dest offset, 1=append data
  rid_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];       //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  char    path[4096];      //** command path for phoebus transfers
  Resource_t *r;           //** Resource being used
  Cap_t   cap;             //** Key
  ibp_off_t   remote_offset;   //** Offset into allocation to start writing
//  ibp_off_t   offset;          //** Offset into allocation to start writing
//  ibp_off_t   len;             //** Length of write
//  ibp_off_t   pos;             //** Current buf pos
//  ibp_off_t   left;            //** Bytes left to copy
  int     valid_conn;      //** Determines if I need to make a new depot connection
  apr_time_t remote_sto;       //** REmote commands server timeout
  apr_time_t remote_cto;       //** Remote commands client timeout
  char   remote_cap[1024]; //** Remote Read/Write cap for IBP_*SEND/IBP_PULL/IBP_PUSH commands
  Allocation_t a;          //** Allocation for command
  ibp_iovec_t      iovec;      //** USed only for iovec operations > 1
} Cmd_read_t;

typedef struct {
  rid_t rid;               //** RID for querying
  osd_id_t  id;            //** Object id
  char    crid[128];        //** Character version of the RID for querying
  char    cid[64];         //** Character version of the ID for querying
  Resource_t *r;           //** Resource being used 
  int     key_type;
  Cap_t   cap; 
  int     print_blocks;
  int64_t   offset;          //** Offset into allocation to start reading
  uint64_t  len;             //** Length of read
} Cmd_internal_get_alloc_t;

typedef struct {
  rid_t rid;               //** RID for querying
  long int duration;   //** New max duration for allocation
  int   trash_type;
  char    crid[128];        //** Character version of the RID for querying
  char    trash_id[1024];  //** Trash file id
} Cmd_internal_undelete_t;

typedef struct {
  rid_t rid;                //** RID for rescanning or 0 if all
  char    crid[128];        //** Character version of the RID for querying
} Cmd_internal_rescan_t;

typedef struct {
  rid_t rid;                //** RID for rescanning or 0 if all
  char    crid[128];        //** Character version of the RID for querying
  int   force_rebuild;      //** Only used for mount command
  int   delay;              //** Only used for umount command
  char msg[1024];           //** Message to add to the log file
} Cmd_internal_mount_t;

typedef struct {
  rid_t rid;                //** RID for setting/getting the mode
  char    crid[128];        //** Character version of the RID for querying
  int   mode;               //** New RWM mode
} Cmd_internal_mode_t;


typedef union {            //** Union of command args
    Cmd_allocate_t allocate;
    Cmd_status_t   status;
    Cmd_manage_t   manage;
    Cmd_merge_t    merge;
    Cmd_write_t    write;
    Cmd_read_t     read;
    Cmd_alias_alloc_t  alias_alloc;
    Cmd_internal_get_alloc_t get_alloc;
    Cmd_internal_date_free_t date_free;
    Cmd_internal_expire_log_t expire_log;
    Cmd_internal_undelete_t   undelete;
    Cmd_internal_rescan_t   rescan;
    Cmd_internal_mount_t   mount;
    Cmd_internal_mode_t   mode;
} Cmd_args_t;

typedef struct {           //** Stores the state of the command
  int   state;             //** Internal command state or phase
  int   command;           //** Command being processed
  int   version;           //** Command version
  Cmd_args_t cargs;         //** Command args
} Cmd_state_t;

typedef struct ibp_task_struct {            //** Data structure sent to the job_pool thread
  tbx_ns_t *ns;          //** Stream to use
  tbx_network_t   *net;         //** Network connection is managed by
  tbx_ns_chksum_t ncs;          //** NS Chksum to use if needed
  int         enable_chksum;
  Allocation_address_t  ipadd; //** Used for updating allocations
  Cmd_state_t cmd;
  int command_acl[COMMAND_TABLE_MAX+1];  //** ACL's for commands based on ns
  uint64_t tid;            //** Unique task id used for debugging purposes only
  int      myid;           //** Thread id
  tbx_ns_timeout_t  timeout;      //** Max wait time for select() calls
  apr_time_t      cmd_timeout;  //** Timeout for the command in seconds since the epoch
  apr_thread_mutex_t *lock;     //** Task lock
  struct ibp_task_struct *parent;
  struct ibp_task_struct *child;
  int         dpinuse;      //** Used in IBP_copy commands
  int         submitted;      //** USed to control access to the task
  Transfer_stat_t  stat;     //** Command stats
} ibp_task_t;

#endif

