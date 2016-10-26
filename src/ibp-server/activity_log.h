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

#include <ibp-server/visibility.h>
#include <stdint.h>
#include <stdio.h>
#include <apr_time.h>
#include "osd_abstract.h"

#define ALOG_VERSION_1  1
#define ALOG_VERSION  ALOG_VERSION_1

//**** modes for opening an alog file
#define ALOG_READ   0
#define ALOG_APPEND 1

//*** Record types ****
#define ALOG_REC_VALIDATE_GET_CHKSUM   207    //** IBP_VALIDATE_CHKSUM and IBP_GET_CHKSUM
#define ALOG_REC_INT_GET_CONFIG        208    //** Get config command
#define ALOG_REC_IBP_MERGE             209    //** Merge allocate command
#define ALOG_REC_IBP_SPLIT_ALLOCATE64  210    //** 64 bit split allocate
#define ALOG_REC_IBP_SPLIT_ALLOCATE32  211    //** 32 bit split allocate
#define ALOG_REC_INT_EXPIRE_LIST      212    //** Internal_expire_list
#define ALOG_REC_INT_DATE_FREE        213    //** Internal_date_free
#define ALOG_REC_ALIAS_COPY64         214    //** IBP_SEND/PUSH/PULL
#define ALOG_REC_ALIAS_COPY32         215    //**    ""
#define ALOG_REC_COPY64               216    //**    ""
#define ALOG_REC_COPY32               217    //**    ""
#define ALOG_REC_ALIAS_READ64         218    //** IBP_READ
#define ALOG_REC_ALIAS_READ32         219    //** IBP_READ
#define ALOG_REC_READ64               220    //** IBP_READ
#define ALOG_REC_READ32               221    //** IBP_READ
#define ALOG_REC_ALIAS_WRITE_APPEND64 222    //** IBP_STORE
#define ALOG_REC_ALIAS_WRITE_APPEND32 223    //** IBP_STORE
#define ALOG_REC_ALIAS_WRITE64        224    //** IBP_WRITE
#define ALOG_REC_ALIAS_WRITE32        225    //** IBP_WRITE
#define ALOG_REC_WRITE_APPEND64       226    //** IBP_STORE
#define ALOG_REC_WRITE_APPEND32       227    //** IBP_STORE
#define ALOG_REC_WRITE64              228    //** IBP_WRITE
#define ALOG_REC_WRITE32              229    //** IBP_WRITE
#define ALOG_REC_ALIAS_MANAGE_PROBE   230    //** IBP_ALIAS_MANAGE/IBP_PROBE
#define ALOG_REC_MANAGE_PROBE         231    //** IBP_MANAGE/IBP_PROBE
#define ALOG_REC_MANAGE_CHANGE        232    //** IBP_MANAGE/IBP_CHNG
#define ALOG_REC_ALIAS_MANAGE_CHANGE  233    //** IBP_ALIAS_MANAGE/IBP_CHNG
#define ALOG_REC_MANAGE_INCDEC        234    //** IBP_MANAGE IBP_INCR/IBP_DECR
#define ALOG_REC_ALIAS_MANAGE_INCDEC  235    //** IBP_ALIAS_MANAGE IBP_INCR/IBP_DECR
#define ALOG_REC_MANAGE_BAD          236    //** Handles generic IBP_ALIAS_MANAGE/IBP_MANAGE errors
#define ALOG_REC_STATUS_CHANGE       237    //** IBP_STATUS/IBP_ST_CHANGE
#define ALOG_REC_STATUS_INQ          238    //** IBP_STATUS/IBP_ST_INQ
#define ALOG_REC_STATUS_STATS        239    //** IBP_STATUS/IBP_ST_STATS
#define ALOG_REC_STATUS_RES          240    //** IBP_STATUS/IBP_ST_RES
#define ALOG_REC_STATUS_VERSION      241    //** IBP_STATUS/IBP_ST_VERSION
#define ALOG_REC_ALIAS_ALLOC32       242    //** ALIAS allocation
#define ALOG_REC_ALIAS_ALLOC64       243    //** ALIAS allocation
#define ALOG_REC_INTERNAL_GET_ALLOC  244    //** Internal get_alloc command
#define ALOG_REC_IBP_RENAME      245    //** Rename command
#define ALOG_REC_OSD_ID          246    //** Prints an osd_id
#define ALOG_REC_IBP_ALLOCATE64  247    //** 64 bit allocate
#define ALOG_REC_IBP_ALLOCATE32  248    //** 32 bit allocate
#define ALOG_REC_CMD_RESULT      249    //** generic command result
#define ALOG_REC_RESOURCE_LIST   250    //** List of resources
#define ALOG_REC_THREAD_CLOSE    251    //** Close existing thread
#define ALOG_REC_THREAD_OPEN     252    //** Create new thread
#define ALOG_REC_IBP_CONFIG      253    //** IBP config information
#define ALOG_REC_CLOSE           254    //** Written automatically upon close
#define ALOG_REC_OPEN            255    //** Written automatically upon open

typedef struct {  //*** Main file header
  uint64_t version;
  uint64_t start_time;
  uint64_t end_time;
  uint64_t state;
} __attribute__((__packed__)) alog_file_header_t;

typedef struct {  //** Activity log record header - 1 byte ID
   uint64_t time;             //** Command time stamp
   uint8_t  command;          //** Primary command
   uint8_t thread_id;         //** Thread ID.  Used to link with NS id
} __attribute__((__packed__)) alog_header1_t;

typedef struct {  //** Activity log record header - 2 byte ID
   uint64_t time;             //** Command time stamp
   uint8_t command;           //** Primary command
   uint16_t thread_id;        //** Thread ID.  Used to link with NS id
} __attribute__((__packed__)) alog_header2_t;


typedef struct {
   char name[256];
} rl_map_t;

typedef struct {
   int id;
   int family;
   int used;
   char address[16];
} ns_map_t;

struct alog_entry_s;
typedef struct alog_entry_s alog_entry_t;
typedef struct activity_log_s {
  alog_file_header_t header;
  alog_entry_t *table;
  char *name;
  size_t max_size;
  size_t curr_size;
  int max_id;
  int mode;
  ns_map_t *ns_map;
  rl_map_t *rl_map;
  int nres;
  int (*append_header)(FILE *fd, int id, int command);
  int (*read_header)(FILE *fd, apr_time_t *time, int *id, int *command);
  void(*print_header)(FILE *fd, apr_time_t t, int id);
  FILE *fd;
} activity_log_t;

struct alog_entry_s {   //** Activity log vector table for command decoding
   int (*process_entry)(activity_log_t *alog, int cmd, FILE *outfd);
};


int alog_append_validate_get_chksum(int tid, int command, int ri, osd_id_t pid, osd_id_t id);
int alog_append_ibp_merge(int tid, osd_id_t mid, osd_id_t cid, int rindex);
int alog_append_internal_get_config(int tid);
int alog_append_internal_expire_list(int tid, int ri, apr_time_t start_time, int max_rec);
int alog_append_internal_date_free(int tid, int ri, uint64_t size);
int alog_append_dd_copy(int cmd, int tid, int ri, osd_id_t pid, osd_id_t id,
        uint64_t size, uint64_t offset, uint64_t offset2, int write_mode, int ctype, char *path, int port, 
        int family, const char *address, const char *key, const char *typekey);
int alog_append_read(int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size);
int alog_append_write(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size);
int alog_append_alias_manage_probe(int tid, int ri, osd_id_t pid, osd_id_t id);
int alog_append_manage_probe(int tid, int ri, osd_id_t id);
int alog_append_manage_change(int tid, int ri, osd_id_t id, uint64_t size, int rel, apr_time_t t);
int alog_append_alias_manage_change(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t t);
int alog_append_manage_incdec(int tid, int cmd, int subcmd, int ri, osd_id_t pid, osd_id_t id, int cap_type);
int alog_append_manage_bad(int tid, int command, int subcmd);
int alog_append_subcmd(int tid, int command, int subcmd);
int alog_append_status_inq(int tid, int ri);
int alog_append_status_stats(int tid, apr_time_t start_time);
int alog_append_status_change(int tid);
int alog_append_status_version(int tid);
int alog_append_status_res(int tid);
int alog_append_osd_id(int tid, osd_id_t id);
int alog_append_alias_alloc(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t expire);
int alog_append_internal_get_alloc(int tid, int rindex, osd_id_t id);
int alog_append_ibp_rename(int tid, int rindex, osd_id_t id);
int alog_append_ibp_allocate(int tid, int rindex, uint64_t max_size, int atype, int rel, apr_time_t expiration);
int alog_append_ibp_split_allocate(int tid, int rindex, osd_id_t id, uint64_t max_size, int atype, int rel, apr_time_t expiration);
int alog_append_cmd_result(int tid, int status);
int alog_append_thread_open(int tid, int ns_id, int family, char *address);
int alog_append_thread_close(int tid, int ncmds);
int activity_log_open_rec(activity_log_t *alog);
void activity_log_close_rec(activity_log_t *alog);

int alog_read_ibp_config_rec(activity_log_t *alog, int cmd, FILE *outfd);
int activity_log_read_open_rec(activity_log_t *alog, int cmd, FILE *outfd);
int activity_log_read_close_rec(activity_log_t *alog, int cmd, FILE *outfd);

IBPS_API int activity_log_read_next_entry(activity_log_t *alog, FILE *fd);

int activity_log_read_header_1byte_id(FILE *fd, apr_time_t *t, int *id, int *command);
int activity_log_read_header_2byte_id(FILE *fd, apr_time_t *t, int *id, int *command);

int activity_log_append_header_1byte_id(FILE *fd, int id, int command);
int activity_log_append_header_2byte_id(FILE *fd, int id, int command);

IBPS_API void _alog_init_constants();
int _alog_config();
IBPS_API void alog_open();
IBPS_API void alog_close();
void print_header_date_utime_id(FILE *fd, apr_time_t t, int id);
void print_header_date_id(FILE *fd, apr_time_t t, int id);
void print_header_utime_id(FILE *fd, apr_time_t t, int id);
void activity_log_move_to_eof(activity_log_t *alog);
IBPS_API alog_file_header_t get_alog_header(activity_log_t *alog);
int write_file_header(activity_log_t *alog);
int read_file_header(activity_log_t *alog);
int update_file_header(activity_log_t *alog, int state);
IBPS_API activity_log_t * activity_log_open(const char *logname, int max_id, int mode);
IBPS_API void activity_log_close(activity_log_t *alog);








 

   
