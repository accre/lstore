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

//*** Make sure I get the limits macros
#define __STDC_LIMIT_MACROS

#include <glob.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <sys/socket.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <apr_pools.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/net_sock.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include "activity_log.h"
#include <ibp-server/ibp_server.h>
#include "subnet.h"
#include "envelope.h"
#include "envelope_net.h"
#include "resource_list.h"

#define STATE_GOOD 1
#define STATE_BAD  0

#define alog_lock() apr_thread_mutex_lock(_alog_lock)
#define alog_unlock() apr_thread_mutex_unlock(_alog_lock)

#define alog_mode_check() if (_alog_max_size <= 0) return(0);

//** Macros for reading/writing the log file
#define aread(fd, buffer, nbytes, ...) \
   if ((int)fread(buffer, 1, nbytes, fd) != nbytes) { \
      log_printf(0, __VA_ARGS__); \
      return(1); \
   }

#define awrite(fd, buffer, nbytes, ...) \
   if ((int)fwrite(buffer, 1, nbytes, fd) != nbytes) { \
      log_printf(0, __VA_ARGS__); \
      return(1); \
   }

#define awrite_ul(fd, buffer, nbytes, ...) \
   if ((int)fwrite(buffer, 1, nbytes, fd) != nbytes) { \
      log_printf(0, __VA_ARGS__); \
      apr_thread_mutex_unlock(_alog_lock); \
      return(1); \
   }

//** Macro for checking alog size and forcing data to be sent
#define alog_checksize() \
   if ((int)ftell(_alog->fd) > _alog_max_size) { \
      _alog_send_data(); \
   }

void _alog_send_data();  

//***** Global variables used by singleton *******
apr_thread_mutex_t  *_alog_lock = NULL;
apr_thread_mutex_t *_alog_send_lock = NULL;
apr_thread_t       *_alog_send_thread = NULL;
apr_pool_t         *_alog_send_mpool = NULL;
apr_pool_t         *_alog_mpool = NULL;
activity_log_t *_alog = NULL;
const char *_alog_name;
int64_t _alog_max_size;
size_t _alog_size;
int    _alog_count = 0;
tbx_stack_t *_alog_pending_stack = NULL;

const env_command_t ECMD_ALOG_SEND = {{{0,0,2,0}}}; 

const char *_ibp_error_map[61];
const char *_ibp_subcmd_map[45];
const char *_ibp_st_map[6];
const char *_ibp_rel_map[3];
const char *_ibp_type_map[5];
const char *_ibp_ctype_map[3];
const char *_ibp_captype_map[3];
const char *_ibp_cmd_map[IBP_MAX_NUM_CMDS+1];
const char *_ibp_wmode_map[2];

//***********************************************************************************
// alog_init - Init routines for use.
//***********************************************************************************

void alog_init()
{
  if (_alog_mpool != NULL) return;

  apr_pool_create(&_alog_mpool, NULL);
  apr_thread_mutex_create(&_alog_lock, APR_THREAD_MUTEX_DEFAULT,_alog_mpool);
  apr_thread_mutex_create(&_alog_send_lock, APR_THREAD_MUTEX_DEFAULT,_alog_mpool);
}

//***********************************************************************************
// alog_destroy - Tear down memory pool.
//***********************************************************************************

void alog_destroy()
{
  apr_pool_destroy(_alog_mpool);
}


//***********************************************************************************
//------- Routines below are the "singleton" version for use by ibp_server ----------
//***********************************************************************************

//************************************************************************
//  INTERNAL_GET_CONFIG
//************************************************************************

//-----------------------------------------------------------

int alog_append_internal_get_config(int tid)
{
   alog_mode_check();

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_INT_GET_CONFIG);
   
   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_internal_get_config(activity_log_t *alog, int cmd, FILE *outfd)
{
   if (outfd != NULL) {
      fprintf(outfd, "CMD:INTERNAL_GET_CONFIG\n");
   }

   return(0);
}

//************************************************************************
//  INTERNAL_EXPIRE_LIST
//************************************************************************

typedef struct {
   uint32_t max_rec;
   uint64_t time;   
   uint8_t ri;
} __attribute__((__packed__)) _alog_internal_expire_list_t;

//-----------------------------------------------------------

int alog_append_internal_expire_list(int tid, int ri, apr_time_t start_time, int max_rec)
{
   alog_mode_check();

   _alog_internal_expire_list_t a;

   a.max_rec = max_rec;
   a.time = start_time;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_INT_EXPIRE_LIST);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_int_expire_list: Error with write!\n");
   
   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_internal_expire_list(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_internal_expire_list_t a;
   apr_time_t t;
   int n;
   char buf[128];

   aread(alog->fd, &a, sizeof(a), "alog_read_int_expire_list: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      t = ibp2apr_time(a.time); n = a.max_rec;
      apr_ctime(buf, ibp2apr_time(a.time));
      buf[strlen(buf)-1] = '\0';  //** chomp the \n from ctime_r

      fprintf(outfd, "CMD:INTERNAL_EXPIRE_LIST  RID:%s max_rec: %d  start_time: %s(" TT ")\n",
         alog->rl_map[a.ri].name, n, buf, t);
   }

   return(0);
}

//************************************************************************
//  INTERNAL_DATE_FREE
//************************************************************************

typedef struct {
   uint64_t size;
   uint8_t ri;
} __attribute__((__packed__)) _alog_internal_date_free_t;

//-----------------------------------------------------------

int alog_append_internal_date_free(int tid, int ri, uint64_t size)
{
   _alog_internal_date_free_t a;

   alog_mode_check();

   a.size = size;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_INT_DATE_FREE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_int_date_free: Error with write!\n");
   
   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_internal_date_free(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_internal_date_free_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_int_date_free: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:INTERNAL_DATE_FREE  RID:%s size:" LU "\n",
         alog->rl_map[a.ri].name, a.size);
   }

   return(0);
}

//************************************************************************
//  VALIDATE_GET_CHKSUM
//************************************************************************

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint8_t ri;
   uint8_t cmd;
} __attribute__((__packed__)) _alog_validate_get_chksum_t;

//-----------------------------------------------------------

int alog_append_validate_get_chksum(int tid, int command, int ri, osd_id_t pid, osd_id_t id)
{
   _alog_validate_get_chksum_t a;

   alog_mode_check();

   a.pid = pid;
   a.id = id;
   a.ri = ri;
   a.cmd = command;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_VALIDATE_GET_CHKSUM);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_validate_get_chksum: Error with write!\n");
   
   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_validate_get_chksum(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_validate_get_chksum_t a;

   aread(alog->fd, &a, sizeof(a), "alog_validate_get_chksum: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      if (a.cmd == IBP_VALIDATE_CHKSUM) {
         fprintf(outfd, "CMD:IBP_VALIDATE_CHKSUM  RID:%s PID:" LU " ID:" LU "\n",
            alog->rl_map[a.ri].name, a.pid, a.id);
      } else if (a.cmd == IBP_GET_CHKSUM) {
         fprintf(outfd, "CMD:IBP_GET_CHKSUM  RID:%s PID:" LU " ID:" LU "\n",
            alog->rl_map[a.ri].name, a.pid, a.id);
      }
   }

   return(0);
}


//************************************************************************
//  IBP_SEND
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_copy_append64_t;

typedef struct {
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_copy_append32_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_copy_append64_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_copy_append32_t;

//------------------------------------------------------------------------

typedef struct {
   uint16_t port;
   uint8_t family;
   uint8_t key_size;
   uint8_t typekey_size;
} __attribute__((__packed__)) _alog_cap_t;

typedef struct {
  unsigned char cmd;
  unsigned char wmode;
  unsigned char ctype;
} __attribute__((__packed__)) _alog_copy_t;

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_string16(int nbytes, char *string)
{
  uint16_t n = nbytes;

  awrite_ul(_alog->fd, &n, sizeof(n), "alog_append_string16: Error with write!\n");
  awrite_ul(_alog->fd, string, nbytes, "alog_append_string16: Error with write!\n");

  return(0);
}

//------------------------------------------------------------------------

int _alog_read_string16(activity_log_t *alog, int *nbytes, char *string)
{
  uint16_t n;

  aread(alog->fd, &n, sizeof(n), "alog_append_string16: Error with read!\n");
  aread(alog->fd, string, n, "alog_append_string16: Error with read!\n");

  *nbytes = n;

  return(0);
}

//------------------------------------------------------------------------

int _alog_append_cap(int port, int family, const char *address, const char *key, const char *typekey)
{
  _alog_cap_t a;
  int n;

  alog_mode_check();

  a.port = port;
  a.family = 0;  n = 4;
  if (family != AF_INET) { a.family = 1; n = 16; }
  a.key_size = strlen(key)+1;
  a.typekey_size = strlen(typekey)+1;

  awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_cap: Error with write!\n");
  awrite_ul(_alog->fd, address, n, "alog_append_cap: Error with write!\n");
  awrite_ul(_alog->fd, key, a.key_size, "alog_append_cap: Error with write!\n");
  awrite_ul(_alog->fd, typekey, a.typekey_size, "alog_append_cap: Error with write!\n");
  
  return(0);
}

//------------------------------------------------------------------------

int _alog_read_cap(activity_log_t *alog, int *port, int *family, char *address, char *key, char *typekey)
{
  _alog_cap_t a;
  int n;

  aread(alog->fd, &a, sizeof(a), "alog_read_cap: Error with read!\n");

  *port = a.port;
  *family = AF_INET; n = 4;
  if (a.family != 0) { *family = AF_INET6; n = 16; }

  aread(alog->fd, address, n, "alog_read_cap: Error with read!\n");
  aread(alog->fd, key, a.key_size, "alog_read_cap: Error with read!\n");
  aread(alog->fd, typekey, a.typekey_size, "alog_read_cap: Error with read!\n");
  
  return(0);
}

//------------------------------------------------------------------------

int _alog_append_alias_copy32(int cmd, int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t size, 
     uint64_t offset, uint64_t off2, int write_mode, int ctype, char *path, int port, int family, const char *address, const char *key, const char *typekey)
{
   _alog_alias_copy_append32_t a;
   _alog_copy_t ca;
   uint32_t offset2;

   alog_mode_check();

   a.id = id;
   a.pid = pid;
   a.size = size;
   a.ri = ri;
   a.offset = offset;
   
   offset2 = off2;
   ca.cmd = cmd;
   ca.wmode = write_mode;
   ca.ctype = ctype;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_COPY32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_copy32: Error with write!\n");
   awrite_ul(_alog->fd, &ca, sizeof(ca), "alog_append_alias_copy32: Error with write!\n");
   awrite_ul(_alog->fd, &offset2, sizeof(offset2), "alog_append_alias_copy32: Error with write!\n");
   _alog_append_cap(port, family, address, key, typekey);
   if (ctype != IBP_TCP) _alog_append_string16(strlen(path)+1, path);

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_copy32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_copy_append32_t a;
   _alog_copy_t ca;
   uint32_t off32;
   uint64_t offset2, offset, size;
   int port, family, nbytes;
   char address[16], key[512], typekey[512], host[128];
   char path[4096], tmp[4096];

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_copy32: Error with read!\n");
   aread(alog->fd, &ca, sizeof(ca), "alog_read_alias_copy32: Error with read!\n");
   aread(alog->fd, &off32, sizeof(off32), "alog_read_alias_copy32: Error with read!\n");
   _alog_read_cap(alog, &port, &family, address, key, typekey);

   if (ca.ctype != IBP_TCP) {
     _alog_read_string16(alog, &nbytes, tmp);
     sprintf(path, "PATH:%s", tmp);
   } else {
     path[0] = '\0';
   }    

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      offset2 = off32; offset = a.offset; size = a.size;
      address2ipdecstr(host, address, family);
      fprintf(outfd, "CMD:%s ctype:%s %s wmode:%s RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU 
         " host:%s:%d key:%s typekey:%s offset2:" LU "\n",
         _ibp_cmd_map[ca.cmd], _ibp_ctype_map[ca.ctype], path, _ibp_wmode_map[ca.wmode], alog->rl_map[a.ri].name, 
         a.pid, a.id, offset, size, host, port, key, typekey, offset2);
   }

   return(0);
}

int _alog_append_alias_copy64(int cmd, int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t size, 
     uint64_t offset, uint64_t offset2, int write_mode, int ctype, char *path, int port, int family, const char *address, const char *key, const char *typekey)
{
   _alog_alias_copy_append64_t a;
   _alog_copy_t ca;

   alog_mode_check();

   a.id = id;
   a.pid = pid;
   a.size = size;
   a.ri = ri;
   a.offset = offset;

   ca.cmd = cmd;
   ca.wmode = write_mode;
   ca.ctype = ctype;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_COPY64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_copy64: Error with write!\n");
   awrite_ul(_alog->fd, &ca, sizeof(ca), "alog_append_alias_copy64: Error with write!\n");
   awrite_ul(_alog->fd, &offset2, sizeof(offset2), "alog_append_alias_copy64: Error with write!\n");
   _alog_append_cap(port, family, address, key, typekey);
   if (ctype != IBP_TCP) _alog_append_string16(strlen(path)+1, path);

   alog_unlock();
   return(0);
}

int alog_read_alias_copy64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_copy_append64_t a;
   _alog_copy_t ca;
   uint64_t offset2;
   int port, family, nbytes;
   char address[16], key[512], typekey[512], host[128];
   char path[4096], tmp[4096];

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_copy64: Error with read!\n");
   aread(alog->fd, &ca, sizeof(ca), "alog_read_alias_copy64: Error with read!\n");
   aread(alog->fd, &offset2, sizeof(offset2), "alog_read_alias_copy64: Error with read!\n");
   _alog_read_cap(alog, &port, &family, address, key, typekey);

   if (ca.ctype != IBP_TCP) {
     _alog_read_string16(alog, &nbytes, tmp);   
     sprintf(path, "PATH:%s", tmp);
   } else {
     path[0] = '\0';
   }    

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      address2ipdecstr(host, address, family);
      fprintf(outfd, "CMD:%s ctype:%s %s wmode:%s RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU 
         " host:%s:%d key:%s typekey:%s offset2:" LU "\n",
         _ibp_cmd_map[ca.cmd], _ibp_ctype_map[ca.ctype], path, _ibp_wmode_map[ca.wmode], alog->rl_map[a.ri].name, 
         a.pid, a.id, a.offset, a.size, host, port, key, typekey, offset2);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_copy64(int cmd, int tid, int ri, osd_id_t id, uint64_t size, 
     uint64_t offset, uint64_t offset2, int write_mode, int ctype, char *path, int port, int family, const char *address, const char *key, const char *typekey)
{
   _alog_copy_append64_t a;
   _alog_copy_t ca;

   alog_mode_check();

   a.id = id;
   a.size = size;
   a.ri = ri;
   a.offset = offset;

   ca.cmd = cmd;
   ca.wmode = write_mode;
   ca.ctype = ctype;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_COPY64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_copy64: Error with write!\n");
   awrite_ul(_alog->fd, &ca, sizeof(ca), "alog_append_copy64: Error with write!\n");
   awrite_ul(_alog->fd, &offset2, sizeof(offset2), "alog_append_copy64: Error with write!\n");
   _alog_append_cap(port, family, address, key, typekey);
   if (ctype != IBP_TCP) _alog_append_string16(strlen(path)+1, path);

   alog_unlock();
   return(0);
}

int alog_read_copy64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_copy_append64_t a;
   _alog_copy_t ca;
   uint64_t offset2;
   int port, family, nbytes;
   char address[16], key[512], typekey[512], host[128];
   char path[4096], tmp[4096];

   aread(alog->fd, &a, sizeof(a), "alog_read_copy64: Error with read!\n");
   aread(alog->fd, &ca, sizeof(ca), "alog_read_copy64: Error with read!\n");
   aread(alog->fd, &offset2, sizeof(offset2), "alog_read_copy64: Error with read!\n");
   _alog_read_cap(alog, &port, &family, address, key, typekey);

   if (ca.ctype != IBP_TCP) {
     _alog_read_string16(alog, &nbytes, tmp);   
     sprintf(path, "PATH:%s", tmp);
   } else {
     path[0] = '\0';
   }    

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      address2ipdecstr(host, address, family);
      fprintf(outfd, "CMD:%s ctype:%s %s wmode:%s RID:%s id:" LU " offset:" LU " size:" LU 
         " host:%s:%d key:%s typekey:%s offset2:" LU "\n",
         _ibp_cmd_map[ca.cmd], _ibp_ctype_map[ca.ctype], path, _ibp_wmode_map[ca.wmode], alog->rl_map[a.ri].name, 
         a.id, a.offset, a.size, host, port, key, typekey, offset2);
   }

   return(0);
}


//------------------------------------------------------------------------

int _alog_append_copy32(int cmd, int tid, int ri, osd_id_t id, uint64_t size, 
     uint64_t offset, uint64_t offset2, int write_mode, int ctype, char *path, int port, int family, const char *address, const char *key, const char *typekey)
{
   _alog_copy_append32_t a;
   _alog_copy_t ca;
   uint32_t off32 = offset2;

   alog_mode_check();

   a.id = id;
   a.size = size;
   a.ri = ri;
   a.offset = offset;

   ca.cmd = cmd;
   ca.wmode = write_mode;
   ca.ctype = ctype;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_COPY32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_copy32: Error with write!\n");
   awrite_ul(_alog->fd, &ca, sizeof(ca), "alog_append_copy32: Error with write!\n");
   awrite_ul(_alog->fd, &off32, sizeof(off32), "alog_append_copy32: Error with write!\n");
   _alog_append_cap(port, family, address, key, typekey);
   if (ctype != IBP_TCP) _alog_append_string16(strlen(path)+1, path);

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_copy32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_copy_append32_t a;
   _alog_copy_t ca;
   uint32_t off32;
   uint64_t offset2, offset, size;
   int port, family, nbytes;
   char address[16], key[512], typekey[512], host[128];
   char path[4096], tmp[4096];

   aread(alog->fd, &a, sizeof(a), "alog_read_copy32: Error with read!\n");
   aread(alog->fd, &ca, sizeof(ca), "alog_read_copy32: Error with read!\n");
   aread(alog->fd, &off32, sizeof(off32), "alog_read_copy32: Error with read!\n");
   _alog_read_cap(alog, &port, &family, address, key, typekey);

   if (ca.ctype != IBP_TCP) {
     _alog_read_string16(alog, &nbytes, tmp);   
     sprintf(path, "PATH:%s", tmp);
   } else {
     path[0] = '\0';
   }    

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      offset2 = off32; offset = a.offset; size = a.size;
      address2ipdecstr(host, address, family);
      fprintf(outfd, "CMD:%s ctype:%s %s wmode:%s RID:%s id:" LU " offset:" LU " size:" LU 
         " host:%s:%d key:%s typekey:%s offset2:" LU "\n",
         _ibp_cmd_map[ca.cmd], _ibp_ctype_map[ca.ctype], path, _ibp_wmode_map[ca.wmode], alog->rl_map[a.ri].name, 
         a.id, offset, size, host, port, key, typekey, offset2);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int alog_append_dd_copy(int cmd, int tid, int ri, osd_id_t pid, osd_id_t id, 
       uint64_t size, uint64_t offset, uint64_t offset2, int write_mode, int ctype, char *path, 
       int port, int family, const char *address, const char *key, const char *typekey)
{

  alog_mode_check();

  if ((size < UINT32_MAX) && (offset<UINT32_MAX) && (offset2<UINT32_MAX)) {
     if (pid == 0) {
        return(_alog_append_copy32(cmd, tid, ri, id, size, offset, offset2, write_mode, ctype, path, port, family, address, key, typekey));
     } else {
        return(_alog_append_alias_copy32(cmd, tid, ri, pid, id, size, offset, offset2, write_mode, ctype, path, port, family, address, key, typekey));
     }
  } else {
     if (pid == 0) {
        return(_alog_append_copy64(cmd, tid, ri, id, size, offset, offset2, write_mode, ctype, path, port, family, address, key, typekey));
     } else {
        return(_alog_append_alias_copy64(cmd, tid, ri, pid, id, size, offset, offset2, write_mode, ctype, path, port, family, address, key, typekey));
     }
  }

  return(0);
}

//************************************************************************
//  IBP_READ
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_read64_t;

typedef struct {
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_read32_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_read64_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_read32_t;


//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_read32(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_read32_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_READ32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_READ32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_read32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_read32_t a;
   uint64_t offset, size;

   aread(alog->fd, &a, sizeof(a), "alog_read_read32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      offset = a.offset; size = a.size;
      fprintf(outfd, "CMD:IBP_LOAD RID:%s id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, offset, size);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_read64(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_read64_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_READ64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_write64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_read64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_read64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_read64: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_LOAD RID:%s id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, a.offset, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_alias_read32(int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_read32_t a;

   alog_mode_check();

//int d = ftell(_alog->fd);
//log_printf(0, "_alog_append_alias_read32:  Start!!!!!!!!!!! fpos=%d\n", d);
   a.id = id;
   a.pid = pid;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_READ32);
//d = ftell(_alog->fd);
//log_printf(0, "_alog_append_alias_read32: after header fpos=%d\n", d);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_read32: Error with write!\n");
//d = ftell(_alog->fd);
//log_printf(0, "_alog_append_alias_read32: after rec fpos=%d\n", d);

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_read32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_read32_t a;
   uint64_t offset, size;

//log_printf(0, "alog_read_alias_read32:  Start!!!!!!!!!\n");
   aread(alog->fd, &a, sizeof(a), "alog_read_alias_read32: Error with read!\n");

   if (outfd != NULL) {
//log_printf(0, "alog_read_alias_read32:  Printing data\n");
      if (a.ri == 255) a.ri = alog->nres;
      offset = a.offset; size = a.size;
      fprintf(outfd, "CMD:IBP_LOAD RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, offset, size);
   }

//log_printf(0, "alog_read_alias_read32:  End!!!!!!!!!\n");

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_alias_read64(int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_read64_t a;

   a.id = id;
   a.pid = pid;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_READ64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_read32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_read64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_read64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_write32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_LOAD RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, a.offset, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int alog_append_read(int tid, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{

  if (offset < UINT32_MAX) {
     if (pid == 0) {
        return(_alog_append_read32(tid, ri, id, offset, size));
     } else {
        return(_alog_append_alias_read32(tid, ri, pid, id, offset, size));
     }
  } else {
     if (pid == 0) {
        return(_alog_append_read64(tid, ri, id, offset, size));
     } else {
        return(_alog_append_alias_read64(tid, ri, pid, id, offset, size));
     }
  }

  return(0);
}

//************************************************************************
//  IBP_WRITE/IBP_STORE
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t size;
   uint8_t ri;
} __attribute__((__packed__)) _alog_write_append64_t;

typedef struct {
   osd_id_t id;
   uint32_t size;
   uint8_t ri;
} __attribute__((__packed__)) _alog_write_append32_t;

typedef struct {
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_write64_t;

typedef struct {
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_write32_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint64_t size;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_write_append64_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint32_t size;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_write_append32_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint64_t size;
   uint64_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_write64_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint32_t size;
   uint32_t offset;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_write32_t;

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_write32(int tid, int cmd, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_write32_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_WRITE32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_write32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_write32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_write32_t a;
   uint64_t offset, size;

   aread(alog->fd, &a, sizeof(a), "alog_read_write32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      offset = a.offset; size = a.size;
      fprintf(outfd, "CMD:IBP_WRITE RID:%s id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, offset, size);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_write64(int tid, int cmd, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_write64_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_WRITE64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_write64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_write64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_write64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_write64: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_WRITE RID:%s id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, a.offset, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_write_append32(int tid, int cmd, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_write_append32_t a;

   alog_mode_check();

   a.id = id;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_WRITE_APPEND32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_write_append32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_write_append32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_write_append32_t a;
   uint64_t size;

   aread(alog->fd, &a, sizeof(a), "alog_read_write_append32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      size = a.size;
      fprintf(outfd, "CMD:IBP_STORE RID:%s id:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, size);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_write_append64(int tid, int cmd, int ri, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_write_append64_t a;

   alog_mode_check();

   a.id = id;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_WRITE_APPEND64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_write64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_write_append64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_write_append64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_write_append64: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_STORE RID:%s id:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.id, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_alias_write32(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_write32_t a;

   alog_mode_check();

   a.id = id;
   a.pid = pid;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_WRITE32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_write32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_write32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_write32_t a;
   uint64_t offset, size;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_write32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      offset = a.offset; size = a.size;
      fprintf(outfd, "CMD:IBP_WRITE RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, offset, size);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_alias_write64(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_write64_t a;

   alog_mode_check();

   a.id = id;
   a.pid = pid;
   a.offset = offset;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_WRITE64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_write64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_write64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_write64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_write64: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_WRITE RID:%s pid:" LU " id:" LU " offset:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, a.offset, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_alias_write_append32(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_write_append32_t a;

   alog_mode_check();

   a.pid = pid;
   a.id = id;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_WRITE_APPEND32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_write_append32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_write_append32(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_write_append32_t a;
   uint64_t size;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_write_append32: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      size = a.size;
      fprintf(outfd, "CMD:IBP_STORE RID:%s pid:" LU " id:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, size);
   }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_alias_write_append64(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
   _alog_alias_write_append64_t a;

   alog_mode_check();

   a.pid = pid;
   a.id = id;
   a.size = size;
   a.ri = ri;
   
   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_WRITE_APPEND64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_write64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_write_append64(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_write_append64_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_write_append64: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      fprintf(outfd, "CMD:IBP_STORE RID:%s pid:" LU " id:" LU " size:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id, a.size);
   }

   return(0);
}

//------------------------------------------------------------------------

int alog_append_write(int tid, int cmd, int ri, osd_id_t pid, osd_id_t id, uint64_t offset, uint64_t size)
{
  alog_mode_check();

  if (cmd == IBP_WRITE) {
     if (offset < UINT32_MAX) {
        if (pid == 0) {
           return(_alog_append_write32(tid, cmd, ri, id, offset, size));
        } else {
           return(_alog_append_alias_write32(tid, cmd, ri, pid, id, offset, size));
        }
     } else {
        if (pid == 0) {
           return(_alog_append_write64(tid, cmd, ri, id, offset, size));
        } else {
           return(_alog_append_alias_write64(tid, cmd, ri, pid, id, offset, size));
        }
     }
  } else { //** Append write
     if (offset < UINT32_MAX) {
        if (pid == 0) {
           return(_alog_append_write_append32(tid, cmd, ri, id, offset, size));
        } else {
           return(_alog_append_alias_write_append32(tid, cmd, ri, pid, id, offset, size));
        }
     } else {
        if (pid == 0) {
           return(_alog_append_write_append64(tid, cmd, ri, id, offset, size));
        } else {
           return(_alog_append_alias_write_append64(tid, cmd, ri, pid, id, offset, size));
        }
     }
  }

  return(0);
}

//************************************************************************
//  IBP_MANAGE/IBP_PROBE
//************************************************************************

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_manage_probe_t;

int alog_append_alias_manage_probe(int tid, int ri, osd_id_t pid, osd_id_t id)
{
   _alog_alias_manage_probe_t a;

   alog_mode_check();

   a.pid = pid;
   a.id = id;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_MANAGE_PROBE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_manage_probe: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_manage_probe(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_manage_probe_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_manage_probe: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_ALIAS_MANAGE SUBCMD:IBP_PROBE RID:%s pid:" LU " id:" LU "\n",
         alog->rl_map[a.ri].name, a.pid, a.id);
   }

   return(0);

}


//************************************************************************
//  IBP_MANAGE/IBP_PROBE
//************************************************************************

typedef struct {
   osd_id_t id;
   uint8_t ri;
} __attribute__((__packed__)) _alog_manage_probe_t;

int alog_append_manage_probe(int tid, int ri, osd_id_t id)
{
   _alog_manage_probe_t a;

   alog_mode_check();

   a.id = id;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_MANAGE_PROBE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_manage_probe: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_manage_probe(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_manage_probe_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_manage_probe: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_MANAGE SUBCMD:IBP_PROBE RID:%s id:" LU "\n",
         alog->rl_map[a.ri].name, a.id);
   }

   return(0);

}

//************************************************************************
//  IBP_MANAGE/IBP_CHNG
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t size;
   uint64_t time;
   uint8_t ri;
   uint8_t rel;
} __attribute__((__packed__)) _alog_manage_change_t;

int alog_append_manage_change(int tid, int ri, osd_id_t id, uint64_t size, int rel, apr_time_t t)
{
   _alog_manage_change_t a;

   alog_mode_check();

   a.id = id;
   a.size = size;
   a.time = t;
   a.rel = rel;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_MANAGE_CHANGE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_manage_change: Error with write!\n");

   alog_unlock();
   return(0);

}

//------------------------------------------------------------------------

int alog_read_manage_change(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_manage_change_t a;
   char buf[128];
   apr_time_t t;

   aread(alog->fd, &a, sizeof(a), "alog_read_manage_change: Error with read!\n");

   if (outfd != NULL) {
      t = a.time;
      apr_ctime(buf, ibp2apr_time(a.time));
      buf[strlen(buf)-1] = '\0';  //** chomp the \n from ctime_r
      fprintf(outfd, "CMD:IBP_MANAGE SUBCMD:IBP_CHNG RID:%s id:" LU " reliablity:%s size:" LU " expiration: %s(" TT")\n",
         alog->rl_map[a.ri].name, a.id, _ibp_rel_map[a.rel], a.size, buf, t);
   }

   return(0);

}

//************************************************************************
//  IBP_ALIAS_MANAGE/IBP_CHNG
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t offset;
   uint64_t size;
   uint64_t time;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_manage_change_t;

int alog_append_alias_manage_change(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t t)
{
   _alog_alias_manage_change_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.time = t;
   a.ri = ri;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_MANAGE_CHANGE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_manage_change: Error with write!\n");

   alog_unlock();
   return(0);

}

//------------------------------------------------------------------------

int alog_read_alias_manage_change(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_manage_change_t a;
   char buf[128];
   apr_time_t t;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_manage_change: Error with read!\n");

   if (outfd != NULL) {
      t = a.time;
      apr_ctime(buf, ibp2apr_time(a.time));
      buf[strlen(buf)-1] = '\0';  //** chomp the \n from ctime_r
      fprintf(outfd, "CMD:IBP_ALIAS_MANAGE SUBCMD:IBP_CHNG RID:%s  id:" LU " offset:" LU " size:" LU " expiration: %s(" TT")\n",
         alog->rl_map[a.ri].name, a.id, a.offset, a.size, buf, t);
   }

   return(0);

}

//************************************************************************
//  IBP_*_MANAGE/IBP_DECR|IBP_INCR
//************************************************************************

typedef struct {
   osd_id_t id;
   uint8_t ri;
   uint8_t captype;
   uint8_t subcmd;
} __attribute__((__packed__)) _alog_manage_incdec_t;

typedef struct {
   osd_id_t pid;
   osd_id_t id;
   uint8_t ri;
   uint8_t captype;
   uint8_t subcmd;
} __attribute__((__packed__)) _alog_alias_manage_incdec_t;

//------------------------------------------------------------------------

int _alog_append_pm_incdec(int tid, int cmd, int subcmd, int ri, osd_id_t pid, osd_id_t id, int cap_type)
{
   _alog_alias_manage_incdec_t a;

   alog_mode_check();

   a.id = id;
   a.pid = pid;
   a.ri = ri;
   a.captype = cap_type;
   a.subcmd = subcmd;
   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_MANAGE_INCDEC);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_pm_incdec: Error with write!\n");

   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_manage_incdec(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alias_manage_incdec_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_manage_incdec: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_ALIAS_MANAGE SUBCMD:%s RID:%s pid:" LU " id:" LU " cap_type:%s\n",
         _ibp_subcmd_map[a.subcmd], alog->rl_map[a.ri].name, a.pid, a.id, _ibp_captype_map[a.captype]);
   }

   return(0);

}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int _alog_append_m_incdec(int tid, int cmd, int subcmd, int ri, osd_id_t id, int cap_type)
{
   _alog_manage_incdec_t a;

   alog_mode_check();

   a.id = id;
   a.ri = ri;
   a.captype = cap_type;
   a.subcmd = subcmd;
   _alog->append_header(_alog->fd, tid, ALOG_REC_MANAGE_INCDEC);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_m_incdec: Error with write!\n");

   return(0);
}

//------------------------------------------------------------------------

int alog_read_manage_incdec(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_manage_incdec_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_alias_manage_incdec: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_MANAGE SUBCMD:%s RID:%s id:" LU " cap_type:%s\n",
         _ibp_subcmd_map[a.subcmd], alog->rl_map[a.ri].name, a.id, _ibp_captype_map[a.captype]);
   }

   return(0);

}

//------------------------------------------------------------------------
//------------------------------------------------------------------------

int alog_append_manage_incdec(int tid, int cmd, int subcmd, int ri, osd_id_t pid, osd_id_t id, int cap_type)
{
   alog_mode_check();

   alog_lock();
   alog_checksize();

   if (cmd == IBP_ALIAS_MANAGE) {
     _alog_append_pm_incdec(tid, cmd, subcmd, ri, pid, id, cap_type);
   } else {
     _alog_append_m_incdec(tid, cmd, subcmd, ri, id, cap_type);
   }

   alog_unlock();
   return(0);    
}

//************************************************************************
// routines to handle the various IBP_MANAGE/IBP_ALIAS_MANGE early failures
//************************************************************************

typedef struct {
   uint8_t cmd;
   uint8_t subcmd;
} __attribute__((__packed__)) _alog_manage_bad_t;


int alog_append_manage_bad(int tid, int command, int subcmd)
{
   _alog_manage_bad_t a;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   a.cmd = command;
   a.subcmd = subcmd;
   _alog->append_header(_alog->fd, tid, ALOG_REC_MANAGE_BAD);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_manage_bad: Error with write!\n");

   alog_unlock();
   return(0);    
}

//------------------------------------------------------------------------

int alog_read_manage_bad(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_manage_bad_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_manage_bad: Error with read!\n");

   if (outfd != NULL) {
      if (a.cmd == IBP_MANAGE) {
         fprintf(outfd, "CMD:IBP_MANAGE SUBCMD:%s\n", _ibp_subcmd_map[a.subcmd]);
      } else {
         fprintf(outfd, "CMD:IBP_ALIAS_MANAGE SUBCMD:%s\n", _ibp_subcmd_map[a.subcmd]);
      }
   }

   return(0);

}

//************************************************************************
//  generic routines for handling IBP_STATUS/IBP_ST_INQ
//************************************************************************

int alog_append_status_inq(int tid, int ri)
{
   uint8_t a;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   a = ri;
   _alog->append_header(_alog->fd, tid, ALOG_REC_STATUS_INQ);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_status_inq: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_status_inq(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint8_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_stats_inq: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_STATUS SUBCMD:IP_ST_INQ  RID:%s \n", alog->rl_map[a].name);
   }

   return(0);

}

//************************************************************************
//  generic routines for handling IBP_STATUS/IBP_ST_STATS
//************************************************************************

int alog_append_status_stats(int tid, apr_time_t start_time)
{
   uint32_t a;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   a = start_time;
   _alog->append_header(_alog->fd, tid, ALOG_REC_STATUS_STATS);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_status_stats: Error with write!\n");

   alog_unlock();
   return(0);  
}

//------------------------------------------------------------------------

int alog_read_status_stats(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint32_t a;
   apr_time_t t;

   aread(alog->fd, &a, sizeof(a), "alog_read_stats_stats: Error with read!\n");

   if (outfd != NULL) {
      t = a;
      fprintf(outfd, "CMD:IBP_STATUS SUBCMD:IP_ST_STATS start_time:" TT " \n", t);
   }

   return(0);

}

//************************************************************************
// generic routines to handle simple subcommand
//************************************************************************

int alog_append_subcmd(int tid, int command, int subcmd)
{
   uint8_t a;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   a = subcmd;
   _alog->append_header(_alog->fd, tid, command);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_subcmd: Error with write!\n");

   alog_unlock();
   return(0);  
}

//------------------------------------------------------------------------

int alog_read_subcmd(const char *cmd_name, activity_log_t *alog, const char **sub_name, FILE *outfd)
{
   uint8_t a;

   aread(alog->fd, &a, sizeof(a), "alog_read_subcmd: Error with read!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:%s SUBCMD:%s\n", cmd_name, sub_name[a]);
   }

   return(0);
}

//************************************************************************
//  IBP_STATUS/IBP_ST_CHANGE routine
//************************************************************************

int alog_append_status_change(int tid)
{
   return(alog_append_subcmd(tid, ALOG_REC_STATUS_CHANGE, IBP_ST_CHANGE));
}

//************************************************************************
//  IBP_STATUS/IBP_ST_RES routine
//************************************************************************

int alog_append_status_res(int tid)
{
   return(alog_append_subcmd(tid, ALOG_REC_STATUS_VERSION, IBP_ST_RES));
}

//************************************************************************
//  IBP_STATUS/IBP_ST_VERSION routine
//************************************************************************

int alog_append_status_version(int tid)
{
   return(alog_append_subcmd(tid, ALOG_REC_STATUS_VERSION, IBP_ST_VERSION));
}

//------------------------------------------------------------------------

int alog_read_status_subcmd(activity_log_t *alog, int cmd, FILE *outfd)
{
   return(alog_read_subcmd("IBP_STATUS", alog, _ibp_st_map, outfd));
}

//************************************************************************
// generic routine that store/print the resource index and osd_id
//************************************************************************

typedef struct {
   osd_id_t id;
   uint8_t ri;
} __attribute__((__packed__)) _alog_res_id_t;

int alog_append_res_id(int command, int tid, int rindex, osd_id_t id)
{
   _alog_res_id_t a;

   alog_mode_check();

   if (rindex == -1) rindex = 255;

   a.ri = rindex;
   a.id = id;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, command);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_res_id: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_res_id(const char *cmd_name, activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_res_id_t a;
   int ri;

   aread(alog->fd, &a, sizeof(a), "alog_read_res_id: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      ri = a.ri;
      fprintf(outfd, "CMD:%s RID: %s oid:" LU "\n", cmd_name, alog->rl_map[ri].name, a.id);
   }

   return(0);
}

//************************************************************************
// alias_get_alloc commands
//************************************************************************

typedef struct {
   osd_id_t id;
   uint64_t offset;
   uint64_t size;
   uint32_t expire;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_alloc64_t;

typedef struct {
   osd_id_t id;
   uint32_t offset;
   uint32_t size;
   uint32_t expire;
   uint8_t ri;
} __attribute__((__packed__)) _alog_alias_alloc32_t;

//------------------------------------------------------------------------

int _alog_append_alias_alloc32(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t expire)
{
   _alog_alias_alloc32_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.expire = expire;
   a.ri = ri;

   alog_lock();
   alog_checksize();
   
   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_ALLOC32);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_alloc32: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_alloc32(activity_log_t *alog, int cmd, FILE *outfd)
{
  _alog_alias_alloc32_t a;
  uint64_t offset, size;
  apr_time_t t;
  char buf[128];

  aread(alog->fd, &a, sizeof(a), "alog_read_alias_alloc32: Error with read!\n");

  if (outfd != NULL) {
     if (a.ri == 255) a.ri = alog->nres;
     offset = a.offset; size = a.size; t = a.expire;
     apr_ctime(buf, ibp2apr_time(a.expire));
     buf[strlen(buf)-1] = '\0';  //** chomp the \n from ctime_r

     fprintf(outfd, "CMD:IBP_ALIAS_ALLOCATE  RID:%s id:" LU " offset:" LU " size:" LU " expire:%s(" TT ")\n", 
         alog->rl_map[a.ri].name, a.id, offset, size, buf, t);
  }

   return(0);
}

//------------------------------------------------------------------------

int _alog_append_alias_alloc64(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t expire)
{
   _alog_alias_alloc64_t a;

   alog_mode_check();

   a.id = id;
   a.offset = offset;
   a.size = size;
   a.expire = expire;
   a.ri = ri;

   alog_lock();
   alog_checksize();
   
   _alog->append_header(_alog->fd, tid, ALOG_REC_ALIAS_ALLOC64);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_alias_alloc64: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_alloc64(activity_log_t *alog, int cmd, FILE *outfd)
{
  _alog_alias_alloc64_t a;
  uint64_t offset, size;
  apr_time_t t;
  char buf[128];

  aread(alog->fd, &a, sizeof(a), "alog_read_alias_alloc64: Error with read!\n");

  if (outfd != NULL) {
     if (a.ri == 255) a.ri = alog->nres;
     offset = a.offset; size = a.size; t = a.expire;
     apr_ctime(buf, ibp2apr_time(a.expire));
     buf[strlen(buf)-1] = '\0';  //** chomp the \n from ctime_r

     fprintf(outfd, "CMD:IBP_ALIAS_ALLOCATE  RID:%s id:" LU " offset:" LU " size:" LU " expire:%s(" TT ")\n", 
         alog->rl_map[a.ri].name, a.id, offset, size, buf, t);
  }

   return(0);
}

//------------------------------------------------------------------------

int alog_append_alias_alloc(int tid, int ri, osd_id_t id, uint64_t offset, uint64_t size, apr_time_t expire)
{
   alog_mode_check();

   if (ri == -1) ri = 255;

   if ((offset < UINT32_MAX) && (size < UINT32_MAX)) {
      _alog_append_alias_alloc32(tid, ri, id, offset, size, expire);
   } else {
      _alog_append_alias_alloc64(tid, ri, id, offset, size, expire);
   } 

   return(0);
}

//------------------------------------------------------------------------

int alog_read_alias_alloc(activity_log_t *alog, int cmd, FILE *outfd)
{
   return(alog_read_res_id("IBP_ALIAS_ALLOCATE", alog, cmd, outfd));
}

//************************************************************************
// internal_get_alloc commands  -- This uses the same formay as the rename
//************************************************************************

int alog_append_internal_get_alloc(int tid, int rindex, osd_id_t id)
{
   return(alog_append_res_id(ALOG_REC_INTERNAL_GET_ALLOC, tid, rindex, id));
}

//------------------------------------------------------------------------

int alog_read_internal_get_alloc(activity_log_t *alog, int cmd, FILE *outfd)
{
   return(alog_read_res_id("INTERNAL_GET_ALLOC", alog, cmd, outfd));
}


//************************************************************************
// ibp_rename commands
//************************************************************************

int alog_append_ibp_rename(int tid, int rindex, osd_id_t id)
{
   return(alog_append_res_id(ALOG_REC_IBP_RENAME, tid, rindex, id));
}

//------------------------------------------------------------------------

int alog_read_ibp_rename(activity_log_t *alog, int cmd, FILE *outfd)
{
   return(alog_read_res_id("IBP_RENAME", alog, cmd, outfd));
}

//************************************************************************
// ibp_merge_allocate commands
//************************************************************************

int alog_append_ibp_merge(int tid, osd_id_t mid, osd_id_t cid, int rindex)
{
   _alog_res_id_t a;

   alog_mode_check();

   if (rindex == -1) rindex = 255;

   a.ri = rindex;
   a.id = mid;

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_IBP_MERGE);
   awrite_ul(_alog->fd, &a, sizeof(a), "alog_append_ibp_merge: Error with write!\n");

   awrite_ul(_alog->fd, &cid, sizeof(cid), "alog_append_ibp_merge: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_ibp_merge(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_res_id_t a;
   osd_id_t cid;
   int ri;

   aread(alog->fd, &a, sizeof(a), "alog_read_ibp_merge: Error with read!\n");
   aread(alog->fd, &cid, sizeof(cid), "alog_read_ibp_merge: Error with read!\n");

   if (outfd != NULL) {
      if (a.ri == 255) a.ri = alog->nres;
      ri = a.ri;
      fprintf(outfd, "CMD:IBP_MERGE_ALLOCATE RID: %s mid:" LU " cid:" LU "\n", 
         alog->rl_map[ri].name, a.id, cid);
   }

   return(0);
}

//************************************************************************
// Routines to print osi_id's
//************************************************************************

int alog_append_osd_id(int tid, osd_id_t id)
{
   alog_mode_check();

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_OSD_ID);
   awrite_ul(_alog->fd, &id, sizeof(id), "alog_append_osd_id: Error with write!\n");

   alog_unlock();
   return(0);
}

//------------------------------------------------------------------------

int alog_read_osd_id(activity_log_t *alog, int cmd, FILE *outfd)
{
  osd_id_t id;
  
  aread(alog->fd, &id, sizeof(id), "alog_read_osd_id: Error with read!\n");

  if (outfd != NULL) {
     fprintf(outfd, "CMD:OSD_ID " LU "\n", id);
  }

  return(0);
}


//************************************************************************
//  ibp_allocate commands
//************************************************************************
typedef struct {
   uint32_t size;
   uint32_t expiration;
   uint8_t ri;
   uint8_t atype;
   uint8_t rel;
} __attribute__((__packed__)) _alog_alloc32_t;

typedef struct {
   uint64_t size;
   uint32_t expiration;
   uint8_t ri;
   uint8_t atype;
   uint8_t rel;
} __attribute__((__packed__)) _alog_alloc64_t;

int alog_append_ibp_allocate(int tid, int rindex, uint64_t max_size, int atype, int rel, apr_time_t expiration)
{
   void *d;
   int nbytes, cmd;
   _alog_alloc64_t a64;
   _alog_alloc32_t a32;

   alog_mode_check();

   if (rindex == -1) rindex = 255;

   alog_lock();
   alog_checksize();
     
   if (max_size < UINT32_MAX) {
      nbytes = sizeof(a32);
      cmd = ALOG_REC_IBP_ALLOCATE32;
      d = (void *)&a32;
      a32.ri=rindex; a32.atype=atype; a32.rel=rel; a32.expiration=expiration; a32.size=max_size;
   } else {
      nbytes = sizeof(a64);
      cmd = ALOG_REC_IBP_ALLOCATE64;
      d = (void *)&a64;
      a64.ri=rindex; a64.atype=atype; a64.rel=rel; a64.expiration=expiration; a64.size=max_size;
   } 

   _alog->append_header(_alog->fd, tid, cmd);
   awrite_ul(_alog->fd, d, nbytes, "alog_append_ibp_allocate: Error with write!\n");
 
   alog_unlock();
   return(0);
}

//--------------------------------------------------------------------------

int alog_read_ibp_allocate(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alloc64_t a64;
   _alog_alloc32_t a32;
   int ri, atype, rel;
   apr_time_t t;
   uint64_t msize;
   char buffer[256];
   
   if (cmd == ALOG_REC_IBP_ALLOCATE32) {
     aread(alog->fd, &a32, sizeof(a32), "alog_read_ibp_allocate: Error with read!\n");
     ri=a32.ri; atype=a32.atype; rel=a32.rel; t=a32.expiration; msize=a32.size;
   } else {
     aread(alog->fd, &a64, sizeof(a32), "alog_read_ibp_allocate: Error with read!\n");
     ri=a64.ri; atype=a64.atype; rel=a64.rel; t=a64.expiration; msize=a64.size;
   }    

   if (outfd != NULL) {
      if (ri == 255) ri = alog->nres;
      apr_ctime(buffer, ibp2apr_time(t));
      buffer[strlen(buffer)-1] = '\0';  //** chomp the \n from ctime_r
      fprintf(outfd, "CMD:IBP_ALLOCATE RID:%s type:%s rel:%s size=" LU " expiration:%s(" TT ")\n", 
         alog->rl_map[ri].name, _ibp_type_map[atype], _ibp_rel_map[rel], msize, buffer, t);
   }

   return(0);
}


int alog_append_ibp_split_allocate(int tid, int rindex, osd_id_t mid, uint64_t max_size, int atype, int rel, apr_time_t expiration)
{
   void *d;
   int nbytes, cmd;
   _alog_alloc64_t a64;
   _alog_alloc32_t a32;

   alog_mode_check();

   if (rindex == -1) rindex = 255;

   alog_lock();
   alog_checksize();
     
   if (max_size < UINT32_MAX) {
      nbytes = sizeof(a32);
      cmd = ALOG_REC_IBP_SPLIT_ALLOCATE32;
      d = (void *)&a32;
      a32.ri=rindex; a32.atype=atype; a32.rel=rel; a32.expiration=expiration; a32.size=max_size;
   } else {
      nbytes = sizeof(a64);
      cmd = ALOG_REC_IBP_SPLIT_ALLOCATE64;
      d = (void *)&a64;
      a64.ri=rindex; a64.atype=atype; a64.rel=rel; a64.expiration=expiration; a64.size=max_size;
   } 

   _alog->append_header(_alog->fd, tid, cmd);
   awrite_ul(_alog->fd, &mid, sizeof(mid), "alog_append_ibp_split_allocate: Error with write!\n");
   awrite_ul(_alog->fd, d, nbytes, "alog_append_ibp_split_allocate: Error with write!\n");
 
   alog_unlock();
   return(0);
}

//--------------------------------------------------------------------------

int alog_read_ibp_split_allocate(activity_log_t *alog, int cmd, FILE *outfd)
{
   _alog_alloc64_t a64;
   _alog_alloc32_t a32;
   int ri, atype, rel;
   osd_id_t mid;
   apr_time_t t;
   uint64_t msize;
   char buffer[256];
   
   aread(alog->fd, &mid, sizeof(mid), "alog_read_ibp_split_allocate: Error with read!\n");
   
   if (cmd == ALOG_REC_IBP_SPLIT_ALLOCATE32) {
     aread(alog->fd, &a32, sizeof(a32), "alog_read_ibp_split_allocate: Error with read!\n");
     ri=a32.ri; atype=a32.atype; rel=a32.rel; t=a32.expiration; msize=a32.size;
   } else {
     aread(alog->fd, &a64, sizeof(a32), "alog_read_ibp_split_allocate: Error with read!\n");
     ri=a64.ri; atype=a64.atype; rel=a64.rel; t=a64.expiration; msize=a64.size;
   }    

   if (outfd != NULL) {
      if (ri == 255) ri = alog->nres;
      apr_ctime(buffer, ibp2apr_time(t));
      buffer[strlen(buffer)-1] = '\0';  //** chomp the \n from ctime_r
      fprintf(outfd, "CMD:IBP_SPLIT_ALLOCATE MID:" LU " RID:%s type:%s rel:%s size:" LU " expiration:%s(" TT ")\n", 
         mid, alog->rl_map[ri].name, _ibp_type_map[atype], _ibp_rel_map[rel], msize, buffer, t);
   }

   return(0);
}

//************************************************************************
// alog_append_cmd_result - Used by send_cmd_result to store in alog
//************************************************************************

int alog_append_cmd_result(int tid, int status)
{
   uint8_t n8;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   _alog->append_header(_alog->fd, tid, ALOG_REC_CMD_RESULT);

   n8 = -status;
   if (status > 0) n8 = 0; // ** IBP_OK = 1 but there is no 0 error
   awrite_ul(_alog->fd, &n8, sizeof(n8), "alog_append_cmd_result: Error with write!\n");
   
   alog_unlock();

   return(0);
}

//--------------------------------------------------------------------------

int alog_read_cmd_result(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint8_t n8;
   int status;

   aread(alog->fd, &n8, sizeof(n8), "alog_read_cmd_result: Error with read!\n");
   status = -n8;
   if (outfd != NULL) {
      if (n8 == 0) status = IBP_OK;
      fprintf(outfd, "RESULT: %s(%d) -------------------\n", _ibp_error_map[n8], status);
   }

   return(0);
}

//************************************************************************
//  alog_append_thread_open - Stores the new thread info 
//************************************************************************

int _alog_append_thread_open(int tid, int ns_id, int family, char *address)
{
   uint32_t n32;
   uint8_t n8;
   uint16_t n16;
   ns_map_t *nsmap;

   alog_mode_check();

   //** Add the mapping.
   nsmap = &(_alog->ns_map[tid]);
   nsmap->id = ns_id;  
   nsmap->family = family; 
   nsmap->used = 1;
   memcpy(nsmap->address, address, sizeof(nsmap->address));
 
   _alog->append_header(_alog->fd, tid, ALOG_REC_THREAD_OPEN);

   n16 = tid; awrite_ul(_alog->fd, &n16, sizeof(n16), "alog_append_thread_open: Error with write!\n");
   n32 = ns_id; awrite_ul(_alog->fd, &n32, sizeof(n32), "alog_append_thread_open: Error with write!\n");
   
   n8 = 0; n16 = 4; if (family != AF_INET) { n8 = 1; n16 = 16; }
   awrite_ul(_alog->fd, &n8, sizeof(n8), "alog_append_thread_open: Error with write!\n");
   awrite_ul(_alog->fd, address, n16, "alog_append_thread_open: Error with write!\n");

   return(0);   
}

int alog_append_thread_open(int tid, int ns_id, int family, char *address)
{
   int n;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   n = _alog_append_thread_open(tid, ns_id, family, address);

   alog_unlock();
   return(n);
}

//--------------------------------------------------------------------------

int alog_read_thread_open(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint32_t n32;
   uint8_t n8;
   uint16_t n16;
   int tid, ns_id, family;
   char rawaddress[16], address[128];
   ns_map_t *nsmap;

   aread(alog->fd, &n16, sizeof(n16), "alog_read_thread_open: Error with read!\n"); tid = n16;
   aread(alog->fd, &n32, sizeof(n32), "alog_read_thread_open: Error with read!\n"); ns_id = n32;
   aread(alog->fd, &n8, sizeof(n8), "alog_read_thread_open: Error with read!\n"); family = AF_INET;
   n16 = 4;
   if ( n8 != 0 ) { family = AF_INET6; n16 = 16; }
   aread(alog->fd, rawaddress, n16, "alog_read_thread_open: Error with read!\n");

   if (tid >= alog->max_id) {
     log_printf(0, "alog_read_thread_open: Invalid TID!!! tid=%d max_tid=%d\n", tid, alog->max_id);
     fprintf(outfd, "CMD:THREAD_OPEN ERROR INVALID id:%d max:%d\n", tid, alog->max_id);
     return(1);
   }

   nsmap = &(alog->ns_map[tid]);
   nsmap->id = ns_id;
   nsmap->family = family;
   nsmap->used = 1;
   memcpy(nsmap->address, address, sizeof(nsmap->address));

   if (outfd != NULL) {
      address2ipdecstr(address, rawaddress, family);
      fprintf(outfd, "CMD:THREAD_OPEN id:%d ns:%d address:%s\n", tid, ns_id, address);
   }

   return(0);
}

//************************************************************************
//  alog_append_thread_close - Stores the close thread info 
//************************************************************************

int alog_append_thread_close(int tid, int ncmds)
{
   uint32_t n = ncmds;

   alog_mode_check();

   alog_lock();
   alog_checksize();

   _alog->ns_map[tid].used = 0;

   _alog->append_header(_alog->fd, tid, ALOG_REC_THREAD_CLOSE);

   awrite_ul(_alog->fd, &n, sizeof(n), "alog_append_thread_close: Error writing rec\n");

   alog_unlock();

   return(0);      
}

//--------------------------------------------------------------------------

int alog_read_thread_close(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint32_t n;
   int temp;

   aread(alog->fd, &n, sizeof(n), "activity_log_read_close_rec: Error getting ncommands");

   if (outfd != NULL) {
      temp = n;
      fprintf(outfd, "CMD:THREAD_CLOSE  commands: %d -----------------------------------------\n", temp);
   }
   
   return(0);
}

//************************************************************************
// _alog_config - Adds initial ibp records to the alog file
//************************************************************************

int _alog_config()
{
   long start_pos;
   long end_pos;
   char buffer[100*1024];
   int used = 0;
   uint32_t nbytes = 0;

   alog_mode_check();

//   alog_checksize();

   _alog->append_header(_alog->fd, 0, ALOG_REC_IBP_CONFIG);

   start_pos = ftell(_alog->fd);  //** Keep track of the starting position

   //** Preserve the space for the config length
   awrite_ul(_alog->fd, &nbytes, sizeof(nbytes), "_alog_config: Error storing placeholder!\n");
   
   //*** Pring the config **
   print_config(buffer, &used, sizeof(buffer), global_config);
   fprintf(_alog->fd, "%s", buffer);

   end_pos = ftell(_alog->fd);    //*** Keep track of my final position
   fseek(_alog->fd, start_pos, SEEK_SET);  //** Move back to the length field
   nbytes = end_pos - (start_pos + sizeof(nbytes));   //** and write it 
   awrite_ul(_alog->fd, &nbytes, sizeof(nbytes), "_alog_config: Error storing config size!\n");
   fseek(_alog->fd, end_pos, SEEK_SET);    //** Move to the end of the record

   return(0);
}

//------------------------------------------------------------------------

int alog_read_ibp_config_rec(activity_log_t *alog, int cmd, FILE *outfd)
{
   char buffer[2048];
   int i, j, bufsize;
   uint32_t nbytes;

   aread(alog->fd, &nbytes, sizeof(nbytes), "alog_read_ibp_config_rec:  Error reading size!\n");

   if (outfd != NULL) {
      fprintf(outfd, "CMD:IBP_CONFIG -------------------------------\n");
      bufsize = sizeof(buffer) - 1;
      for (i=0; i<(int)nbytes; i = i + bufsize) {
         j = nbytes - i;
         if (j > bufsize) j = bufsize;
         aread(alog->fd, buffer, j,"alog_read_ibp_config_rec:  Short read!\n");
         fwrite(buffer, j, 1, outfd);
      }
   } else {
      fseek(alog->fd, nbytes, SEEK_CUR);
   }

   return(0);
}

//************************************************************************
//  _alog_resources - Stores the resource list in the activity log
//************************************************************************

int _alog_resources()
{
   uint8_t n8;
   uint16_t n16;
   int i;
   resource_list_iterator_t it;
   Resource_t *r;

   alog_mode_check();
   
//   alog_checksize();

   _alog->append_header(_alog->fd, 0, ALOG_REC_RESOURCE_LIST);

   n16 = resource_list_n_used(global_config->rl);
   awrite_ul(_alog->fd, &n16, sizeof(n16), "_alog_resources: Error storing data!\n");
i=n16;
log_printf(0, "alog_read_res: nres=%d\n", i);

   i = 0;
   it = resource_list_iterator(global_config->rl);
   while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
log_printf(0, "alog_read_res: i=%d rl_index=%d\n", i, r->rl_index);
      n16 = r->rl_index;
      awrite_ul(_alog->fd, &n16, sizeof(n16), "_alog_resources: Error storing data!\n");
      n8 = strlen(r->name);
      awrite_ul(_alog->fd, &n8, sizeof(n8), "_alog_resources: Error storing data!\n");
      awrite_ul(_alog->fd, r->name, n8, "_alog_resources: Error storing data!\n");           
      i++;
   }
   resource_list_iterator_destroy(global_config->rl, &it);
   return(0);
}

//------------------------------------------------------------------------

int alog_read_resource_list(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint8_t n8;
   uint16_t n16;
   int i, nres, index;
   char name[512];

   if (alog->rl_map != NULL) free(alog->rl_map);

   aread(alog->fd, &n16, sizeof(n16), "alog_read_resource_list: Error reading data!\n");
   nres = n16;
   if (outfd != NULL) {
      fprintf(outfd, "CMD:RESOURCE_LIST nres: %d -------------------------------\n", nres);
   }

   alog->rl_map = (rl_map_t *)malloc(sizeof(rl_map_t)*(nres+1));
   assert(alog->rl_map != NULL);
   alog->nres = nres;
   strcpy(alog->rl_map[nres].name, "BAD");

   for (i=0; i < nres; i++) {
      aread(alog->fd, &n16, sizeof(n16), "alog_read_resource_list: Error reading data!\n");
      index = n16;

      aread(alog->fd, &n8, sizeof(n8), "alog_read_resource_list: Error reading data!\n");     
      aread(alog->fd, name, n8, "alog_read_resource_list: Error reading data!\n");           
      name[n8] = '\0';

      strcpy(alog->rl_map[index].name, name);

      if (outfd != NULL) {
         fprintf(outfd, "%d - %s\n", index, name);
      }
   }

   return(0);
}

//************************************************************************
// _alog_init_constants
//************************************************************************

void _alog_init_constants()
{
   memset(_ibp_error_map, 0, sizeof(_ibp_error_map));
   memset(_ibp_subcmd_map, 0, sizeof(_ibp_subcmd_map));
   memset(_ibp_st_map, 0, sizeof(_ibp_st_map));
   memset(_ibp_type_map, 0, sizeof(_ibp_type_map));
   memset(_ibp_ctype_map, 0, sizeof(_ibp_ctype_map));
   memset(_ibp_rel_map, 0, sizeof(_ibp_rel_map));
   memset(_ibp_captype_map, 0, sizeof(_ibp_captype_map));
   memset(_ibp_cmd_map, 0, sizeof(_ibp_cmd_map));
   memset(_ibp_wmode_map, 0, sizeof(_ibp_wmode_map));


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

   _ibp_rel_map[ALLOC_SOFT] = "IBP_SOFT";
   _ibp_rel_map[ALLOC_HARD] = "IBP_HARD";

   _ibp_type_map[IBP_BYTEARRAY] = "IBP_BYTEARRAY";
   _ibp_type_map[IBP_BUFFER] = "IBP_BUFFER";
   _ibp_type_map[IBP_FIFO] = "IBP_FIFO";
   _ibp_type_map[IBP_CIRQ] = "IBP_CIRQ";

   _ibp_ctype_map[IBP_TCP] = "IBP_TCP";
   _ibp_ctype_map[IBP_PHOEBUS] = "IBP_PHOEBUS";

   _ibp_captype_map[READ_CAP] = "IBP_READCAP";
   _ibp_captype_map[WRITE_CAP] = "IBP_WRITECAP";
   _ibp_captype_map[MANAGE_CAP] = "IBP_MANAGECAP";

   _ibp_cmd_map[IBP_PUSH] = "IBP_PUSH";
   _ibp_cmd_map[IBP_PULL] = "IBP_PULL";
   _ibp_cmd_map[IBP_SEND] = "IBP_SEND";
   _ibp_cmd_map[IBP_PHOEBUS_SEND] = "IBP_PHOEBUS_SEND";

   _ibp_wmode_map[0] = "USE_OFFSET";
   _ibp_wmode_map[1] = "APPEND";
}

//************************************************************************
// alog_open - Opens the alog file for use
//************************************************************************

void alog_open()
{
   if (_alog_lock == NULL) alog_init();
   alog_lock();  

   _alog_init_constants();

   _alog_name = global_config->server.alog_name;
   _alog_max_size = global_config->server.alog_max_size;

   if (_alog_max_size <= 0) return;

   _alog = activity_log_open(_alog_name, 2*global_config->server.max_threads, ALOG_APPEND);

   assert(_alog != NULL);

   _alog_config();
   _alog_resources();

   alog_unlock();  
}

//************************************************************************
// alog_close - closes the alog file
//************************************************************************

void alog_close()
{
   if (_alog_max_size <= 0) return;

   alog_lock();  

   activity_log_close(_alog);

   alog_unlock();  
}

//************************************************************************
// _alog_transfer_data - Sends data back
//************************************************************************

int _alog_transfer_data(char *fname)
{
  int err = 0;
  int n, nbytes;
  activity_log_t alog;
  const int bufsize = 1024*1024;
  char buffer[bufsize];
  envelope_t env;
  tbx_ns_t *ns;
  uint32_t response;
  ibp_off_t pos, bpos, nleft, bsize, messagesize;
  tbx_ns_timeout_t dt;

  tbx_ns_timeout_set(&dt, 10, 0);

  //** Make the connection
  ns = tbx_ns_new();
  tbx_ns_sock_config(ns, 0);
  err = tbx_ns_connect(ns, global_config->server.alog_host, global_config->server.alog_port, dt);
  if (err != 0) {
     log_printf(0, "_alog_transfer_Data: Can't connect to %s:%d!\n", 
           global_config->server.alog_host, global_config->server.alog_port);
     return(1);
  }

  tbx_ns_timeout_set(&dt, 1, 0);

  //** Get the info for the command
  alog.fd = fopen(fname, "r");
  if (alog.fd == NULL) {
     log_printf(0, "_alog_transfer_Data: Can't open %s for READ!\n", fname);
     tbx_ns_destroy(ns);
     return(2);
  }

  read_file_header(&alog);
  if (alog.header.state == STATE_BAD) {
     log_printf(0, "_alog_transfer_data: alog %s was not closed properly!\n", fname);
  }

  //** Prep and pack the message for sending
  //** Add the message header reserving space for the envelope
  fseeko(alog.fd, 0, SEEK_END);
  messagesize = ftell(alog.fd);
  n = sprintf(&(buffer[ENVELOPE_SIZE]), "%s "  LU " " TT " " TT " " TT "\n", global_config->server.iface[0].hostname,
      messagesize,  alog.header.start_time, alog.header.end_time, apr_time_now());

  //** Encode the header and place it at the begininning
  envelope_set(&env, ECMD_ALOG_SEND, n);
  envelope_encode(&env, (unsigned char *)buffer);
  n += ENVELOPE_SIZE;

  //** send the command
  err = server_ns_write_block(ns, apr_time_now()+apr_time_make(30, 0), buffer, n);
  if (err != NS_OK) {
     fclose(alog.fd);
     log_printf(0, "_alog_transfer_Data: server_ns_write_block failed for sending %s header! err=%d n=%d\n", fname, err, n);
     tbx_ns_destroy(ns);
     return(5);
  }

  //** Wait for the response
  err = envelope_simple_recv(ns, &response);
  if (err != NS_OK) {
     fclose(alog.fd);
     log_printf(0, "_alog_transfer_Data: envelope_simple_recv failed reading response header! err=%d n=%d\n", err, n);
     tbx_ns_destroy(ns);
     return(6);
  }
  if (response != 1) {
     fclose(alog.fd);
     err = response;
     log_printf(0, "_alog_transfer_Data: alog_server returned an error! response=%d\n", err);
     tbx_ns_destroy(ns);
     return(6);
  }

  //** Transfer the data
  nbytes = -100;
  err = 0;
  fseeko(alog.fd, 0, SEEK_SET);
  for (pos = 0; (pos < messagesize) && (err == 0); pos = pos + bufsize) {
     bsize = bufsize;
     if ((pos+bsize) > messagesize) bsize = messagesize - pos;
     nbytes = fread(buffer, bsize, 1, alog.fd);  //** Read the next block

     bpos = 0; nleft = bsize;
     while ((nleft > 0) && (err == 0)) {
        log_printf(15, "_alog_transfer_data: ns=%d pos=" I64T " bpos=" I64T "\n", tbx_ns_getid(ns), pos, bpos);
        nbytes = server_ns_write(ns, &(buffer[bpos]), nleft, dt);

        if (nbytes < 0) {
           err = nbytes;   //** Error with write
        } else if (nbytes > 0) {   //** Normal write
           bpos = bpos + nbytes;
           nleft = bsize - bpos;
        }

     }
  }

  fclose(alog.fd);

  if (err != 0) {
     log_printf(0, "_alog_transfer_Data: failed sending file! err=%d n=%d\n", err, tbx_ns_getid(ns));
     tbx_ns_destroy(ns);
     return(6);
  }

  //** Wait for the response
  err = envelope_simple_recv(ns, &response);
  if (err != NS_OK) {
     log_printf(0, "_alog_transfer_Data: envelope_simple_recv failed reading response after send! err=%d n=%d\n", err, n);
     tbx_ns_destroy(ns);
     return(6);
  }
  if (response != 1) {
     err = response;
     log_printf(0, "_alog_transfer_Data: alog_server returned an error with data! response=%d\n", err);
     tbx_ns_destroy(ns);
     return(6);
  }

  //** Close the connection
  tbx_ns_destroy(ns);

  return(err);
}

//************************************************************************
// _send_alog_thread - Performs the actual sending ot the data in a separate thread
//************************************************************************

void *_send_alog_thread(apr_thread_t *th, void *arg)
{
  char *fname;
  int err;

  apr_thread_mutex_lock(_alog_send_lock);

  //** Check if we have a valid host/port.  If not dump the stack and exit  
  if ((global_config->server.alog_port <= 0) && (global_config->server.alog_host == NULL)) {
     log_printf(10, "_send_alog_thread: Invalid host/port.  Skipping transfer\n");
     tbx_stack_empty(_alog_pending_stack, 1);
  }

  while (tbx_stack_count(_alog_pending_stack) > 0) {
      tbx_stack_move_to_bottom(_alog_pending_stack);
      fname = (char *)tbx_stack_get_current_data(_alog_pending_stack);
      apr_thread_mutex_unlock(_alog_send_lock);

     log_printf(10, "_send_alog_thread: fname=%s\n", fname);

      err = 0;
      if (fname != NULL) err = _alog_transfer_data(fname);

      log_printf(10, "_send_alog_thread: err=%d fname=%s\n", err, fname);

      apr_thread_mutex_lock(_alog_send_lock);
      if (err == 0) {    //** Only Delete it if there were no errors
         log_printf(10, "_send_alog_thread: removing fname=%s\n", fname);
         remove(fname);
      }

      tbx_stack_move_to_bottom(_alog_pending_stack);   //** Always tbx_stack_pop it off the list even if we fail
      tbx_stack_delete_current(_alog_pending_stack, 1, 1);
  }

  //** Unlock before returning
  apr_thread_mutex_unlock(_alog_send_lock);

  return(NULL);
}

//************************************************************************
// _alog_nsmap - Exports the ns_map to the alog file
//************************************************************************

void _alog_nsmap(ns_map_t *nsmap, int n)
{
  int i;

  for (i=0; i<n; i++) {
     if (nsmap[i].used == 1) _alog_append_thread_open(i, nsmap[i].id, nsmap[i].family, nsmap[i].address);
  }
}


//************************************************************************
//  _alog_send_data - Sends the statsitics logs back to a central location
//************************************************************************

void _alog_send_data()
{
  char fname[1024];
  ns_map_t *nsmap;
  int new_transfer = 0;

  log_printf(15, "_alog_send_data: Start.... Need to send data home\n");

//  alog_lock();

  //** Preserve these pointers for the open;
  nsmap = _alog->ns_map; _alog->ns_map = NULL;
    
  activity_log_close(_alog);  //** Close the old activity file

  //** Check if we have too much data pending.  If so drop this data
  apr_thread_mutex_lock(_alog_send_lock);  

  //** Check if this is the 1st time. If so prep the stack
  if (_alog_pending_stack == NULL) _alog_pending_stack = tbx_stack_new();

  //** No active threads so fire up a new one
  if (tbx_stack_count(_alog_pending_stack) == 0) {
     glob_t globbuf;
     char path[4096];
     char *file = NULL;
     int i, len, n;

     new_transfer = 1;
     sprintf(path, "%s.*.[0-9][0-9][0-9][0-9][0-9][0-9]", global_config->server.alog_name);
     i=glob(path, 0, NULL, &globbuf);  //** Get the file list
     log_printf(15, "_alog_send_data: glob(%s, 0, NULL, &globbuf)=%d\n", path, i);

     //** Now iterate through them tbx_stack_pushing them on the stack and determine _alog_count
     _alog_count = 0;
     for (i=0; i<(int)globbuf.gl_pathc; i++) {
        file = globbuf.gl_pathv[i];
        log_printf(15, "_alog_send_data: new transfer.  tbx_stack_pushing %s onto send stack\n", file);
        tbx_stack_push(_alog_pending_stack, strdup(file));

        len = strlen(file);
        sscanf(&(file[len-6]), "%d", &n);
        if (n > _alog_count) _alog_count = n;
     }

     //** Too much data pending so drop the oldest slots 
     while (tbx_stack_count(_alog_pending_stack) >= global_config->server.alog_max_history) {
        tbx_stack_move_to_bottom(_alog_pending_stack);
        file = (char *)tbx_stack_get_current_data(_alog_pending_stack);
        remove(file);
        tbx_stack_delete_current(_alog_pending_stack, 0, 1);
     }
  }

  log_printf(15, "_alog_send_data: tbx_stack_count=%d max_history=%d\n", tbx_stack_count(_alog_pending_stack), global_config->server.alog_max_history);

  //** Transfer already in progress and too much data so drop current
  if (tbx_stack_count(_alog_pending_stack) >= global_config->server.alog_max_history) {  
     remove(_alog_name);
  } else {   //** Now Trigger the transfer
     //** Rename it **
     _alog_count++;  if (_alog_count > 999999) _alog_count = 0;  
     fname[1023] = '\0';
     snprintf(fname, 1023, "%s." TT ".%06d", _alog_name, apr_time_now(), _alog_count);
     rename(_alog_name, fname);

     tbx_stack_push(_alog_pending_stack, strdup(fname));

     //*** Spawn the thread to perform the send if needed ***
     if (new_transfer == 1) {
        if (_alog_send_thread != NULL) {
          apr_pool_destroy(_alog_send_mpool);  //** Only way to free the _alog_send_thread memory:(
        } 
        assert(apr_pool_create(&_alog_send_mpool, NULL) == APR_SUCCESS);
        apr_thread_create(&_alog_send_thread, NULL, _send_alog_thread, NULL, _alog_send_mpool);
     }
  }
  apr_thread_mutex_unlock(_alog_send_lock);  

  //** Now open the fresh log file
  _alog = activity_log_open(_alog_name, 2*global_config->server.max_threads, ALOG_APPEND);

  assert(_alog != NULL);

  _alog_config();
  _alog_resources();
  _alog_nsmap(nsmap, 2*global_config->server.max_threads);
  free(nsmap);

//  alog_unlock();
}


//************************************************************************
//------------ Routines below are generic activity_log routines ----------
//************************************************************************

//************************************************************************
//  activity_log_read_next_entry - Reads and optionally prints the next record
//************************************************************************

int activity_log_read_next_entry(activity_log_t *alog, FILE *fd)
{
  apr_time_t t;
  int id, command, n;

//int d1 = ftell(alog->fd);
//log_printf(0, "next_entry start fpos=%d\n", d);
  n = alog->read_header(alog->fd, &t, &id, &command);
//int d2 = ftell(alog->fd);
//log_printf(0, "next_entry: header block: (%d,%d) * cmd=%d %d\n", d1, d2, command, errno);

  if (n != 0) return(-1);

  if ((alog->ns_map != NULL) && (id < alog->max_id)) id = alog->ns_map[id].id;

  if ((command == ALOG_REC_OPEN) || (command == ALOG_REC_CLOSE)) id = 0;  //** Open commands *determine* the header size so this could be bogus

  alog->print_header(fd, t, id);
  if (alog->table[command].process_entry != NULL) {
     n = alog->table[command].process_entry(alog, command, fd);
  } else {
     log_printf(0, "activity_log_read_next_entry: Invalid command: %d\n", command);
     if (fd != NULL) fprintf(fd, "Invalid header! t=" TT " * id=%d command=%d!!!!!!!!!!!!!!!!\n",t, id, command); 
     return(-2);
  }

  return(n);
}

//************************************************************************
//  activity_log_open_rec - Writes an open alog record
//************************************************************************

int activity_log_open_rec(activity_log_t *alog)
{
   uint32_t n = alog->max_id;

   //** For the open record we always write it using the 1 byte routine **
   activity_log_append_header_1byte_id(alog->fd, 0, ALOG_REC_OPEN);
   awrite(alog->fd, &n, sizeof(n), "activity_log_open_rec: Error writng max_id\n");

   //** Create the space for the network maps
   if (alog->ns_map != NULL) free(alog->ns_map);
   alog->ns_map = (ns_map_t *)malloc(sizeof(ns_map_t)*alog->max_id);
   assert(alog->ns_map != NULL);
   memset(alog->ns_map, 0, sizeof(ns_map_t)*alog->max_id);

   return(0);
}

//------------------------------------------------------------------------

int activity_log_read_open_rec(activity_log_t *alog, int cmd, FILE *outfd)
{
   uint32_t max_id;

//log_printf(0, "alog_read_open_rec: start\n"); tbx_log_flush();

   if (alog->max_id > 255) {   //** open rec is always done with 1byte header
      apr_time_t t;
      int id, command;
      fseek(alog->fd, -sizeof(alog_header2_t), SEEK_CUR);   //** Rewind to the beginning of the record
      activity_log_read_header_1byte_id(alog->fd, &t, &id, &command);  //** Reread the header properly
   }

   //** At this point we can read the max_id and set the fn table properly
   aread(alog->fd, &max_id, sizeof(max_id), "activity_log_read_open_rec: Error getting max_id");

   alog->max_id = max_id;
   if (max_id < 256) {
      alog->append_header = activity_log_append_header_1byte_id;
      alog->read_header = activity_log_read_header_1byte_id;
   } else {
      alog->append_header = activity_log_append_header_2byte_id;
      alog->read_header = activity_log_read_header_2byte_id;
   }


   if (alog->ns_map != NULL) free(alog->ns_map);
   alog->ns_map = (ns_map_t *)malloc(sizeof(ns_map_t)*alog->max_id);
   assert(alog->ns_map != NULL);
   memset(alog->ns_map, 0, sizeof(ns_map_t)*alog->max_id);

   if (outfd != NULL) {
      fprintf(outfd, "CMD:OPEN max_id=%d -------------------------------\n", alog->max_id);
   }

   return(0);
}

//************************************************************************
//  activity_log_close_rec - Writes an open alog record
//************************************************************************

void activity_log_close_rec(activity_log_t *alog)
{
   alog->append_header(alog->fd, 0, ALOG_REC_CLOSE);
}

//------------------------------------------------------------------------

int activity_log_read_close_rec(activity_log_t *alog, int cmd, FILE *outfd)
{
   if (outfd != NULL) {
      fprintf(outfd, "CMD:CLOSE -----------------------------------------\n");
   }

   return(0);
}

//************************************************************************
//  activity_log_append_header - Writes an alog header to disk
//************************************************************************

int activity_log_append_header_1byte_id(FILE *fd, int id, int command)
{
   alog_header1_t h;

   h.thread_id = id;
   h.time = ibp_time_now();
   h.command = command;

//log_printf(0, "1byte: sizeof(1byte_id)=%lu sizeof(2byte_id)=%lu\n", sizeof(h), sizeof(alog_header2_t));

   awrite(fd, &h, sizeof(h), "activity_log_append_header_1byte_di: Error wriing header\n");

   return(0);
}

int activity_log_append_header_2byte_id(FILE *fd, int id, int command)
{
   alog_header2_t h;

   h.thread_id = id;
   h.time = ibp_time_now();
   h.command = command;

//log_printf(0, "2byte: sizeof(1byte_id)=%lu sizeof(2byte_id)=%lu\n", sizeof(h), sizeof(alog_header1_t));

   awrite(fd, &h, sizeof(h), "activity_log_append_header_1byte_di: Error wriing header\n");

   return(0);
}

//************************************************************************
//  activity_log_read_header - Writes an alog header to disk
//************************************************************************

int activity_log_read_header_1byte_id(FILE *fd, apr_time_t *t, int *id, int *command)
{
   alog_header1_t h;

   aread(fd, &h, sizeof(h), "read_1byte_id error\n");

   *t = h.time;
   *id = h.thread_id;
   *command = h.command;

   return(0);
}

int activity_log_read_header_2byte_id(FILE *fd, apr_time_t *t, int *id, int *command)
{
   alog_header2_t h;

   aread(fd, &h, sizeof(h), "read_2byte_id error\n");

   *t = h.time;
   *id = h.thread_id;
   *command = h.command;

   return(0);
}

//************************************************************************
// print_header_date_utime_id - Header variation 
//  "date(utime) id:xx :"
//************************************************************************

void print_header_date_utime_id(FILE *fd, apr_time_t t, int id)
{
   char buffer[256];

   if (fd == NULL) return;

   apr_ctime(buffer, ibp2apr_time(t));
   buffer[strlen(buffer)-1] = '\0';  //** chomp the \n from ctime_r
   fprintf(fd, "%s(" TT ") ns: %d : ", buffer, t, id);
}

//************************************************************************
// print_header_date_id - Header variation 
//  "date id:xx :"
//************************************************************************

void print_header_date_id(FILE *fd, apr_time_t t, int id)
{
   char buffer[256];

   if (fd == NULL) return;

   apr_ctime(buffer, ibp2apr_time(t));
   buffer[strlen(buffer)-1] = '\0';  //** chomp the \n from ctime_r
   fprintf(fd, "%s ns: %d : ", buffer, id);
}

//************************************************************************
// print_header_utime_id - Header variation 
//  "utime id:xx :"
//************************************************************************

void print_header_utime_id(FILE *fd, apr_time_t t, int id)
{
   if (fd == NULL) return;

   fprintf(fd, TT " ns: %d : ", t, id);
}


//************************************************************************
//  activity_log_move_to_eof - Moves to the end of the log file
//************************************************************************

void activity_log_move_to_eof(activity_log_t *alog)
{
   size_t pos;

   do {   //** find the last valid record
     pos = ftell(alog->fd);
   } while (activity_log_read_next_entry(alog, NULL) == 0);

   //** Move back to the end of the last good record
   pos--;
   fseek(alog->fd, pos, SEEK_SET);

   //** Truncate the file here
   pos++;   //** This is the number of good "bytes"
   assert(ftruncate(fileno(alog->fd), pos) == 0);

   //** Now move to where the next byte should go
   fseek(alog->fd, pos, SEEK_SET);
}

//************************************************************************
// write_file_header - Writes the alog files initial header
//************************************************************************

int write_file_header(activity_log_t *alog)
{
   uint64_t n;
   alog_file_header_t *h = &(alog->header);

   fseek(alog->fd, 0, SEEK_SET);

   h->version = ALOG_VERSION;
   h->start_time = time(0);
   h->end_time = 0;
   h->state = STATE_BAD;

   n = h->version; awrite(alog->fd, &n, sizeof(n), "write_file_header: Error storing version\n");  //** Version
   n = h->start_time; awrite(alog->fd, &n, sizeof(n), "write_file_header: Error storing start_time\n");    //** Start time
   n = h->end_time;   awrite(alog->fd, &n, sizeof(n), "write_file_header: Error storing end time\n");    //** End time
   n = h->state; awrite(alog->fd, &n, sizeof(n),"write_file_header: Error storing state\n");    //** Mark the state as in use

   return(0);
}

//************************************************************************
// get_alog_header - Returns the alog file header.
//************************************************************************

alog_file_header_t get_alog_header(activity_log_t *alog)
{
  return(alog->header);
}

//************************************************************************
// read_file_header - Reads the alog file header.
//************************************************************************

int read_file_header(activity_log_t *alog)
{
   uint64_t n;
   alog_file_header_t *h = &(alog->header);

   fseek(alog->fd, 0, SEEK_SET);

   aread(alog->fd, &n, sizeof(n), "read_file_header: error reading version\n");  h->version = n;    //** Version
   aread(alog->fd, &n, sizeof(n), "read_file_header: error reading start_time\n");  h->start_time = n; //** Start time
   aread(alog->fd, &n, sizeof(n), "read_file_header: error reading end time\n");  h->end_time = n;   //** End time
   aread(alog->fd, &n, sizeof(n), "read_file_header: error reading state\n");  h->state = n;      //** State

   return(0);
}

//************************************************************************
// update_file_header - Updates the alog files initial header
//************************************************************************

int update_file_header(activity_log_t *alog, int state)
{
   uint64_t n;

   alog->header.state = state;

   if (state == STATE_BAD) {
      fseek(alog->fd, 3*sizeof(n), SEEK_SET);

      n = STATE_BAD; awrite(alog->fd, &n, sizeof(n), "update_file_header: Error setting STATE_BAD\n");    //** Mark the state as in use
   } else {
      fseek(alog->fd, 2*sizeof(n), SEEK_SET);

      alog->header.end_time = time(0);
      n = alog->header.end_time; awrite(alog->fd, &n, sizeof(n), "update_file_header: Error setting end time\n");    //** End time
      n = STATE_GOOD; awrite(alog->fd, &n, sizeof(n), "update_file_header: Error setting STATE_GOOD\n");    //** Mark the state as in use
   }

   return(0);
}

//************************************************************************
// activity_log_add_commands - Adds the commands to the alog handle
//************************************************************************

void activity_log_add_commands(activity_log_t *alog)
{
  memset(alog->table, 0, sizeof(alog_entry_t)*256);

  alog->table[ALOG_REC_INT_GET_CONFIG].process_entry =  alog_read_internal_get_config;
  alog->table[ALOG_REC_IBP_MERGE].process_entry =  alog_read_ibp_merge;
  alog->table[ALOG_REC_INT_EXPIRE_LIST].process_entry =  alog_read_internal_expire_list;
  alog->table[ALOG_REC_INT_DATE_FREE].process_entry =  alog_read_internal_date_free;
  alog->table[ALOG_REC_ALIAS_COPY64].process_entry =  alog_read_alias_copy64;
  alog->table[ALOG_REC_ALIAS_COPY32].process_entry =  alog_read_alias_copy32;
  alog->table[ALOG_REC_COPY64].process_entry =  alog_read_copy64;
  alog->table[ALOG_REC_COPY32].process_entry =  alog_read_copy32;
  alog->table[ALOG_REC_ALIAS_READ64].process_entry =  alog_read_alias_read64;
  alog->table[ALOG_REC_ALIAS_READ32].process_entry =  alog_read_alias_read32;
  alog->table[ALOG_REC_READ64].process_entry =  alog_read_read64;
  alog->table[ALOG_REC_READ32].process_entry =  alog_read_read32;
  alog->table[ALOG_REC_ALIAS_WRITE_APPEND64].process_entry =  alog_read_alias_write_append64;
  alog->table[ALOG_REC_ALIAS_WRITE_APPEND32].process_entry =  alog_read_alias_write_append32;
  alog->table[ALOG_REC_ALIAS_WRITE64].process_entry =  alog_read_alias_write64;
  alog->table[ALOG_REC_ALIAS_WRITE32].process_entry =  alog_read_alias_write32;
  alog->table[ALOG_REC_WRITE_APPEND64].process_entry =  alog_read_write_append64;
  alog->table[ALOG_REC_WRITE_APPEND32].process_entry =  alog_read_write_append32;
  alog->table[ALOG_REC_WRITE64].process_entry =  alog_read_write64;
  alog->table[ALOG_REC_WRITE32].process_entry =  alog_read_write32;
  alog->table[ALOG_REC_ALIAS_MANAGE_PROBE].process_entry =  alog_read_alias_manage_probe;
  alog->table[ALOG_REC_MANAGE_PROBE].process_entry =  alog_read_manage_probe;
  alog->table[ALOG_REC_MANAGE_CHANGE].process_entry =  alog_read_manage_change;
  alog->table[ALOG_REC_ALIAS_MANAGE_CHANGE].process_entry =  alog_read_alias_manage_change;
  alog->table[ALOG_REC_MANAGE_INCDEC].process_entry =  alog_read_manage_incdec;
  alog->table[ALOG_REC_ALIAS_MANAGE_INCDEC].process_entry =  alog_read_alias_manage_incdec;
  alog->table[ALOG_REC_MANAGE_BAD].process_entry =  alog_read_manage_bad;
  alog->table[ALOG_REC_STATUS_CHANGE].process_entry =  alog_read_status_subcmd;
  alog->table[ALOG_REC_STATUS_INQ].process_entry =  alog_read_status_inq;
  alog->table[ALOG_REC_STATUS_STATS].process_entry =  alog_read_status_stats;
  alog->table[ALOG_REC_STATUS_RES].process_entry =  alog_read_status_subcmd;
  alog->table[ALOG_REC_STATUS_VERSION].process_entry =  alog_read_status_subcmd;
  alog->table[ALOG_REC_ALIAS_ALLOC32].process_entry =  alog_read_alias_alloc32;
  alog->table[ALOG_REC_ALIAS_ALLOC64].process_entry =  alog_read_alias_alloc64;
  alog->table[ALOG_REC_INTERNAL_GET_ALLOC].process_entry =  alog_read_internal_get_alloc;
  alog->table[ALOG_REC_IBP_RENAME].process_entry =  alog_read_ibp_rename;
  alog->table[ALOG_REC_OSD_ID].process_entry =  alog_read_osd_id;
  alog->table[ALOG_REC_IBP_ALLOCATE64].process_entry =  alog_read_ibp_allocate;
  alog->table[ALOG_REC_IBP_ALLOCATE32].process_entry =  alog_read_ibp_allocate;
  alog->table[ALOG_REC_IBP_SPLIT_ALLOCATE64].process_entry =  alog_read_ibp_split_allocate;
  alog->table[ALOG_REC_IBP_SPLIT_ALLOCATE32].process_entry =  alog_read_ibp_split_allocate;
  alog->table[ALOG_REC_CMD_RESULT].process_entry =  alog_read_cmd_result;
  alog->table[ALOG_REC_RESOURCE_LIST].process_entry =  alog_read_resource_list;
  alog->table[ALOG_REC_THREAD_OPEN].process_entry = alog_read_thread_open;
  alog->table[ALOG_REC_THREAD_CLOSE].process_entry = alog_read_thread_close;
  alog->table[ALOG_REC_OPEN].process_entry = activity_log_read_open_rec;
  alog->table[ALOG_REC_CLOSE].process_entry = activity_log_read_close_rec;
  alog->table[ALOG_REC_IBP_CONFIG].process_entry = alog_read_ibp_config_rec;
  alog->table[ALOG_REC_VALIDATE_GET_CHKSUM].process_entry = alog_read_validate_get_chksum;
}


//************************************************************************
// activity_log_open - Opens a stats log.  If it is an existing log 
//     data is appended
//************************************************************************

activity_log_t * activity_log_open(const char *logname, int max_id, int mode)
{
  activity_log_t *alog = (activity_log_t *)malloc(sizeof(activity_log_t));
  assert(alog != NULL);

  alog->table = (alog_entry_t *)malloc(sizeof(alog_entry_t)*256);
  assert(alog->table != NULL);

  alog_init();

  activity_log_add_commands(alog);

  alog->print_header = print_header_date_utime_id;

  alog->nres = 0;
  alog->rl_map = NULL;
  alog->ns_map = NULL;
  alog->mode = mode;
  alog->name = strdup(logname);
  alog->max_id = max_id;
  if (max_id < 256) {
     alog->append_header = activity_log_append_header_1byte_id;
     alog->read_header = activity_log_read_header_1byte_id;
  } else {
     alog->append_header = activity_log_append_header_2byte_id;
     alog->read_header = activity_log_read_header_2byte_id;
  }

  alog->curr_size = 0;
  
  if (mode == ALOG_READ) {
     alog->fd = fopen(alog->name, "r");
     if (alog->fd == NULL) {
        log_printf(0, "activity_log_open: Can't open %s for READ!\n", alog->name);
        return(NULL);
     }

     read_file_header(alog);
     if (alog->header.state == STATE_BAD) {
        log_printf(0, "activity_log_open: alog %s was not closed properly!\n", alog->name);
     }
  } else {
     alog->fd = fopen(alog->name, "r+");
     if (alog->fd == NULL) {
        log_printf(10, "activity_log_t: Doesn't look like %s exists. Creating new file\n", alog->name);

        alog->fd = fopen(alog->name, "w+");
        if (alog->fd == NULL) {
           log_printf(0, "activity_log_t: Can't open %s for READ/WRITE!\n", alog->name);
           return(NULL);
        }
        write_file_header(alog);
     } else {
        read_file_header(alog);
        if (alog->header.state == STATE_BAD) {
           log_printf(0, "activity_log_open: alog %s was not closed properly! Moving to EOF manually.\n", alog->name);
           update_file_header(alog, STATE_BAD);         
           activity_log_move_to_eof(alog);
        } else {
           update_file_header(alog, STATE_BAD);
           fseek(alog->fd, 0, SEEK_END);
        }
     }

     activity_log_open_rec(alog);  //** Write the open record
  }

  _alog_size = ftell(alog->fd);  //** Set the initial size

  return(alog);
}


//************************************************************************
// activity_log_close - Closes a stats log file
//************************************************************************

void activity_log_close(activity_log_t *alog)
{

  if (alog->mode == ALOG_APPEND) {
     activity_log_close_rec(alog);  //** Write the close record
     update_file_header(alog, STATE_GOOD);
  }

  if (alog->ns_map != NULL) free(alog->ns_map);

  fclose(alog->fd);
  free(alog->table);
  free(alog->name);
  free(alog);
}

