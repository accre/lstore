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

#define _log_module_index 133

#include <stdlib.h>
#include <assert.h>
#include "ibp.h"
#include "host_portal.h"
#include "log.h"
#include "opque.h"

ibp_context_t *_ibp_sync = NULL;

//**************************************************************************
//---------------  Unsupported routines are listed below -------------------
//**************************************************************************

#ifndef IBP_REMOVE_UNSUPPORTED

unsigned long int  IBP_mcopy ( IBP_cap pc_SourceCap,
                               IBP_cap pc_TargetCap[],
                               unsigned int pi_CapCnt,
                               IBP_timer ps_src_timeout,
                               IBP_timer ps_tgt_timeout,
                               unsigned long int pl_size,
                               unsigned long int pl_offset,
                               int dm_type[],
                               int dm_port[],
                               int dm_service) 
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;

  return(0);
}

unsigned long int  IBP_datamover (  IBP_cap pc_TargetCap,
                                    IBP_cap pc_ReadCap,
                                    IBP_timer ps_tgt_timeout,
                                    unsigned long int pl_size,
                                    unsigned long int pl_offset,
                                    int dm_type,
                                    int dm_port,
                                    int dm_service)
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
  return(0);
}

int IBP_setAuthenAttribute(char *certFile, char *privateKeyFile , char *passwd)
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
  return(0);
}

int IBP_freeCapSet(IBP_set_of_caps capSet)
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
  return(0);
}

char* DM_Array2String(int numelems, void  *array[], int type)
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
  return(NULL);
}

int IBP_setMaxOpenConn(int max)
{
  IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
  return(0);
}

#endif

//**************************************************************************
// ------------------ Supported routines are given below -------------------
//**************************************************************************


//**************************************************************************
// ibp_sync_command - Executes a command synchronously 
//**************************************************************************

int ibp_sync_command(ibp_op_t *iop) {
 op_generic_t *gop = ibp_get_gop(iop);
  op_status_t status;

 //** Submit if for execution
// gop->base.started_execution = 1;  //** Mark it as being submitted
// submit_hportal_sync(gop->op->pc, gop); 

 gop_set_exec_mode(gop, OP_EXEC_DIRECT);
 gop_start_execution(gop);

 //** and Wait for completion
 gop_waitany(gop);
 status = gop_get_status(gop);
 IBP_errno = status.error_code;

 if (gop->op->cmd.hostport != NULL) {
   free(gop->op->cmd.hostport);
   gop->op->cmd.hostport = NULL;
 }

log_printf(10, "ibp_sync_command: IBP_errno=%d\n", IBP_errno); //flush_log(); 

 return(IBP_errno);
}

//**************************************************************************
// set_ibp_sync_context - Sets the IBP context to use
//**************************************************************************

void set_ibp_sync_context(ibp_context_t *ic)
{
  _ibp_sync = ic;
}

//**************************************************************************
// make_ibp_sync_context
//**************************************************************************

void make_ibp_sync_context()
{
  if (_ibp_sync != NULL) return;

  _ibp_sync = ibp_create_context();
}

//**************************************************************************
// destroy_ibp_sync_context
//**************************************************************************

void destroy_ibp_sync_context()
{
  if (_ibp_sync == NULL) return;

  ibp_destroy_context(_ibp_sync);
}

//**************************************************************************
//  IBP_allocate - Creates an allocation and Returns a set of capabilities
//**************************************************************************

ibp_capset_t *IBP_allocate(ibp_depot_t  *depot, ibp_timer_t *timer, unsigned long int size, ibp_attributes_t *attr)
{
 ibp_op_t op;
 int err;

 make_ibp_sync_context();

 ibp_capset_t *cs = (ibp_capset_t *)malloc(sizeof(ibp_capset_t));
 assert(cs != NULL);

 init_ibp_op(_ibp_sync, &op);
 set_ibp_alloc_op(&op, cs, size, depot, attr, CHKSUM_DEFAULT, 0, timer->ClientTimeout);
 err = ibp_sync_command(&op);
 gop_free(ibp_get_gop(&op), OP_FINALIZE);

 if (err != IBP_OK) {free(cs); cs = NULL; }

 return(cs);
}

//**************************************************************************
//  IBP_write - Stores dataa at the given offset for an IBP allocation
//**************************************************************************

unsigned long int IBP_write(ibp_cap_t *cap, ibp_timer_t  *timer, char *data, 
        unsigned long int size, unsigned long int offset)
{
  ibp_op_t op;
  int err;
  tbuffer_t buf;

  tbuffer_single(&buf, size, data);

  make_ibp_sync_context();
 
  init_ibp_op(_ibp_sync, &op);
  set_ibp_write_op(&op, cap, offset, &buf, 0, size, timer->ClientTimeout);
  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) return(0);
  return(size);
}

//**************************************************************************
//  IBP_store - *Appends* data to an IBP allocation
//**************************************************************************

unsigned long int IBP_store(ibp_cap_t *cap, ibp_timer_t  *timer, char *data, 
        unsigned long int size)
{
  ibp_op_t op;
  int err;
  tbuffer_t buf;

  tbuffer_single(&buf, size, data);

  make_ibp_sync_context();

  init_ibp_op(_ibp_sync, &op);
  set_ibp_append_op(&op, cap, &buf, 0, size, timer->ClientTimeout);
  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) return(0);
  return(size);
}

//**************************************************************************
// IBP_load - Reads data from the given IBP allocation
//**************************************************************************

unsigned long int IBP_load(ibp_cap_t *cap, ibp_timer_t  *timer, char *data, 
        unsigned long int size, unsigned long int offset)
{
  ibp_op_t op;
  int err;
  tbuffer_t buf;

  tbuffer_single(&buf, size, data);

  make_ibp_sync_context();

  init_ibp_op(_ibp_sync, &op);
  set_ibp_read_op(&op, cap, offset, &buf, 0, size, timer->ClientTimeout);
  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) return(0);
  return(size);
}

//**************************************************************************
//  IBP_copy - Copies data between allocations. The data is *appended* to the 
//      end of the destination allocation.
//**************************************************************************

unsigned long int IBP_copy(ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
        unsigned long int size, unsigned long int offset)
{
  ibp_op_t op;
  int err;

  make_ibp_sync_context();

  init_ibp_op(_ibp_sync, &op);
  set_ibp_copyappend_op(&op, NS_TYPE_SOCK, NULL, srccap, destcap, offset, size, src_timer->ClientTimeout, 
        dest_timer->ServerSync, dest_timer->ClientTimeout);
  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) return(0);
  return(size);
}

//**************************************************************************
//  IBP_copy - Copies data between allocations. The data is *appended* to the 
//      end of the destination allocation.
//**************************************************************************

unsigned long int IBP_phoebus_copy(char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
        ibp_off_t size, ibp_off_t offset)
{
  ibp_op_t op;
  int err;

  make_ibp_sync_context();

  init_ibp_op(_ibp_sync, &op);
  set_ibp_copyappend_op(&op, NS_TYPE_PHOEBUS, path, srccap, destcap, offset, size, src_timer->ClientTimeout, 
        dest_timer->ServerSync, dest_timer->ClientTimeout);
  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) return(0);
  return(size);
}

//**************************************************************************
// IBP_manage - Queries/modifies an IBP allocations properties
//**************************************************************************

int IBP_manage(ibp_cap_t *cap, ibp_timer_t  *timer, int cmd, int captype, ibp_capstatus_t *cs)
{
  ibp_op_t op;
  int err;

  make_ibp_sync_context();

  init_ibp_op(_ibp_sync, &op);

log_printf(15, "IBP_manage: cmd=%d cap=%s cctype=%d\n", cmd, cap, op.ic->cc[IBP_MANAGE].type); fflush(stdout);

  err = 0;
  switch (cmd) {
    case IBP_INCR:
    case IBP_DECR:
       set_ibp_modify_count_op(&op, cap, cmd, captype, timer->ClientTimeout);
       break;
    case IBP_PROBE:
       set_ibp_probe_op(&op, cap, cs, timer->ClientTimeout);
       break;
    case IBP_CHNG:
       set_ibp_modify_alloc_op(&op, cap, cs->maxSize, cs->attrib.duration, cs->attrib.reliability, timer->ClientTimeout);
       break;
    default:
       err = 1;
       log_printf(0, "IBP_manage:  Invalid command: %d\n", cmd);
  }

log_printf(15, "AFTER IBP_manage: cmd=%d cap=%s cctype=%d\n", cmd, cap, op.ic->cc[IBP_MANAGE].type); fflush(stdout);

  if (err == 0) {
    err = ibp_sync_command(&op);
  } else {
    IBP_errno = IBP_E_INVALID_PARAMETER;
  }

  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (IBP_errno != IBP_OK) return(-1);
  return(0);
} 

//**************************************************************************
// IBP_status - Reads data from the given IBP allocation
//**************************************************************************

ibp_depotinfo_t *IBP_status(ibp_depot_t *depot, int cmd, ibp_timer_t *timer, char *password,
                        unsigned long int  hard, unsigned long int soft, long duration)
{
  int err;
  ibp_op_t op;
  ibp_depotinfo_t *di = NULL;

  make_ibp_sync_context();
  
  init_ibp_op(_ibp_sync, &op);

  if (cmd == IBP_ST_INQ) {
     di = (ibp_depotinfo_t *)malloc(sizeof(ibp_depotinfo_t));
     assert(di != NULL);
     set_ibp_depot_inq_op(&op, depot, password, di, timer->ClientTimeout);
  } else {
     set_ibp_depot_modify_op(&op, depot, password, hard, soft, duration, timer->ClientTimeout); 
  }

  err = ibp_sync_command(&op);
  gop_free(ibp_get_gop(&op), OP_FINALIZE);

  if (err != IBP_OK) { free(di); return(NULL); }
  return(di);
}

