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

#define _log_module_index 129

#include <assert.h>
#include <apr_pools.h>
#include <apr_thread_proc.h>
#include "apr_wrapper.h"
#include "iniparse.h"
#include "dns_cache.h"
#include "opque.h"
#include "ibp.h"
#include "ibp_misc.h"
#include "network.h"
#include "net_sock.h"
#include "net_phoebus.h"
#include "net_1_ssl.h"
#include "net_2_ssl.h"
#include "log.h"
#include "phoebus.h"
#include "type_malloc.h"

extern apr_thread_once_t *_err_once;

//** These are in bip_op.c
op_status_t vec_read_command(op_generic_t *gop, NetStream_t *ns);
op_status_t vec_write_command(op_generic_t *gop, NetStream_t *ns);

void *_ibp_dup_connect_context(void *connect_context);
void _ibp_destroy_connect_context(void *connect_context);
int _ibp_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout);

void _ibp_op_free(op_generic_t *op, int mode);
void _ibp_submit_op(void *arg, op_generic_t *op);

static portal_fn_t _ibp_base_portal = {
  .dup_connect_context = _ibp_dup_connect_context,
  .destroy_connect_context = _ibp_destroy_connect_context,
  .connect = _ibp_connect,
  .close_connection = close_netstream,
  .sort_tasks = default_sort_ops,
  .submit = _ibp_submit_op,
  .sync_exec = NULL
};

int _ibp_context_count = 0;

typedef struct {
  Stack_t stack;
} rwc_gop_stack_t;

typedef struct {
//  Stack_t *hp_stack;
  Stack_t list_stack;
  pigeon_coop_hole_t pch;
} rw_coalesce_t;

//*************************************************************
// rwc_gop_stack_new - Creates a new rwc_stack_t set
//*************************************************************

void *rwc_gop_stack_new(void *arg, int size)
{
  rwc_gop_stack_t *shelf;
  int i;

  type_malloc_clear(shelf, rwc_gop_stack_t, size);

  for (i=0; i<size; i++) {
    init_stack(&(shelf[i].stack));
  }

  return((void *)shelf);
}

//*************************************************************
// rwc_gop_stack_free - Destroys a rwc_stack_t set
//*************************************************************

void rwc_gop_stack_free(void *arg, int size, void *data)
{
  rwc_gop_stack_t *shelf = (rwc_gop_stack_t *)data;
  int i;

  for (i=0; i<size; i++) {
    empty_stack(&(shelf[i].stack), 0);
  }

  free(shelf);
  return;
}

//*************************************************************
// rwc_stacks_new - Creates a new rw_coalesce_t set
//*************************************************************

void *rwc_stacks_new(void *arg, int size)
{
  rw_coalesce_t *shelf;
  int i;

  type_malloc_clear(shelf, rw_coalesce_t, size);

  for (i=0; i<size; i++) {
    init_stack(&(shelf[i].list_stack));
  }

  return((void *)shelf);
}

//*************************************************************
// rwc_stacks_free - Destroys a rw_coalesce_t set
//*************************************************************

void rwc_stacks_free(void *arg, int size, void *data)
{
  rw_coalesce_t *shelf = (rw_coalesce_t *)data;
  int i;

  for (i=0; i<size; i++) {
    empty_stack(&(shelf[i].list_stack), 0);
  }

  free(shelf);
  return;
}

//*************************************************************
// ibp_rw_submit_coalesce - Registers the RW op so it can
//   be combined with other similar ops
//*************************************************************

int ibp_rw_submit_coalesce(Stack_t *stack, Stack_ele_t *ele)
{
  op_generic_t *gop = (op_generic_t *)get_stack_ele_data(ele);
  ibp_op_t *iop = ibp_get_iop(gop);
  ibp_context_t *ic = iop->ic;
  ibp_op_rw_t *cmd = &(iop->rw_op);
  rw_coalesce_t *rwc;
  pigeon_coop_hole_t pch;

  apr_thread_mutex_lock(ic->lock);

  if (ic->coalesce_enable == 0) {apr_thread_mutex_unlock(ic->lock); return(0); }

  //** If already coalesced return
log_printf(15, "gid=%d coalesced_ops=%p\n", gop_id(gop), gop->op->cmd.coalesced_ops);

  if (gop->op->cmd.coalesced_ops != NULL) { apr_thread_mutex_unlock(ic->lock); return(0); }

  rwc = (rw_coalesce_t *)list_search(ic->coalesced_ops, cmd->cap);
  if (rwc == NULL) {  //** First time so add a stack
    pch = reserve_pigeon_coop_hole(ic->coalesced_stacks);
    rwc = (rw_coalesce_t *)pigeon_coop_hole_data(&pch);
    rwc->pch = pch;
    list_insert(ic->coalesced_ops, cmd->cap, rwc);
//    rwc->hp_stack = stack;
  }

  move_to_bottom(&(rwc->list_stack));
  insert_below(&(rwc->list_stack), ele);
  iop->hp_parent = stack;

  log_printf(15, "ibp_rw_submit_coalesce: gid=%d cap=%s count=%d\n", gop_id(gop), cmd->cap, stack_size(&(rwc->list_stack)));

  apr_thread_mutex_unlock(ic->lock);

  return(0);
}

//*************************************************************
// ibp_rw_coalesce - Coalesces read or write op with other pending ops
//*************************************************************

int ibp_rw_coalesce(op_generic_t *gop1)
{
  ibp_op_t *iop1 = ibp_get_iop(gop1);
  ibp_op_t *iop2;
  ibp_context_t *ic = iop1->ic;
  ibp_op_rw_t *cmd1 = &(iop1->rw_op);
  ibp_op_rw_t *cmd2;
  op_generic_t *gop2;
  ibp_rw_buf_t **rwbuf;
  rw_coalesce_t *rwc;
  Stack_ele_t *ele;
  Stack_t *cstack;
  int64_t workload;
  int n, iov_sum, found_myself;
  rwc_gop_stack_t *rwcg;
  pigeon_coop_hole_t pch;
  Stack_t *my_hp = iop1->hp_parent;;

  apr_thread_mutex_lock(ic->lock);

  if ((ic->coalesce_enable == 0) || (gop1->op->cmd.coalesced_ops != NULL)) { apr_thread_mutex_unlock(ic->lock); return(0); }
//  if (ic->coalesce_enable == 0) {apr_thread_mutex_unlock(ic->lock); return(0); }

  rwc = (rw_coalesce_t *)list_search(ic->coalesced_ops, cmd1->cap);

  if (rwc == NULL) { //** Nothing to do so exit;
    apr_thread_mutex_unlock(ic->lock);
    return(0);
  }

  if (stack_size(&(rwc->list_stack)) == 1) { //** Nothing to do so exit
     ele = (Stack_ele_t *)pop(&(rwc->list_stack));  //** The top most task should be me
     gop2 = (op_generic_t *)get_stack_ele_data(ele);
     if (gop2 != gop1) {
        log_printf(0, "ERROR! top stack element is not me! gid1=%d gid2=%d\n", gop_id(gop1), gop_id(gop2));
        flush_log();
        abort();
     }

     list_remove(ic->coalesced_ops, cmd1->cap, NULL);
     release_pigeon_coop_hole(ic->coalesced_stacks, &(rwc->pch));

     apr_thread_mutex_unlock(ic->lock);
     return(0);
  }

  log_printf(15, "ibp_rw_coalesce: gid=%d cap=%s count=%d\n", gop_id(gop1), cmd1->cap, stack_size(&(rwc->list_stack)));

  workload = 0;
  n = stack_size(&(rwc->list_stack))+1;
  type_malloc(rwbuf, ibp_rw_buf_t *, n);
  pch = reserve_pigeon_coop_hole(ic->coalesced_gop_stacks);
  rwcg = (rwc_gop_stack_t *)pigeon_coop_hole_data(&pch);
  cmd1->rwcg_pch = pch;
  cstack = &(rwcg->stack);
  gop1->op->cmd.coalesced_ops = cstack;
  cmd1->rwbuf = rwbuf;

  //** PAtch over the send command function
  if (cmd1->rw_mode == IBP_READ) {
     gop1->op->cmd.send_command = vec_read_command;
  } else {
     gop1->op->cmd.send_command = vec_write_command;
  }

  n = 0;
  iov_sum = 0;
  found_myself = 0;
  move_to_top(&(rwc->list_stack));
  ele = (Stack_ele_t *)get_ele_data(&(rwc->list_stack));
  do {
     gop2 = (op_generic_t *)get_stack_ele_data(ele);
     iop2 = ibp_get_iop(gop2);
     cmd2 = &(iop2->rw_op);

     //** Unlink the element if needed
     if (gop2 != gop1) {
        if (iop2->hp_parent == my_hp) {
//           move_to_ptr(rwc->hp_stack, ele);
//           stack_unlink_current(rwc->hp_stack, 0);
           move_to_ptr(iop2->hp_parent, ele);
           stack_unlink_current(iop2->hp_parent, 0);
           push_link(cstack, ele);
        }
     } else {
        found_myself = 1;
     }


     //** Can only coalesce ops on the same hoststr
     if (iop2->hp_parent == my_hp) {
log_printf(15, "ibp_rw_coalesce: gop[%d]->gid=%d n_iov=%d io_total=%d\n", n, gop_id(gop2), cmd2->n_iovec_total, iov_sum);
        rwbuf[n] = &(cmd2->buf_single);
        iov_sum += cmd2->n_iovec_total;
        workload += cmd2->size;
        n++;

        delete_current(&(rwc->list_stack), 0, 0);  //** Remove it from the stack and move down to the next
        ele = NULL;
        if (workload < ic->max_coalesce) { ele = (Stack_ele_t *)get_ele_data(&(rwc->list_stack)); }
     } else {
       log_printf(15, "SKIPPING: gop[-]->gid=%d n_iov=%d io_total=%d\n", gop_id(gop2), cmd2->n_iovec_total, iov_sum);
       move_down(&(rwc->list_stack));
       ele = (Stack_ele_t *)get_ele_data(&(rwc->list_stack));
     }
  } while ((ele != NULL) && (workload < ic->max_coalesce) && (iov_sum < 2000));

  if (found_myself == 0) {  //** Oops! Hit the max_coalesce workdload or size so Got to scan the list for myself
     move_to_top(&(rwc->list_stack));
     while ((ele = (Stack_ele_t *)get_ele_data(&(rwc->list_stack))) != NULL) {
       gop2 = (op_generic_t *)get_stack_ele_data(ele);
       if (gop2 == gop1) {
          iop2 = ibp_get_iop(gop2);
          cmd2 = &(iop2->rw_op);
          rwbuf[n] = &(cmd2->buf_single);
          iov_sum += cmd2->n_iovec_total;
          workload += cmd2->size;
          n++;

          delete_current(&(rwc->list_stack), 0, 0);
          found_myself = 1;
          break;
       }

       move_down(&(rwc->list_stack));
     }

     if (found_myself == 0) {
        log_printf(0, "ERROR! Scanned entire list and couldnt find myself! gid1=%d\n", gop_id(gop1));
        flush_log();
        abort();
     }
  }

if (n > 0) log_printf(1, " Coalescing %d ops totaling " I64T " bytes  iov_sum=%d\n", n, workload, iov_sum);
if (stack_size(&(rwc->list_stack)) > 0) log_printf(1, "%d ops left on stack to coalesce\n", stack_size(&(rwc->list_stack)));

  cmd1->n_ops = n;
  cmd1->n_iovec_total = iov_sum;
  cmd1->size = workload;
  gop1->op->cmd.workload = workload + ic->rw_new_command;

  if (stack_size(&(rwc->list_stack)) == 0) {  //** Nothing left so free it
     list_remove(ic->coalesced_ops, cmd1->cap, NULL);
     release_pigeon_coop_hole(ic->coalesced_stacks, &(rwc->pch));
  }

  apr_thread_mutex_unlock(ic->lock);

  return(0);
}


//*************************************************************


portal_fn_t default_ibp_oplist_imp() { return(_ibp_base_portal); }


//*************************************************************

void _ibp_op_free(op_generic_t *gop, int mode)
{
  ibp_op_t *iop;

  log_printf(15, "_ibp_op_free: mode=%d gid=%d gop=%p\n", mode, gop_id(gop), gop); flush_log();

  if (gop->op->cmd.hostport != NULL) {
     free(gop->op->cmd.hostport);
     gop->op->cmd.hostport = NULL;
  }

  if (gop->op->cmd.coalesced_ops != NULL) {  //** Coalesced R/W op so free the stack
log_printf(15, "gid=%d Freeing rwbuf\n", gop_id(gop));
     iop = ibp_get_iop(gop);
     release_pigeon_coop_hole(iop->ic->coalesced_gop_stacks, &(iop->rw_op.rwcg_pch));
     free(iop->rw_op.rwbuf);
     gop->op->cmd.coalesced_ops = NULL;
  }
  gop_generic_free(gop, OP_FINALIZE);  //** I free the actual op

  if (mode == OP_DESTROY) free(gop->free_ptr);
  log_printf(15, "_ibp_op_free: END\n"); flush_log();

}

//*************************************************************

void _ibp_submit_op(void *arg, op_generic_t *gop)
{
// portal_context_t *pc = ((ibp_op_t *)gop->op)->ic->pc;
 portal_context_t *pc = gop->base.pc;

 log_printf(15, "_ibp_submit_op: hpc=%p hpc->table=%p gop=%p gid=%d\n", pc, pc->table, gop, gop_id(gop));

  if (gop->base.execution_mode == OP_EXEC_DIRECT) {
     submit_hp_direct_op(pc, gop);
  } else {
     submit_hp_que_op(pc, gop);
  }
}

//********************************************************************
// _ibp_dup_connect_context - Copies an IBP connect_context structure
//********************************************************************

void *_ibp_dup_connect_context(void *connect_context)
{
  ibp_connect_context_t *cc = (ibp_connect_context_t *)connect_context;
  ibp_connect_context_t *ccdup;

  if (cc == NULL) return(NULL);

  ccdup = (ibp_connect_context_t *)malloc(sizeof(ibp_connect_context_t));
  assert(ccdup != NULL);

  *ccdup = *cc;

  return((void *)ccdup);
}

//********************************************************************
// _ibp_destroy_connect_context - Frees/Destroys a IBP connect_context structure
//********************************************************************

void _ibp_destroy_connect_context(void *connect_context)
{
  if (connect_context == NULL) return;

  free(connect_context);

  return;
}


//**********************************************************
// _ibp_connect - Makes an IBP connection to a remote host
//     If connect_context == NULL then a standard socket based 
//     connection is made.
//**********************************************************

int _ibp_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout)
{
  ibp_connect_context_t *cc = (ibp_connect_context_t *)connect_context;
  int i, n;

int to = timeout;
log_printf(0, "HOST host=%s to=%d\n", host, to);
//log_printf(15, "cc=%p\n", cc); flush_log();
//log_printf(15, "cc->type=%d\n", cc->type); flush_log();
//log_printf(15, "cc->tcpsize=%d\n", cc->tcpsize); flush_log();

  if (cc != NULL) {
     switch(cc->type) {
       case NS_TYPE_SOCK:
          ns_config_sock(ns, cc->tcpsize);
          break;
       case NS_TYPE_PHOEBUS:
          ns_config_phoebus(ns, cc->data, cc->tcpsize);
          break;
       case NS_TYPE_1_SSL:
          ns_config_1_ssl(ns, -1, cc->tcpsize);
          break;
       case NS_TYPE_2_SSL:
//****          ns_config_2_ssl(ns, -1);
          break;
       default:
          log_printf(0, "_ibp__connect: Invalid type=%d Exiting!\n", cc->type);
          return(1);
      }
  } else {
     ns_config_sock(ns, 0);
  }

  //** See if we have an RID if so peel it off
  i = 0;
  while ((host[i] != 0) && (host[i] != '#')) i++;

  if (host[i] == '#') { host[i] = 0; i=-i; }
//log_printf(0, "net_connect(%s)\n", host);
  n = net_connect(ns, host, port, timeout);
  if (i<0) host[-i] = '#';

  return(n);
}


//**********************************************************
// set/unset routines for options
//**********************************************************

int ibp_set_chksum(ibp_context_t *ic, ns_chksum_t *ncs)
{
  ns_chksum_clear(&(ic->ncs));
  if (ncs != NULL) ic->ncs = *ncs;
  return(0);
}
void ibp_get_chksum(ibp_context_t *ic, ns_chksum_t *ncs) { *ncs = ic->ncs; };

void ibp_set_abort_attempts(ibp_context_t *ic, int n) { ic->abort_conn_attempts = n;}
int  ibp_get_abort_attempts(ibp_context_t *ic) { return(ic->abort_conn_attempts); }
void ibp_set_tcpsize(ibp_context_t *ic, int n) { ic->tcpsize = n;}
int  ibp_get_tcpsize(ibp_context_t *ic) { return(ic->tcpsize); }
void ibp_set_min_depot_threads(ibp_context_t *ic, int n) 
   { ic->min_threads = n; ic->pc->min_threads = n; change_all_hportal_conn(ic->pc, ic->min_threads, ic->max_threads, ic->dt_connect); }
int  ibp_get_min_depot_threads(ibp_context_t *ic) { return(ic->min_threads); }
void ibp_set_max_depot_threads(ibp_context_t *ic, int n) 
   { ic->max_threads = n; ic->pc->max_threads = n; change_all_hportal_conn(ic->pc, ic->min_threads, ic->max_threads, ic->dt_connect); }
int  ibp_get_max_depot_threads(ibp_context_t *ic) { return(ic->max_threads); }
void ibp_set_max_connections(ibp_context_t *ic, int n) 
   { ic->max_connections = n; ic->pc->max_connections = n; }
int  ibp_get_max_connections(ibp_context_t *ic) { return(ic->max_connections); }
void ibp_set_command_weight(ibp_context_t *ic, int n) { ic->other_new_command = n; }
int  ibp_get_command_weight(ibp_context_t *ic) { return(ic->other_new_command); }
void ibp_set_max_retry_wait(ibp_context_t *ic, int n) { ic->max_wait = n; ic->pc->max_wait = n;}
int  ibp_get_max_retry_wait(ibp_context_t *ic) { return(ic->max_wait); }
void ibp_set_max_thread_workload(ibp_context_t *ic, int64_t n) { ic->max_workload = n; ic->pc->max_workload = n;}
int64_t  ibp_get_max_thread_workload(ibp_context_t *ic) { return(ic->max_workload); }
void ibp_set_max_coalesce_workload(ibp_context_t *ic, int64_t n) { ic->max_coalesce = n; }
int64_t  ibp_get_max_coalesce_workload(ibp_context_t *ic) { return(ic->max_coalesce); }
void ibp_set_wait_stable_time(ibp_context_t *ic, int n) { ic->wait_stable_time = n; ic->pc->wait_stable_time = n;}
int  ibp_get_wait_stable_time(ibp_context_t *ic) { return(ic->wait_stable_time); }
void ibp_set_check_interval(ibp_context_t *ic, int n) { ic->check_connection_interval = n; ic->pc->check_connection_interval = n;}
int  ibp_get_check_interval(ibp_context_t *ic) { return(ic->check_connection_interval); }
void ibp_set_max_retry(ibp_context_t *ic, int n) { ic->max_retry = n; ic->pc->max_retry = n;}
int  ibp_get_max_retry(ibp_context_t *ic) { return(ic->max_retry); }
void ibp_set_transfer_rate(ibp_context_t *ic, double rate) { ic->transfer_rate = rate;}
double  ibp_get_transfer_rate(ibp_context_t *ic) { return(ic->transfer_rate); }

//**********************************************************
// set_ibp_config - Sets the ibp config options
//**********************************************************

void copy_ibp_config(ibp_context_t *cfg)
{

  cfg->pc->min_idle = cfg->min_idle;
  cfg->pc->min_threads = cfg->min_threads;
  cfg->pc->max_threads = cfg->max_threads;
  cfg->pc->max_connections = cfg->max_connections;
  cfg->pc->max_workload = cfg->max_workload;
  cfg->pc->max_wait = cfg->max_wait;
  cfg->pc->dt_connect = cfg->dt_connect;
  cfg->pc->wait_stable_time = cfg->wait_stable_time;
  cfg->pc->abort_conn_attempts = cfg->abort_conn_attempts;
  cfg->pc->check_connection_interval = cfg->check_connection_interval;
  cfg->pc->max_retry = cfg->max_retry;
}

//**********************************************************
// Set the default CC's for read or write
//**********************************************************

void ibp_set_read_cc(ibp_context_t *ic, ibp_connect_context_t *cc)
{

  ic->cc[IBP_LOAD] = *cc;
  ic->cc[IBP_SEND] = *cc;
  ic->cc[IBP_PHOEBUS_SEND] = *cc;
}

//**********************************************************

void ibp_set_write_cc(ibp_context_t *ic, ibp_connect_context_t *cc)
{
  ic->cc[IBP_WRITE] = *cc;
  ic->cc[IBP_STORE] = *cc;
}

//**********************************************************
// cc_load - Stores a CC from the given keyfile
//**********************************************************

void cc_load(inip_file_t *kf, char *name, ibp_connect_context_t *cc)
{
  char *type = inip_get_string(kf, "ibp_connect", name, NULL);

  if (type == NULL) return;
  
  if (strcmp(type, "socket") == 0) {
     cc->type = NS_TYPE_SOCK;
  } else if (strcmp(type, "phoebus") == 0) {
     cc->type = NS_TYPE_PHOEBUS;
  } else if (strcmp(type, "ssl1") == 0) {
     cc->type = NS_TYPE_1_SSL;
  } else if (strcmp(type, "ssl2") == 0) {
     cc->type = NS_TYPE_2_SSL;
  } else {
     log_printf(0, "cc_load: Invalid CC type! command: %s type: %s\n", name, type);
  }
}

//**********************************************************
// ibp_cc_table_load - Loads the default connect_context for commands
//**********************************************************

void ibp_cc_load(inip_file_t *kf, ibp_context_t *cfg)
{
  int i;
  ibp_connect_context_t cc;

  //** Set everything to the default **
  cc.type = NS_TYPE_SOCK;
  cc.tcpsize = 0;
  cc_load(kf, "default", &cc);
  for (i=0; i<=IBP_MAX_NUM_CMDS; i++) cfg->cc[i] = cc;
  
  //** Now load the individual commands if they exist
  cc_load(kf, "ibp_allocate", &(cfg->cc[IBP_ALLOCATE]));
  cc_load(kf, "ibp_store", &(cfg->cc[IBP_STORE]));
  cc_load(kf, "ibp_status", &(cfg->cc[IBP_STATUS]));
  cc_load(kf, "ibp_send", &(cfg->cc[IBP_SEND]));
  cc_load(kf, "ibp_load", &(cfg->cc[IBP_LOAD]));
  cc_load(kf, "ibp_manage", &(cfg->cc[IBP_MANAGE]));
  cc_load(kf, "ibp_write", &(cfg->cc[IBP_WRITE]));
  cc_load(kf, "ibp_alias_allocate", &(cfg->cc[IBP_ALIAS_ALLOCATE]));
  cc_load(kf, "ibp_alias_manage", &(cfg->cc[IBP_ALIAS_MANAGE]));
  cc_load(kf, "ibp_rename", &(cfg->cc[IBP_RENAME]));
  cc_load(kf, "ibp_phoebus_send", &(cfg->cc[IBP_PHOEBUS_SEND]));
  cc_load(kf, "ibp_push", &(cfg->cc[IBP_PUSH]));
  cc_load(kf, "ibp_pull", &(cfg->cc[IBP_PULL]));
}


//**********************************************************
// ibp_load_config - Loads the ibp client config
//**********************************************************

int ibp_load_config(ibp_context_t *ic, inip_file_t *keyfile, char *section)
{
  apr_time_t t = 0;

  if (section == NULL) section = "ibp";

  ic->abort_conn_attempts = inip_get_integer(keyfile, section, "abort_attempts", ic->abort_conn_attempts);
  t = inip_get_integer(keyfile, section, "min_idle", 0);
  if (t != 0) ic->min_idle = t;
  ic->tcpsize = inip_get_integer(keyfile, section, "tcpsize", ic->tcpsize);
  ic->min_threads = inip_get_integer(keyfile, section, "min_depot_threads", ic->min_threads);
  ic->max_threads = inip_get_integer(keyfile, section, "max_depot_threads", ic->max_threads);
  ic->dt_connect = inip_get_integer(keyfile, section, "dt_connect_us", ic->dt_connect);
  ic->max_connections = inip_get_integer(keyfile, section, "max_connections", ic->max_connections);
  ic->rw_new_command = inip_get_integer(keyfile, section, "rw_command_weight", ic->rw_new_command);
  ic->other_new_command = inip_get_integer(keyfile, section, "other_command_weight", ic->other_new_command);
  ic->max_workload = inip_get_integer(keyfile, section, "max_thread_workload", ic->max_workload);
  ic->max_coalesce = inip_get_integer(keyfile, section, "max_coalesce_workload", ic->max_coalesce);
  ic->coalesce_enable = inip_get_integer(keyfile, section, "coalesce_enable", ic->coalesce_enable);
  ic->max_wait = inip_get_integer(keyfile, section, "max_wait", ic->max_wait);
  ic->wait_stable_time = inip_get_integer(keyfile, section, "wait_stable_time", ic->wait_stable_time);
  ic->check_connection_interval = inip_get_integer(keyfile, section, "check_interval", ic->check_connection_interval);
  ic->max_retry = inip_get_integer(keyfile, section, "max_retry", ic->max_retry);
  ic->connection_mode = inip_get_integer(keyfile, section, "connection_mode", ic->connection_mode);
  ic->transfer_rate = inip_get_double(keyfile, section, "transfer_rate", ic->transfer_rate);
  ic->rr_size = inip_get_integer(keyfile, section, "rr_size", ic->rr_size);

  ibp_cc_load(keyfile, ic);

  phoebus_load_config(keyfile);

  copy_ibp_config(ic);

  log_printf(1, "section=%s cmode=%d min_depot_threads=%d max_depot_threads=%d max_connections=%d max_thread_workload=%d coalesce_enable=%d dt_connect=" TT "\n", section, ic->connection_mode, ic->min_threads, ic->max_threads, ic->max_connections, ic->max_workload, ic->coalesce_enable, ic->dt_connect);

  return(0);
}

//**********************************************************
// ibp_load_config_file - Loads the ibp client config from a text file
//**********************************************************

int ibp_load_config_file(ibp_context_t *ic, char *fname, char *section)
{
  inip_file_t *keyfile;
  int err;

  //* Load the config file
  keyfile = inip_read(fname);
  if (keyfile == NULL) {
    log_printf(0, "Error parsing config file! file=%s\n", fname);
    return(-1);
  }

  err = ibp_load_config(ic, keyfile, section);

  inip_destroy(keyfile);

  return(err);
}


//**********************************************************
// default_ibp_config - Sets the default ibp config options
//**********************************************************

void default_ibp_config(ibp_context_t *ic)
{
  int i;

  ns_chksum_clear(&(ic->ncs));

  ic->tcpsize = 0;
  ic->min_idle = apr_time_make(30, 0);
  ic->min_threads = 1;
  ic->max_threads = 4;
  ic->max_connections = 128;
  ic->rw_new_command = 10*1024;
  ic->other_new_command = 100*1024;
  ic->max_workload = 10*1024*1024;
  ic->max_coalesce = ic->max_workload;
  ic->coalesce_enable = 1;
  ic->max_wait = 30;
  ic->dt_connect = apr_time_from_sec(5);
  ic->wait_stable_time = 15;
  ic->abort_conn_attempts = 4;
  ic->check_connection_interval = 2;
  ic->max_retry = 2;
//  ic->transfer_rate = 1*1024*1024;  //** 1MB/s
  ic->transfer_rate = 0;
  ic->rr_size = 4;
  ic->connection_mode = IBP_CMODE_HOST;

  for (i=0; i<=IBP_MAX_NUM_CMDS; i++) {
     ic->cc[i].type = NS_TYPE_SOCK;
  }

  phoebus_init();

  copy_ibp_config(ic);
}


//**********************************************************
//  ibp_create_context - Creates an IBP context
//**********************************************************

ibp_context_t *ibp_create_context()
{
  ibp_context_t *ic = (ibp_context_t *)malloc(sizeof(ibp_context_t));
  assert(ic != NULL);
  memset(ic, 0, sizeof(ibp_context_t));

  assert(apr_wrapper_start() == APR_SUCCESS);

  if (_ibp_context_count == 0) {
     dns_cache_init(100);

     ibp_configure_signals();
  }

  ic->pc = create_hportal_context(&_ibp_base_portal);

  default_ibp_config(ic);

  apr_pool_create(&(ic->mpool), NULL);

  if (_ibp_context_count == 0) {
     ibp_errno_init();

     init_opque_system();
  }

  _ibp_context_count++;

  atomic_set(ic->n_ops, 0);

  ic->coalesced_stacks = new_pigeon_coop("ibp_coalesced_stacks", 50, sizeof(rw_coalesce_t), NULL, rwc_stacks_new, rwc_stacks_free);
  ic->coalesced_gop_stacks = new_pigeon_coop("ibp_coalesced_gop_stacks", 50, sizeof(rwc_gop_stack_t), NULL, rwc_gop_stack_new, rwc_gop_stack_free);
  ic->coalesced_ops = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  apr_thread_mutex_create(&(ic->lock), APR_THREAD_MUTEX_DEFAULT, ic->mpool);

  return(ic);
}


//**********************************************************
//  ibp_destroy_context - Shuts down the IBP subsystem
//**********************************************************

void ibp_destroy_context(ibp_context_t *ic)
{
  log_printf(15, "ibp_destroy_context: Shutting down! count=%d\n", _ibp_context_count);

  shutdown_hportal(ic->pc);
  destroy_hportal_context(ic->pc);

  destroy_pigeon_coop(ic->coalesced_stacks);
  destroy_pigeon_coop(ic->coalesced_gop_stacks);
  list_destroy(ic->coalesced_ops);

  apr_thread_mutex_destroy(ic->lock);

  apr_pool_destroy(ic->mpool);

  _ibp_context_count--;
  if (_ibp_context_count == 0) {
     finalize_dns_cache();

     destroy_opque_system();

     phoebus_destroy();
  }

  apr_wrapper_stop();

  free(ic);
}
