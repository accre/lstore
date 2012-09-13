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

#define _log_module_index 132

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "ibp.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "ibp_misc.h"
#include "dns_cache.h"
#include "type_malloc.h"
#include "append_printf.h"
#include "type_malloc.h"

#define ibp_set_status(v, opstat, errcode) (v).op_status = status; (v).error_code = errorcode

Net_timeout_t global_dt = 1*1000000;
apr_time_t gop_get_end_time(op_generic_t *gop, int *state);
op_status_t gop_readline_with_timeout(NetStream_t *ns, char *buffer, int size, op_generic_t *gop);
//op_status_t write_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, ibp_off_t pos, ibp_off_t size);
op_status_t gop_write_block(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, ibp_off_t pos, ibp_off_t size);
op_status_t gop_read_block(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, ibp_off_t pos, ibp_off_t size);

op_status_t status_get_recv(op_generic_t *gop, NetStream_t *ns);
void _ibp_op_free(op_generic_t *op, int mode);

op_status_t vec_read_command(op_generic_t *gop, NetStream_t *ns);
op_status_t vec_write_command(op_generic_t *gop, NetStream_t *ns);

op_status_t ibp_success_status = {OP_STATE_SUCCESS, IBP_OK};
op_status_t ibp_failure_status = {OP_STATE_FAILURE, 0};
op_status_t ibp_retry_status = {OP_STATE_RETRY, ERR_RETRY_DEADSOCKET};
op_status_t ibp_dead_status = {OP_STATE_DEAD, ERR_RETRY_DEADSOCKET};
op_status_t ibp_timeout_status = {OP_STATE_TIMEOUT, IBP_E_CLIENT_TIMEOUT};
op_status_t ibp_invalid_host_status = {OP_STATE_INVALID_HOST, IBP_E_INVALID_HOST};
op_status_t ibp_cant_connect_status = {OP_STATE_CANT_CONNECT, IBP_E_CANT_CONNECT};
op_status_t ibp_error_status = {OP_STATE_ERROR, 0};

//*************************************************************
// set_hostport - Sets the hostport string
//   This needs to be changed for each type of NS connection
//*************************************************************

void set_hostport(char *hostport, int max_size, char *host, int port, ibp_connect_context_t *cc)
{
  char in_addr[DNS_ADDR_MAX];
  char ip[64];
  int type, i;

  type = (cc == NULL) ? NS_TYPE_SOCK : cc->type;

//log_printf(0, "HOST host=%s\n", host);
  i = 0;
  while ((host[i] != 0) && (host[i] != '#')) i++;
  if (host[i] == '#') { host[i] = 0; i=-i; }

  if (lookup_host(host, in_addr, ip) != 0) {
     if (i<0) host[-i] = '#';
     log_printf(1, "set_hostport:  Failed to lookup host: %s\n", host);
     hostport[max_size-1] = '\0';
     snprintf(hostport, max_size-1, "%s:%d:%d:0", host, port, type);
     return;
  }
  if (i<0) host[-i] = '#';

//  inet_ntop(AF_INET, (void *)in_addr, ip, 63);
  ip[63] = '\0';

  hostport[max_size-1] = '\0';
  if (type == NS_TYPE_PHOEBUS) {
     snprintf(hostport, max_size-1, "%s" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%s", 
            host, port, type, phoebus_get_key((phoebus_t *)cc->data));
//     snprintf(hostport, max_size-1, "%s" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%s", 
//            ip, port, type, phoebus_get_key((phoebus_t *)cc->data));
  } else {
     snprintf(hostport, max_size-1, "%s" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "0", 
            host, port, type);
//     snprintf(hostport, max_size-1, "%s" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "0", 
//            ip, port, type);
  }

//  log_printf(15, "HOST host=%s hostport=%s\n", host, hostport);
}

//*************************************************************
// change_hostport_cc  - Changes the CC field in the host port string
//*************************************************************

char *change_hostport_cc(char *old_hostport, ibp_connect_context_t *cc)
{
  char host[MAX_HOST_SIZE], new_hostport[MAX_HOST_SIZE];
  char *hp2 = strdup(old_hostport);
  char *bstate;
  int fin, port;

  strncpy(host, string_token(hp2, HP_HOSTPORT_SEPARATOR, &bstate, &fin), sizeof(host)-1); host[sizeof(host)-1] = '\0';
  port = atoi(bstate);

  set_hostport(new_hostport, sizeof(new_hostport), host, port, cc);

  free(hp2);
  return(strdup(new_hostport));
}

//*************************************************************
// process_error - Looks for the generic socket connection from the depot
//*************************************************************

op_status_t process_error(op_generic_t *gop, op_status_t *err, int status, double wait_time, char **bstate)
{
  int fin;
  apr_time_t sec, usec;

  if (status == IBP_E_OUT_OF_SOCKETS) {
     if (bstate != NULL) {
        wait_time = atof(string_token(NULL, " ", bstate, &fin));
     }
     if (wait_time < 0) wait_time = 0;
     sec=wait_time; usec = (wait_time-sec)*1000000;
     gop->op->cmd.retry_wait = apr_time_make(sec, usec);

     log_printf(5, "gid=%d status=%d retry_wait=%lf (s,us)=(" TT "," TT ")\n", gop_id(gop), status, wait_time, sec, usec);
     *err = ibp_retry_status;
  } else if (status == IBP_OK) {
    err->op_status = OP_STATE_SUCCESS;
    err->error_code = IBP_OK;
//  } else if (status == IBP_E_BAD_FORMAT) {
//    err->op_status = OP_STATE_RETRY;
//    err->error_code = IBP_E_BAD_FORMAT;
  } else {
    err->op_status = OP_STATE_FAILURE;
    err->error_code = status;
  }

  log_printf(5, "gid=%d status=%d ibp_err=%d\n", gop_id(gop), err->op_status, err->error_code);

  return(*err);
}

//*************************************************************
//  gop_get_end_time - Gets the op completion time
//*************************************************************

apr_time_t gop_get_end_time(op_generic_t *gop, int *state)
{
  apr_time_t end_time;
  command_op_t *cmd = &(gop->op->cmd);

  if (*state == 0) {
    *state = atomic_get(cmd->on_top);
    if (*state == 0) {
       end_time = apr_time_now() + apr_time_make(10,0);  //** Default to 10 secs while percolating to the top
    } else {  //** We're on top so use the official end time
       lock_gop(gop);
       end_time = cmd->end_time;
       unlock_gop(gop);
    }
  } else {
    end_time = cmd->end_time;  //** This won't change after we're on top so no need to lock
  }

//apr_time_t dt = end_time - apr_time_now();
//log_printf(0, "end_time=" TT " dt=" TT " state=%d\n", end_time, dt, *state);
  return(end_time);
}

//*************************************************************
// send_command - Sends a text string.  USed for sending IBP commands
//*************************************************************

op_status_t send_command(op_generic_t *gop, NetStream_t *ns, char *command)
{
  Net_timeout_t dt;
  set_net_timeout(&dt, 5, 0);
  tbuffer_t buf;
  op_status_t status;

  log_printf(5, "send_command: ns=%d gid=%d command=%s\n", ns_getid(ns), gop_id(gop), command);

  int len = strlen(command);
  tbuffer_single(&buf, len, command);
  status = gop_write_block(ns, gop, &buf, 0, len);
  if (status.op_status !=  OP_STATE_SUCCESS) {
     log_printf(10, "send_command: Error=%d! ns=%d command=!%s!", status.op_status, ns_getid(ns), command);
     return(ibp_retry_status);
  }

  return(status);
}

//*************************************************************
// gop_readline_with_timeout - Reads a line of text with a
//    command timeout
//*************************************************************

op_status_t gop_readline_with_timeout(NetStream_t *ns, char *buffer, int size, op_generic_t *gop)
{
  int nbytes, n, nleft, pos;
  int err, state;
  apr_time_t end_time;
  op_status_t status;
  tbuffer_t tbuf;

  log_printf(15, "readline_with_timeout: START ns=%d size=%d\n", ns_getid(ns), size);
  state = 0;
  nbytes = 0;
  err = 0;
  nleft = size;
  tbuffer_single(&tbuf, size, buffer);
  pos = 0;
  end_time = gop_get_end_time(gop, &state);
  while ((err == 0) && (apr_time_now() <= end_time) && (nleft > 0)) {
     n = readline_netstream_raw(ns, &tbuf, pos, nleft, global_dt, &err);
     nleft = nleft - n;
     nbytes = nbytes + n;
     pos = pos + nbytes;
     log_printf(15, "readline_with_timeout: nbytes=%d nleft=%d err=%d time=" TT " end_time=" TT " ns=%d buffer=%s\n", nbytes, nleft, err, apr_time_now(), end_time, ns_getid(ns), buffer);
     if (nleft > 0) end_time = gop_get_end_time(gop, &state);
  }

  if (err > 0) {
     err = IBP_OK;
     status = ibp_success_status;
     log_printf(15, "readline_with_timeout: END nbytes=%d command=%s\n", nbytes, buffer);
  } else {
     if (err == 0) {
        if (nbytes < size) {
           log_printf(15, "readline_with_timeout: END Client timeout time=" TT " end_time=" TT "ns=%d\n", apr_time_now(), end_time, ns_getid(ns));
        } else {
           log_printf(0, "readline_with_timeout:  END Out of sync issue!! nbytes=%d size=%d ns=%d\n", nbytes, size, ns_getid(ns));
           flush_log();
           err = 0; //** GEnerate a core dump
//           err = 1 / err;
        }
        err = ERR_RETRY_DEADSOCKET;
        status = ibp_retry_status;
     } else {
        log_printf(15, "readline_with_timeout: END connection error=%d ns=%d\n", err, ns_getid(ns));
        err = ERR_RETRY_DEADSOCKET;
        status = ibp_retry_status;
     }
  }


  return(status);
}

//*************************************************************
// init_ibp_op - Initialies an IBP op
//*************************************************************

void init_ibp_op(ibp_context_t *ic, ibp_op_t *op)
{
  op_generic_t *gop;

  //** Clear it
  type_memclear(op, ibp_op_t, 1);

  //** Now munge the pointers
  gop = &(op->gop);
  gop_init(gop);
  gop->op = &(op->dop);
  gop->op->priv = op;
  gop->type = Q_TYPE_OPERATION;
  op->ic = ic;
  op->dop.priv = op;
  op->dop.pc = ic->pc; //**IS this needed?????
  gop->base.free = _ibp_op_free;
  gop->free_ptr = op;
  gop->base.pc = ic->pc;
  gop->base.status = op_error_status;
}

//*************************************************************
// new_ibp_op - Allocates space for a new op
//*************************************************************

ibp_op_t *new_ibp_op(ibp_context_t *ic)
{
  ibp_op_t *op;

  //** Make the struct and clear it
  type_malloc(op, ibp_op_t, 1);

  atomic_inc(ic->n_ops);
  init_ibp_op(ic, op);

  return(op);
}

//*************************************************************
// init_ibp_base_op - initializes  generic op variables
//*************************************************************

void init_ibp_base_op(ibp_op_t *iop, char *logstr, int timeout_sec, int workload, char *hostport,
     int cmp_size, int primary_cmd, int sub_cmd)
{
  command_op_t *cmd = &(iop->dop.cmd);

  iop->primary_cmd = primary_cmd;
  iop->sub_cmd = sub_cmd;
  cmd->timeout = apr_time_make(timeout_sec, 0);
  cmd->retry_count = iop->ic->max_retry;
  cmd->workload = workload;
  cmd->hostport = hostport;
  cmd->cmp_size = cmp_size;
  cmd->send_command = NULL;
  cmd->send_phase = NULL;
  cmd->recv_phase = NULL;
  cmd->on_submit = NULL;
  cmd->before_exec = NULL;
  cmd->destroy_command = NULL;

  cmd->coalesced_ops = NULL;

  cmd->connect_context = &(iop->ic->cc[primary_cmd]);
  ns_chksum_init(&(iop->ncs));
}

//*************************************************************
//  Setter routines for optional fields
//*************************************************************

void ibp_op_set_ncs(op_generic_t *gop, ns_chksum_t *ncs)
{
  if ( ncs == NULL) return;

  ibp_op_t *op = ibp_get_iop(gop);
  op->ncs = *ncs;
}

int ibp_cc_type(ibp_connect_context_t *cc)
{
  if ( cc == NULL) return(NS_TYPE_UNKNOWN);

  return(cc->type);
}

void ibp_op_set_cc(op_generic_t *gop, ibp_connect_context_t *cc)
{
  ibp_op_t *op;
  char *orig;

  if (cc == NULL) return;

  op = ibp_get_iop(gop);

  op->dop.cmd.connect_context = cc;

  orig = op->dop.cmd.hostport;
  if (orig == NULL) return;

  op->dop.cmd.hostport = change_hostport_cc(orig, cc);
  free(orig);
}

//*************************************************************
//  ibp_chksum_set - Initializes the stream chksum.
//     if blocksize == 0 then the default IBP block size is used
//     if cs == NULL the chksum is disabled
//*************************************************************

int ibp_chksum_set(ns_chksum_t *ncs, chksum_t *cs, int blocksize)
{
   if (cs == NULL) {
      ns_chksum_clear(ncs);
   } else {
      if (blocksize == 0) blocksize = IBP_CHKSUM_BLOCKSIZE;
      ns_chksum_set(ncs, cs, blocksize);
      ns_chksum_enable(ncs);
   }

   return(0);
}


//=============================================================
//=============================================================

//=============================================================
//  Read routines
//=============================================================

//*************************************************************
// set_ibp_read_op - Generates a new read operation
//*************************************************************

void set_ibp_read_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
   set_ibp_rw_op(op, IBP_READ, cap, offset, buffer, boff, len, timeout);
}

//*************************************************************
// new_ibp_read_op - Generates a new read operation
//*************************************************************

op_generic_t *new_ibp_read_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
   op_generic_t *op = new_ibp_rw_op(ic, IBP_READ, cap, offset, buffer, boff, len, timeout);
   return(op);
}


//*************************************************************
// set_ibp_vec_read_op - Generates a new vector read operation
//*************************************************************

void set_ibp_vec_read_op(ibp_op_t *op, ibp_cap_t *cap, int n_vec, ibp_iovec_t *vec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
  op_generic_t *gop = ibp_get_gop(op);

  set_ibp_rw_op(op, IBP_READ, cap, 0, buffer, boff, len, timeout);
  op->rw_op.n_ops = 1;
  op->rw_op.n_iovec_total = n_vec;
  op->rw_op.buf_single.n_iovec = n_vec;
  op->rw_op.buf_single.iovec = vec;

  gop->op->cmd.send_command = vec_read_command;
}

//*************************************************************
// new_ibp_vec_read_op - Generates a new vector read operation
//*************************************************************

op_generic_t *new_ibp_vec_read_op(ibp_context_t *ic, ibp_cap_t *cap, int n_vec, ibp_iovec_t *vec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

   set_ibp_vec_read_op(op, cap, n_vec, vec, buffer, boff, len, timeout);

   return(ibp_get_gop(op));
}

//*************************************************************

op_status_t vec_read_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int bufsize = 204800;
  char stackbuffer[bufsize];
  char *buffer = stackbuffer;
  int i, j, used;
  ibp_op_rw_t *cmd;
  ibp_rw_buf_t *rwbuf;
  op_status_t err;

  cmd = &(op->rw_op);

  used = 0;

  //** Store the base command
  if (ns_chksum_is_valid(&(op->ncs)) == 0) {
     append_printf(buffer, &used, bufsize, "%d %d %s %s %d", IBPv040, IBP_VEC_READ, cmd->key, cmd->typekey, cmd->n_iovec_total);
  } else {
     append_printf(buffer, &used, bufsize, "%d %d %d " I64T " %s %s %d",
        IBPv040, IBP_VEC_READ_CHKSUM, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
        cmd->key, cmd->typekey, cmd->n_iovec_total);
  }

  //** Add the IO vec list
  for (j=0; j<cmd->n_ops; j++) {
     rwbuf = cmd->rwbuf[j];
     for (i=0; i<rwbuf->n_iovec; i++) {
        if (used >= (bufsize-100)) {
           bufsize = bufsize * 1.5;
           if (buffer == stackbuffer) {
              buffer = (char *)malloc(bufsize);
              memcpy(buffer, stackbuffer, used);
           } else {
             buffer = (char *)realloc(buffer, bufsize);
           }
        }
        append_printf(buffer, &used, bufsize, " " I64T " " I64T, rwbuf->iovec[i].offset, rwbuf->iovec[i].len);
     }
  }

  //** Add the timeout
  append_printf(buffer, &used, bufsize, " %d\n", (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_set(ns, op->ncs);
  ns_write_chksum_disable(ns);

//log_printf(15, "vec_read_command: sending command ns=%d\n", ns_getid(ns));
  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "read_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  if (buffer != stackbuffer) free(buffer);

  return(err);
}

//*************************************************************

op_status_t read_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_rw_t *cmd;

  cmd = &(op->rw_op);

  if (ns_chksum_is_valid(&(op->ncs)) == 0) { 
//     ns_chksum_reset(&(op->ncs));
     snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d\n", 
        IBPv040, IBP_LOAD, cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else {
     snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " " I64T " %d\n", 
        IBPv040, IBP_LOAD_CHKSUM, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)), 
        cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
  }

  ns_write_chksum_set(ns, op->ncs);
  ns_write_chksum_disable(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "read_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t gop_read_block(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, ibp_off_t pos, ibp_off_t size)
{
  int nbytes, state, bpos, nleft;
  op_status_t status;
  Net_timeout_t dt;
  apr_time_t end_time;

  state = 0;
  set_net_timeout(&dt, 1, 0);
  end_time = gop_get_end_time(gop, &state);

  nbytes = 0;
  bpos = pos;
  nleft = size;
  while ((nbytes != -1) && (nleft > 0) && (apr_time_now() < end_time)) {
     nbytes = read_netstream(ns, buffer, bpos, nleft, dt);
     if (nbytes != -1) {
        bpos += nbytes;
        nleft -= nbytes;
     }

     end_time = gop_get_end_time(gop, &state);

  }

  if (nleft == 0) {
     status = ibp_success_status;
  } else if (apr_time_now() > end_time) {
     status = ibp_timeout_status;
  } else {
     status = ibp_retry_status;  //** Dead connection so retry
  }

  return(status);
}

//*************************************************************

op_status_t read_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  ibp_off_t nbytes;
  int i, status, fin;
  op_status_t err;
  char buffer[1024];
  char *bstate;
  ibp_op_rw_t *cmd;
  double swait;
  ibp_rw_buf_t *rwbuf;

  cmd = &(op->rw_op);

  //** Need to read the depot status info
  log_printf(15, "read_recv: ns=%d starting command size=" OT "\n", ns_getid(ns), cmd->size);

  ns_read_chksum_set(ns, op->ncs);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "read_recv: after readline err = %d  ns=%d buffer=%s\n", err, ns_getid(ns), buffer);

  rwbuf = cmd->rwbuf[0];

  err = ibp_success_status;
  status = IBP_E_GENERIC;
  status = atoi(string_token(buffer, " ", &bstate, &fin));
  swait = atof(string_token(NULL, " ", &bstate, &fin)); nbytes = swait;
  if ((status != IBP_OK) || (nbytes != cmd->size)) {
     log_printf(15, "read_recv: (read) ns=%d cap=%s offset[0]=" I64T " len[0]=" I64T " err=%d Error!  status=%d bytes=!%s!\n", 
          ns_getid(ns), cmd->cap, rwbuf->iovec[0].offset, rwbuf->size, err, status, buffer);

     //**  If coalesced ops then free the coalesced mallocs
//     if (cmd->n_ops > 1) free(cmd->rwbuf);
//     return(process_error(gop, &err, status, swait, NULL));
      process_error(gop, &err, status, swait, NULL);
//      if (err.op_status != OP_STATE_RETRY) {
//         if (cmd->n_ops > 1) free(cmd->rwbuf);
//      }
      return(err);
  }


  //** Turn on chksumming if needed
  if (ns_chksum_is_valid(&(op->ncs)) == 1) {
     ns_chksum_reset(&(op->ncs));
     ns_read_chksum_set(ns, op->ncs);
     ns_read_chksum_enable(ns);
  }

  for (i=0; i<cmd->n_ops; i++) {
    rwbuf = cmd->rwbuf[i];
    err = gop_read_block(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
  log_printf(5, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), ns_getid(ns), i, rwbuf->size);
    if (err.op_status != OP_STATE_SUCCESS) break;

    log_printf(15, "read_recv: ns=%d op_index=%d  size=" I64T " pos=" I64T " time=" TT "\n", ns_getid(ns), i,
        rwbuf->size, rwbuf->boff, apr_time_now());
  }

  if (err.op_status == OP_STATE_SUCCESS) {  //** Call the next block routine to process the last chunk
     if (ns_chksum_is_valid(&(op->ncs)) == 1) {
         if (ns_read_chksum_flush(ns) != 0) { err.op_status = OP_STATE_FAILURE; err.error_code = IBP_E_CHKSUM; }
         ns_read_chksum_disable(ns);
     }
  } else {
     rwbuf = cmd->rwbuf[0];
     log_printf(0, "read_recv: (read) ns=%d cap=%s offset[0]=" I64T " len[0]=" I64T " Error!\n", 
         ns_getid(ns), cmd->cap, rwbuf->iovec[0].offset, rwbuf->size);
  }

  //**  If coalesced ops then free the coalesced mallocs
//  if (cmd->n_ops > 1) {
//     free(cmd->rwbuf);
//  }

  return(err);
}

//=============================================================
//  Write routines
//=============================================================

//*************************************************************
// set_ibp_write_op - Generates a new write operation
//*************************************************************

void set_ibp_write_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
   set_ibp_rw_op(op, IBP_WRITE, cap, offset, buffer, bpos, len, timeout);
}

//*************************************************************
// new_ibp_write_op - Creates/Generates a new write operation
//*************************************************************

op_generic_t *new_ibp_write_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
   op_generic_t *gop = new_ibp_rw_op(ic, IBP_WRITE, cap, offset, buffer, bpos, len, timeout);
//   ibp_op_t *iop = ibp_get_iop(gop);

//log_printf(15, "new_ibp_write_op: gid=%d next_block=%p\n", gop_id(gop), iop->rw_op.next_block); flush_log();
   return(gop);
}

//*************************************************************
// set_ibp_vec_write_op - Generates a new vec write operation
//*************************************************************

void set_ibp_vec_write_op(ibp_op_t *op, ibp_cap_t *cap, int n_iovec, ibp_iovec_t *iovec, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
  op_generic_t *gop = ibp_get_gop(op);

  set_ibp_rw_op(op, IBP_WRITE, cap, 0, buffer, bpos, len, timeout);
  op->rw_op.n_ops = 1;
  op->rw_op.n_iovec_total = n_iovec;
  op->rw_op.buf_single.n_iovec = n_iovec;
  op->rw_op.buf_single.iovec = iovec;

  gop->op->cmd.send_command = vec_write_command;

}

//*************************************************************
// new_ibp_vec_write_op - Creates/Generates a new vec write operation
//*************************************************************

op_generic_t *new_ibp_vec_write_op(ibp_context_t *ic, ibp_cap_t *cap, int n_iovec, ibp_iovec_t *iovec, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_vec_write_op(op, cap, n_iovec, iovec, buffer, bpos, len, timeout);

  return(ibp_get_gop(op));
}

//*************************************************************

op_status_t vec_write_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int bufsize = 204800;
  char stackbuffer[bufsize];
  char *buffer = stackbuffer;
  op_status_t err;
  int i,j,used;
  ibp_op_rw_t *cmd;
  ibp_rw_buf_t *rwbuf;

  cmd = &(op->rw_op);

  used = 0;

  //** Store base command
  if (ns_chksum_is_valid(&(op->ncs)) == 0) {
     append_printf(buffer, &used, bufsize, "%d %d %s %s %d",
          IBPv040, IBP_VEC_WRITE, cmd->key, cmd->typekey, cmd->n_iovec_total);
  } else {
     append_printf(buffer, &used, bufsize, "%d %d %d %d %s %s %d", 
          IBPv040, IBP_VEC_WRITE_CHKSUM, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
          cmd->key, cmd->typekey, cmd->n_iovec_total);
  }


  //** Add the IO vec list
  for (j=0; j<cmd->n_ops; j++) {
     rwbuf = cmd->rwbuf[j];
     for (i=0; i<rwbuf->n_iovec; i++) {
        if (used >= (bufsize-100)) {
           bufsize = bufsize * 1.5;
           if (buffer == stackbuffer) {
              buffer = (char *)malloc(bufsize);
              memcpy(buffer, stackbuffer, used);
           } else {
             buffer = (char *)realloc(buffer, bufsize);
           }
        }
        append_printf(buffer, &used, bufsize, " " I64T " " I64T, rwbuf->iovec[i].offset, rwbuf->iovec[i].len);
     }
  }

  //** Add the timeout
  append_printf(buffer, &used, bufsize, " %d\n", (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_set(ns, op->ncs);
  ns_write_chksum_disable(ns);

//log_printf(15, "vec_write_command: sending command ns=%d\n", ns_getid(ns));

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "write_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

//log_printf(0, "write_command: END gid=%d next_block=%p\n", gop_id(gop), cmd->next_block);

  if (buffer != stackbuffer) free(buffer);

  return(err);
}

//*************************************************************

op_status_t write_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_rw_t *cmd;

  cmd = &(op->rw_op);

//  log_printf(10, "write_command: gid=%d next_block=%p\n", gop_id(gop), cmd->next_block);

  if (ns_chksum_is_valid(&(op->ncs)) == 0) {
     snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d\n",
          IBPv040, IBP_WRITE, cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else {
     snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " " I64T " %d\n",
          IBPv040, IBP_WRITE_CHKSUM, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
          cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
  }

  ns_write_chksum_set(ns, op->ncs);
  ns_write_chksum_disable(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "write_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

//log_printf(0, "write_command: END gid=%d next_block=%p\n", gop_id(gop), cmd->next_block);

  return(err);
}

//*************************************************************

op_status_t gop_write_block(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, ibp_off_t pos, ibp_off_t size)
{
  int nbytes, state, bpos, nleft;
  op_status_t status;
  Net_timeout_t dt;
  apr_time_t end_time;

  state = 0;
  set_net_timeout(&dt, 1, 0);
  end_time = gop_get_end_time(gop, &state);

  nbytes = 0;
  bpos = pos;
  nleft = size;
  while ((nbytes != -1) && (nleft > 0) && (apr_time_now() < end_time)) {
     nbytes = write_netstream(ns, buffer, bpos, nleft, dt);
     if (nbytes != -1) {
        bpos += nbytes;
        nleft -= nbytes;
     }

     end_time = gop_get_end_time(gop, &state);
  }

  if (nleft == 0) {
     status = ibp_success_status;
  } else if (apr_time_now() > end_time) {
log_printf(5, "gid=%d timeout! now=" TT " end=" TT " state=%d\n", gop_id(gop), apr_time_now(), end_time, state);
     status = ibp_timeout_status;
  } else {
log_printf(5, "gid=%d timeout! RETRY\n", gop_id(gop));
     status = ibp_retry_status;  //** Dead connection so retry
  }

  return(status);
}

//*************************************************************

op_status_t write_send(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *iop = ibp_get_iop(gop);
  int i;
  op_status_t err;
  ibp_op_rw_t *cmd = &(iop->rw_op);
  ibp_rw_buf_t *rwbuf;

//  log_printf(10, "write_send: START gid=%d next_block=%p\n", gop_id(gop), cmd->next_block);

  err = ibp_failure_status;

  //** Turn on chksumming if needed
  if (ns_chksum_is_valid(&(iop->ncs)) == 1) {
     log_printf(15, "write_send: ns=%d chksum_type=%d\n", ns_getid(ns), ns_chksum_type(&(iop->ncs)));
     ns_chksum_reset(&(iop->ncs));
     ns_write_chksum_set(ns, iop->ncs);
     ns_write_chksum_enable(ns);
  }

  log_printf(10, "write_send: gid=%d n_ops=%d\n", gop_id(gop), cmd->n_ops);

  for (i=0; i<cmd->n_ops; i++) {
    rwbuf = cmd->rwbuf[i];
  log_printf(5, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), ns_getid(ns), i, rwbuf->size);
    err = gop_write_block(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
  log_printf(5, "gid=%d ns=%d i=%d status=%d\n", gop_id(gop), ns_getid(ns), i, err.op_status);
    if (err.op_status != OP_STATE_SUCCESS) break;
  }

  log_printf(15, "write_send: END ns=%d status=%d\n", ns_getid(ns), err.op_status);

  if ((ns_chksum_is_valid(&(iop->ncs)) == 1) && (err.op_status == OP_STATE_SUCCESS)) {
     if (ns_write_chksum_flush(ns) != 0) { err.op_status = OP_STATE_FAILURE; err.error_code = IBP_E_CHKSUM; }
     ns_write_chksum_disable(ns);
  }

  return(err);
}

//*************************************************************

op_status_t write_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  int status, fin;
  ibp_off_t nbytes;
  ibp_op_rw_t *cmd;
  char *bstate;

  log_printf(15, "write_recv: Start!!! ns=%d\n", ns_getid(ns));

  cmd = &(op->rw_op);

  ns_read_chksum_disable(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  status = -1;
  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if (status != IBP_OK) {
    log_printf(15, "write_recv: ns=%d id=%d cap=%s n_ops=%d  Error!  status=%s\n",
       ns_getid(ns), gop_get_id(gop), cmd->cap, cmd->n_ops, buffer);
    process_error(gop, &err, status, -1, &bstate);
  } else {
    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status == OP_STATE_SUCCESS) {
      log_printf(15, "write_recv: ns=%d cap=%s gid=%d n_ops=%d status/nbytes=%s\n",
             ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
       status = -1; nbytes = -1;
       status = atoi(string_token(buffer, " ", &bstate, &fin));
       sscanf(string_token(NULL, " ", &bstate, &fin), I64T, &nbytes);
//       sscanf(buffer, "%d %d\n", &status, &nbytes);
       if ((nbytes != cmd->size) || (status != IBP_OK)) {
          log_printf(15, "write_recv: ns=%d cap=%s gid=%d n_ops=%d Error! status/nbytes=%s\n",
             ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
          err.op_status = OP_STATE_FAILURE; err.error_code = status;
       } else {
         err = ibp_success_status;
       }
    } else {
       log_printf(15, "write_recv: ns=%d cap=%s gid=%d n_ops=%d Error withreadline! buffer=%s\n",
          ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
       //**  If coalesced ops then free the coalesced mallocs
//       if (cmd->n_ops > 1) free(cmd->rwbuf);

        return(err);
    }
  }

  //**  If coalesced ops then free the coalesced mallocs
//  if ((cmd->n_ops > 1) && (err.op_status != OP_STATE_RETRY)) free(cmd->rwbuf);

  return(err);
}

//=============================================================
//  IBP append routines
//=============================================================

//*************************************************************

op_status_t append_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_rw_t *cmd;

  cmd = &(op->rw_op);

 if (ns_chksum_is_valid(&(op->ncs)) == 0) {
//     ns_chksum_reset(&(op->ncs));
     snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " %d\n",
          IBPv040, IBP_STORE, cmd->key, cmd->typekey, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else {
     snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " %d\n",
          IBPv040, IBP_STORE_CHKSUM, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
          cmd->key, cmd->typekey, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  }

  ns_write_chksum_set(ns, op->ncs);
  ns_write_chksum_disable(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "append_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}


//*************************************************************
// new_ibp_append_op - Creates/Generates a new write operation
//*************************************************************

op_generic_t *new_ibp_append_op(ibp_context_t *ic, ibp_cap_t *cap, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
   op_generic_t *gop = new_ibp_rw_op(ic, IBP_STORE, cap, 0, buffer, bpos, len, timeout);
   if (gop == NULL) return(NULL);

   gop->op->cmd.send_command = append_command;
   return(gop);
}

//*************************************************************
// set_ibp_append_op - Generates a new write operation
//*************************************************************

void set_ibp_append_op(ibp_op_t *op, ibp_cap_t *cap, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
   //** Dirty way to fill in the fields
   set_ibp_rw_op(op, IBP_WRITE, cap, 0, buffer, bpos, len, timeout);
   op_generic_t *gop = ibp_get_gop(op);

   gop->op->cmd.send_command = append_command;
   gop->op->cmd.send_phase = write_send;
   gop->op->cmd.recv_phase = write_recv;
}

//=============================================================
//=============================================================

//*************************************************************
// set_ibp_rw_op - Generates a new IO operation
//*************************************************************

void set_ibp_rw_op(ibp_op_t *op, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_rw_t *cmd;
  ibp_rw_buf_t *rwbuf;

  cmd = &(op->rw_op);

  init_ibp_base_op(op, "rw", timeout, op->ic->rw_new_command + len, NULL, len, rw_type, IBP_NOP);
  op_generic_t *gop = ibp_get_gop(op);


  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[rw_type]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->cap = cap;
  cmd->size = len; //** This is the total size

  rwbuf = &(cmd->buf_single);
  cmd->bs_ptr = rwbuf;
  cmd->rwbuf = &(cmd->bs_ptr);
  cmd->n_ops = 1;
  cmd->n_iovec_total = 1;
  cmd->rw_mode = rw_type;

  rwbuf->iovec = &(rwbuf->iovec_single);

  rwbuf->n_iovec = 1;
  rwbuf->iovec->offset = offset;
  rwbuf->iovec->len = len;
  rwbuf->buffer = buffer;
  rwbuf->boff = bpos;
  rwbuf->size = len;

  if (rw_type == IBP_WRITE) { 
     gop->op->cmd.send_command = write_command;
     gop->op->cmd.send_phase = write_send;
     gop->op->cmd.recv_phase = write_recv;
     gop->op->cmd.on_submit = ibp_rw_submit_coalesce;
     gop->op->cmd.before_exec = ibp_rw_coalesce;
  } else {
     gop->op->cmd.send_command = read_command;
     gop->op->cmd.send_phase = NULL;
     gop->op->cmd.recv_phase = read_recv;
     gop->op->cmd.on_submit = ibp_rw_submit_coalesce;
     gop->op->cmd.before_exec = ibp_rw_coalesce;
  }

  op->ncs = op->ic->ncs;  //** Copy the default network chksum
}

//*************************************************************
// new_ibp_rw_op - Creates/Generates a new IO operation
//*************************************************************

op_generic_t *new_ibp_rw_op(ibp_context_t *ic, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_rw_op(op, rw_type, cap, offset, buffer, bpos, len, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// IBP_VALIDATE_CHKSUM routines
//=============================================================

//*************************************************************

op_status_t validate_chksum_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_validate_chksum_t *cmd = &(op->validate_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d\n", 
      IBPv040, IBP_VALIDATE_CHKSUM, cmd->key, cmd->typekey, cmd->correct_errors, 
      (int)apr_time_sec(gop->op->cmd.timeout));
  
  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "validate_chksum_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t validate_chksum_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  int status, fin;
  ibp_off_t nerrors;
  ibp_op_validate_chksum_t *cmd = &(op->validate_op);
  char *bstate;
  double swait;

  log_printf(15, "validate_chksum_recv: Start!!! ns=%d\n", ns_getid(ns));

  ns_read_chksum_disable(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "validate_chksum_recv: ns=%d cap=%s status/n_errors=%s\n",
             ns_getid(ns), cmd->cap, buffer);

  //** Get the status and number of bad blocks(if available)
  status = -1; nerrors = -1;
  status = atoi(string_token(buffer, " ", &bstate, &fin));
  sscanf(string_token(NULL, " ", &bstate, &fin), "%lf", &swait); nerrors = swait;
  *cmd->n_bad_blocks = nerrors;

  return(process_error(gop, &err, status, swait, NULL));
}

//*************************************************************
//  set_ibp_validate_chksum_op - Generates a new IBP_VALIDATE_CHKSUM operation
//*************************************************************

void set_ibp_validate_chksum_op(ibp_op_t *op, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks,
       int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  ibp_op_validate_chksum_t *cmd;
  int port;

  init_ibp_base_op(op, "validate_chksum", timeout, op->ic->other_new_command, NULL, 1, IBP_VALIDATE_CHKSUM, IBP_NOP);
  op_generic_t *gop = ibp_get_gop(op);

  cmd = &(op->validate_op);

  parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_VALIDATE_CHKSUM]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd = &(op->validate_op);
  cmd->correct_errors = correct_errors;
  cmd->n_bad_blocks = n_bad_blocks;

  gop->op->cmd.send_command = validate_chksum_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = validate_chksum_recv;
}

//*************************************************************
//  new_ibp_validate_chksum_op - Creates a new IBP_VALIDATE_CHKSUM operation
//*************************************************************

op_generic_t *new_ibp_validate_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks, 
       int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_validate_chksum_op(op, mcap, correct_errors, n_bad_blocks, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// IBP_GET_CHKSUM routines
//=============================================================

//*************************************************************

op_status_t get_chksum_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_get_chksum_t *cmd = &(op->get_chksum_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d\n", 
      IBPv040, IBP_GET_CHKSUM, cmd->key, cmd->typekey, cmd->chksum_info_only, 
      (int)apr_time_sec(gop->op->cmd.timeout));
  
  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "get_chksum_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t get_chksum_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  int status, fin;
  ibp_op_get_chksum_t *cmd = &(op->get_chksum_op);
  char *bstate;
  tbuffer_t buf;

  log_printf(15, "get_chksum_recv: Start!!! ns=%d\n", ns_getid(ns));

  ns_read_chksum_disable(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "get_chksum_recv: ns=%d cap=%s status string=%s\n",
             ns_getid(ns), cmd->cap, buffer);

  //** Get the status
  status = -1;
  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if (status != IBP_OK) {
     log_printf(15, "get_chksum_recv: ns=%d cap=%s status error=%d!\n",  ns_getid(ns), cmd->cap, status);
     return(process_error(gop, &err, status, -1, &bstate));
  }

  //** Now get the chksum type
  *cmd->cs_type = CHKSUM_NONE;
  sscanf(string_token(NULL, " ", &bstate, &fin), "%d", cmd->cs_type);

//    status cs_type cs_size block_size nblocks nbytes\n
//    ...nbytes_of_chksum...

  //** Now parse the rest of the chksum info line
  sscanf(string_token(NULL, " ", &bstate, &fin), "%d", cmd->cs_size);
  sscanf(string_token(NULL, " ", &bstate, &fin), I64T, cmd->blocksize);
  sscanf(string_token(NULL, " ", &bstate, &fin), I64T, cmd->nblocks);
  sscanf(string_token(NULL, " ", &bstate, &fin), I64T, cmd->n_chksumbytes);

//log_printf(0, "get_chksum_recv: type=%d size=%d bs=" I64T " nb=" I64T " nbytes=" I64T "\n", 
//  *cmd->cs_type, *cmd->cs_size, *cmd->blocksize, *cmd->nblocks, *cmd->n_chksumbytes);

  if (cmd->chksum_info_only == 1) {  //** Only wanted the chksum info so return
    return(ibp_success_status);
  }

  //** Check and make sure the buffer is large enough
  if (*cmd->n_chksumbytes > cmd->bufsize) {
     log_printf(15, "get_chksum_recv: ns=%d cap=%s buffer too small!  bufsize=" I64T " need= " I64T "\n",  ns_getid(ns), cmd->cap, cmd->bufsize, *cmd->n_chksumbytes);    
     close_netstream(ns);
     _op_set_status(err, OP_STATE_FAILURE, IBP_E_WOULD_EXCEED_LIMIT);
     return(err);
  }

  //** Finally read in the chksum
  tbuffer_single(&buf, *cmd->n_chksumbytes, cmd->buffer);
  err = gop_read_block(ns, gop, &buf, 0, *cmd->n_chksumbytes);

  return(err);
}

//*************************************************************
//  set_ibp_get_chksum_op - Generates a new IBP_VALIDATE_CHKSUM operation
//*************************************************************


void set_ibp_get_chksum_op(ibp_op_t *op, ibp_cap_t *mcap, int chksum_info_only,
       int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
       int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  ibp_op_get_chksum_t *cmd;
  int port;

  init_ibp_base_op(op, "get_chksum", timeout, op->ic->other_new_command, NULL, 1, IBP_VALIDATE_CHKSUM, IBP_NOP);
  op_generic_t *gop = ibp_get_gop(op);

  cmd = &(op->get_chksum_op);

  parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_VALIDATE_CHKSUM]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->cap = mcap;
  cmd = &(op->get_chksum_op);
  cmd->chksum_info_only = chksum_info_only;
  cmd->cs_type = cs_type;
  cmd->cs_size = cs_size;
  cmd->blocksize = blocksize;
  cmd->nblocks = nblocks;
  cmd->n_chksumbytes = n_chksumbytes;

  gop->op->cmd.send_command = get_chksum_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = get_chksum_recv;
}

//*************************************************************
//  new_ibp_get_chksum_op - Creates a new IBP_GET_CHKSUM operation
//*************************************************************

op_generic_t *new_ibp_get_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap, int chksum_info_only,
       int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *nbytes, char *buffer, ibp_off_t bufsize,
       int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_get_chksum_op(op, mcap, chksum_info_only, cs_type, cs_size, blocksize, nblocks, nbytes, buffer, bufsize, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  Allocate routines
//=============================================================

//*************************************************************

op_status_t allocate_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
//  apr_time_t atime, now;
  op_status_t err;
  ibp_op_alloc_t *cmd = &(op->alloc_op);

log_printf(10, "allocate_command: cs_type=%d\n", cmd->disk_chksum_type);

//apr_time_t dur = cmd->duration;
//log_printf(0, "allocate_command: cs_type=%d duration=" TT "\n", cmd->disk_chksum_type, cmd->duration);

  if (cmd->disk_chksum_type == CHKSUM_DEFAULT) {  //** Normal allocation
     snprintf(buffer, sizeof(buffer), "%d %d %s %d %d %d " I64T " %d\n",
          IBPv040, IBP_ALLOCATE, cmd->depot->rid.name, cmd->attr->reliability, cmd->attr->type,
          cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else if ((chksum_valid_type(cmd->disk_chksum_type) == 1) || (cmd->disk_chksum_type == CHKSUM_NONE)) {
     snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %d %d %d " I64T " %d\n",
          IBPv040, IBP_ALLOCATE_CHKSUM, cmd->disk_chksum_type, cmd->disk_blocksize, cmd->depot->rid.name, cmd->attr->reliability, cmd->attr->type,
          cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else {
    log_printf(10, "allocate_command: Invalid chksum type! type=%d ns=%d\n", cmd->disk_chksum_type, ns_getid(ns));
    _op_set_status(err, OP_STATE_FAILURE, IBP_E_CHKSUM_TYPE);
    return(err);
  }

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "allocate_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t split_allocate_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_alloc_t *cmd = &(op->alloc_op);

  if (cmd->disk_chksum_type == CHKSUM_DEFAULT) {  //** Normal split allocation
      snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d " I64T " %d\n",
          IBPv040, IBP_SPLIT_ALLOCATE, cmd->key, cmd->typekey, cmd->attr->reliability, cmd->attr->type,
          cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else if ((chksum_valid_type(cmd->disk_chksum_type) == 1) || (cmd->disk_chksum_type == CHKSUM_NONE)) {
      snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s %d %d %d " I64T " %d\n", 
          IBPv040, IBP_SPLIT_ALLOCATE_CHKSUM, cmd->disk_chksum_type, cmd->disk_blocksize, cmd->key, cmd->typekey, cmd->attr->reliability, cmd->attr->type, 
          cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
  } else {
    log_printf(10, "split_allocate_command: Invalid chksum type! type=%d ns=%d\n", cmd->disk_chksum_type, ns_getid(ns));
    _op_set_status(err, OP_STATE_FAILURE, IBP_E_CHKSUM_TYPE);
    return(err);
  }
  
  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "split_allocate_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t allocate_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int status, fin;
  char buffer[1025];
  char rcap[1025], wcap[1025], mcap[1025];
  char *bstate;
  op_status_t err;
  ibp_op_alloc_t *cmd = &(op->alloc_op);

  //** Need to read the depot status info
  log_printf(15, "allocate_recv: ns=%d Start\n", ns_getid(ns));

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "allocate_recv: after readline ns=%d buffer=%s\n", ns_getid(ns), buffer);

  sscanf(string_token(buffer, " ", &bstate, &fin), "%d", &status);
  if (status != IBP_OK) {
     log_printf(1, "alloc_recv: ns=%d Error! status=%d bstate=%s\n", ns_getid(ns), status, bstate);
     return(process_error(gop, &err, status, -1, &bstate));
  }        

  rcap[0] = '\0';
  wcap[0] = '\0';
  mcap[0] = '\0';
  strncpy(rcap, string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1); rcap[sizeof(rcap)-1] = '\0';
  strncpy(wcap, string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1); wcap[sizeof(wcap)-1] = '\0';
  strncpy(mcap, string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1); mcap[sizeof(mcap)-1] = '\0';
  
  if ((strlen(rcap) == 0) || (strlen(wcap) == 0) || (strlen(mcap) == 0)) {
     log_printf(0, "alloc_recv: ns=%d Error reading caps!  buffer=%s\n", ns_getid(ns), buffer);
     if (sscanf(buffer, "%d", &status) != 1) {
        log_printf(1, "alloc_recv: ns=%d Can't read status!\n", ns_getid(ns));
        _op_set_status(err, OP_STATE_FAILURE, IBP_E_GENERIC);
        return(err);
     } else {
        _op_set_status(err, OP_STATE_FAILURE, status);
        return(err);
     }
  }        

  cmd->caps->readCap = strdup(rcap);
  cmd->caps->writeCap = strdup(wcap);
  cmd->caps->manageCap = strdup(mcap);

  log_printf(15, "alloc_recv: ns=%d rcap=%s wcap=%s mcap=%s\n", ns_getid(ns),
       cmd->caps->readCap, cmd->caps->writeCap, cmd->caps->manageCap);

  return(ibp_success_status);
}

//*************************************************************
//  set_ibp_alloc_op - generates a new IBP_ALLOC operation
//*************************************************************

void set_ibp_alloc_op(ibp_op_t *op, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot,
       ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char pchost[MAX_HOST_SIZE];
  ibp_op_alloc_t *cmd;

//log_printf(15, "set_ibp_alloc_op: start. _hpc_config=%p\n", op->ic->hpc);

  ibppc_form_host(op->ic, pchost, sizeof(pchost), depot->host, depot->rid);
  set_hostport(hoststr, sizeof(hoststr), pchost, depot->port, &(op->ic->cc[IBP_ALLOCATE]));

//log_printf(15, "set_ibp_alloc_op: before init_ibp_base_op\n");

  init_ibp_base_op(op, "alloc", timeout, op->ic->other_new_command, strdup(hoststr), 1, IBP_ALLOCATE, IBP_NOP);
//log_printf(15, "set_ibp_alloc_op: after init_ibp_base_op\n");

  cmd = &(op->alloc_op);
  cmd->caps = caps;
  cmd->depot = depot;
  cmd->attr = attr;

  cmd->duration = cmd->attr->duration - time(NULL);  //** This is in sec NOT APR time
  if (cmd->duration < 0) cmd->duration = cmd->attr->duration;

  cmd->size = size;
  cmd->disk_chksum_type = disk_cs_type;
  cmd->disk_blocksize = disk_blocksize;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = allocate_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = allocate_recv;
}

//*************************************************************
//  new_ibp_alloc_op - Creates a new IBP_ALLOC operation
//*************************************************************

op_generic_t *new_ibp_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot, ibp_attributes_t *attr, 
       int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_alloc_op(op, caps, size, depot, attr, disk_cs_type, disk_blocksize, timeout);

  return(ibp_get_gop(op));
}

//*************************************************************
//  set_ibp_split_alloc_op - generates a new IBP_SPLIT_ALLOCATION operation
//*************************************************************

void set_ibp_split_alloc_op(ibp_op_t *op, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size, 
       ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  ibp_op_alloc_t *cmd;
  int port;

  init_ibp_base_op(op, "split_allocate", timeout, op->ic->other_new_command, NULL, 1, IBP_SPLIT_ALLOCATE, IBP_NOP);

  cmd = &(op->alloc_op);

  parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SPLIT_ALLOCATE]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd = &(op->alloc_op);
  cmd->caps = caps;
  cmd->attr = attr;

  cmd->duration = cmd->attr->duration - time(NULL);  //** This is in sec NOT APR time
  if (cmd->duration < 0) cmd->duration = cmd->attr->duration;

  cmd->size = size;
  cmd->disk_chksum_type = disk_cs_type;
  cmd->disk_blocksize = disk_blocksize;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = split_allocate_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = allocate_recv;
}

//*************************************************************
//  new_ibp_split_alloc_op - generates a new IBP_SPLIT_ALLOCATION operation
//*************************************************************

op_generic_t *new_ibp_split_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size,
       ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_split_alloc_op(op, mcap, caps, size, attr, disk_cs_type, disk_blocksize, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  Rename routines
//=============================================================

//*************************************************************

op_status_t rename_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_alloc_t *cmd = &(op->alloc_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d\n", 
       IBPv040, IBP_RENAME, cmd->key, cmd->typekey, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "rename_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
//  set_ibp_rename_op - generates a new IBP_RENAME operation
//*************************************************************

void set_ibp_rename_op(ibp_op_t *op, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  ibp_op_alloc_t *cmd;
  int port;

log_printf(15, "set_ibp_rename_op: start. ic=%p\n", op->ic);

  init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_RENAME, IBP_NOP);

  cmd = &(op->alloc_op);

  parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_RENAME]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->caps = caps;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = rename_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = allocate_recv;
}

//*************************************************************
//  new_ibp_rename_op - Creates a new IBP_RENAME operation
//*************************************************************

op_generic_t *new_ibp_rename_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_rename_op(op, caps, mcap, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  Merge routines
//=============================================================

//*************************************************************

op_status_t merge_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_merge_alloc_t *cmd = &(op->merge_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %s %s %d\n", 
       IBPv040, IBP_MERGE_ALLOCATE, cmd->mkey, cmd->mtypekey, cmd->ckey, cmd->ctypekey, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "merge_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
//  set_ibp_merge_alloc_op - generates a new IBP_MERGE_ALLOCATE operation
//*************************************************************

void set_ibp_merge_alloc_op(ibp_op_t *op, ibp_cap_t *mcap, ibp_cap_t *ccap, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  char chost[MAX_HOST_SIZE];
  ibp_op_merge_alloc_t *cmd;
  int port, cport;

  log_printf(15, "set_ibp_merge_op: start. ic=%p\n", op->ic);

  init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_RENAME, IBP_NOP);

  cmd = &(op->merge_op);

  parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MERGE_ALLOCATE]));
  op->dop.cmd.hostport = strdup(hoststr);

  parse_cap(op->ic, ccap, chost, &cport, cmd->ckey, cmd->ctypekey);

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = merge_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;
}

//*************************************************************
//  new_ibp_merge_alloc_op - Creates a new IBP_MERGE_ALLOCATION operation
//*************************************************************

op_generic_t *new_ibp_merge_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_cap_t *ccap, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_merge_alloc_op(op, mcap, ccap, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  Alias Allocate routines
//=============================================================

//*************************************************************

op_status_t alias_allocate_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_alloc_t *cmd = &(op->alloc_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d %d\n",
       IBPv040, IBP_ALIAS_ALLOCATE, cmd->key, cmd->typekey, cmd->offset, cmd->size, cmd->duration, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "alias_allocate_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
//  set_ibp_alias_alloc_op - generates a new IBP_ALIAS_ALLOC operation
//*************************************************************

void set_ibp_alias_alloc_op(ibp_op_t *op, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, 
   int duration, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char host[MAX_HOST_SIZE];
  ibp_op_alloc_t *cmd;
  int port;

  log_printf(15, "set_ibp_alias_alloc_op: start. ic=%p\n", op->ic);

  init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_ALLOCATE, IBP_NOP);

  cmd = &(op->alloc_op);

  parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_ALLOCATE]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->offset = offset;
  cmd->size = size;
  if (duration == 0) {
     cmd->duration = 0;
  } else {
    cmd->duration = duration - time(NULL); //** This is in sec NOT APR time
  }


  cmd->caps = caps;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = alias_allocate_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = allocate_recv;
}

//*************************************************************
//  new_ibp_alias_alloc_op - Creates a new IBP_ALIAS_ALLOC operation
//*************************************************************

op_generic_t *new_ibp_alias_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, 
   int duration, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  
  set_ibp_alias_alloc_op(op, caps, mcap, offset, size, duration, timeout);

  return(ibp_get_gop(op));
}


//=============================================================
//  modify_count routines
//=============================================================

//*************************************************************

op_status_t modify_count_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_probe_t *cmd;

  cmd = &(op->probe_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d\n", 
       IBPv040, cmd->cmd, cmd->key, cmd->typekey, cmd->mode, cmd->captype, (int)apr_time_sec(gop->op->cmd.timeout));

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t alias_modify_count_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_probe_t *cmd;

  cmd = &(op->probe_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %s %s %d\n", 
       IBPv040, cmd->cmd, cmd->key, cmd->typekey, cmd->mode, cmd->captype, cmd->mkey, cmd->mtypekey, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t status_get_recv(op_generic_t *gop, NetStream_t *ns)
{
  int status, fin;
  char buffer[1025];
  char *bstate;
  op_status_t err;

  //** Need to read the depot status info
  log_printf(15, "status_get_recv: ns=%d Start", ns->id);

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "status_get_recv: after readline ns=%d buffer=%s\n", ns_getid(ns), buffer);

  status = IBP_E_GENERIC;
  status = atoi(string_token(buffer, " ", &bstate, &fin));

  return(process_error(gop, &err, status, -1, &bstate));
}

//*************************************************************
//  set_ibp_generic_modify_count_op - Generates an operation to modify
//     an allocations reference count
//*************************************************************

void set_ibp_generic_modify_count_op(int command, ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_probe_t *cmd;

  if ((command != IBP_MANAGE) && (command != IBP_ALIAS_MANAGE)) {
     log_printf(0, "set_ibp_generic_modify_count_op: Invalid command! should be IBP_MANAGE or IBP_ALIAS_MANAGE.  Got %d\n", command);
     return;
  }  
  if ((mode != IBP_INCR) && (mode != IBP_DECR)) {
     log_printf(0, "new_ibp_modify_count_op: Invalid mode! should be IBP_INCR or IBP_DECR\n");
     return;
  }
  if ((captype != IBP_WRITECAP) && (captype != IBP_READCAP)) {
     log_printf(0, "new_ibp_modify_count_op: Invalid captype! should be IBP_READCAP or IBP_WRITECAP\n");
     return;
  }

  init_ibp_base_op(op, "modify_count", timeout, op->ic->other_new_command, NULL, 1, command, mode);

  cmd = &(op->probe_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[command]));
  op->dop.cmd.hostport = strdup(hoststr);

  if (command == IBP_ALIAS_MANAGE) parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);

  cmd->cmd = command;
  cmd->cap = cap;
  cmd->mode = mode;
  cmd->captype = captype;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = modify_count_command;
  if (command == IBP_ALIAS_MANAGE) gop->op->cmd.send_command = alias_modify_count_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;
}

//*************************************************************
//  *_ibp_modify_count_op - Generates an operation to modify 
//     an allocations reference count
//*************************************************************

void set_ibp_modify_count_op(ibp_op_t *op, ibp_cap_t *cap, int mode, int captype, int timeout)
{
  set_ibp_generic_modify_count_op(IBP_MANAGE, op, cap, NULL, mode, captype, timeout);
}

//***************************

op_generic_t *new_ibp_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, int mode, int captype, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_generic_modify_count_op(IBP_MANAGE, op, cap, NULL, mode, captype, timeout);

  return(ibp_get_gop(op));
}

//*************************************************************
//  *_ibp_alias_modify_count_op - Generates an operation to modify 
//     a ALIAS allocations reference count
//*************************************************************

void set_ibp_alias_modify_count_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout)
{
  set_ibp_generic_modify_count_op(IBP_ALIAS_MANAGE, op, cap, mcap, mode, captype, timeout);
}

//***************************

op_generic_t *new_ibp_alias_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_generic_modify_count_op(IBP_ALIAS_MANAGE, op, cap, mcap, mode, captype, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// Modify allocation routines
//=============================================================

//*************************************************************

op_status_t modify_alloc_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  int atime;
  ibp_op_modify_alloc_t *cmd;

  cmd = &(op->mod_alloc_op);

  atime = cmd->duration - time(NULL); //** This is in sec NOT APR time
  if (atime < 0) atime = cmd->duration;

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d " I64T " %d %d %d\n", 
       IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_CHNG, IBP_MANAGECAP, cmd->size, atime, 
       cmd->reliability, (int)apr_time_sec(gop->op->cmd.timeout));

//  log_printf(0, "modify_alloc_command: buffer=!%s!\n", buffer);

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
// set_ibp_modify_alloc_op - Modifes the size, duration, and 
//   reliability of an existing allocation.
//*************************************************************

void set_ibp_modify_alloc_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, 
     int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_modify_alloc_t *cmd;
  
  init_ibp_base_op(op, "modify_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_CHNG);

  cmd = &(op->mod_alloc_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->cap = cap;
  cmd->size = size;
  cmd->duration = duration;
  cmd->reliability = reliability;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = modify_alloc_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;
  
}

//*************************************************************

op_generic_t *new_ibp_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_modify_alloc_op(op, cap, size, duration, reliability, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// Alias Modify allocation routines
//=============================================================

//*************************************************************

op_status_t alias_modify_alloc_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  int atime;
  ibp_op_modify_alloc_t *cmd;

  cmd = &(op->mod_alloc_op);

  atime = cmd->duration - time(NULL); //** This is in sec NOT APR time
  if (atime < 0) atime = cmd->duration;

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d " I64T " " I64T " %d %s %s %d\n",
       IBPv040, IBP_ALIAS_MANAGE, cmd->key, cmd->typekey, IBP_CHNG, cmd->offset,  cmd->size, atime,
       cmd->mkey, cmd->mtypekey, (int)apr_time_sec(gop->op->cmd.timeout));

//  log_printf(0, "alias_modify_alloc_command: buffer=!%s!\n", buffer);

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "alias_modify_count_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
// set_ibp_alias_modify_alloc_op - Modifes the size, duration, and
//   reliability of an existing allocation.
//*************************************************************

void set_ibp_alias_modify_alloc_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration,
     int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_modify_alloc_t *cmd;

  init_ibp_base_op(op, "alias_modify_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_MANAGE, IBP_CHNG);

  cmd = &(op->mod_alloc_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_MANAGE]));
  op->dop.cmd.hostport = strdup(hoststr);

  parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);

  cmd->cap = cap;
  cmd->offset = offset;
  cmd->size = size;
  cmd->duration = duration;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = alias_modify_alloc_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;

}

//*************************************************************

op_generic_t *new_ibp_alias_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_alias_modify_alloc_op(op, cap, mcap, offset, size, duration, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// IBP_TRUNCATE routines
//=============================================================

//*************************************************************

op_status_t truncate_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_modify_alloc_t *cmd;

  cmd = &(op->mod_alloc_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d " I64T " %d\n",
       IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_TRUNCATE, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));

//  log_printf(0, "truncate__command: buffer=!%s!\n", buffer);

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "truncate_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
// set_ibp_truncate_op - Modifes the size, duration, and 
//   reliability of an existing allocation.
//*************************************************************

void set_ibp_truncate_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t size, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_modify_alloc_t *cmd;
  
  init_ibp_base_op(op, "truncate_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_CHNG);

  cmd = &(op->mod_alloc_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->cap = cap;
  cmd->size = size;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = truncate_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;
  
}

//*************************************************************

op_generic_t *new_ibp_truncate_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_truncate_op(op, cap, size, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  Remove routines
//=============================================================

//*************************************************************
//  set_ibp_remove_op - Generates a remove allocation operation
//*************************************************************

void set_ibp_remove_op(ibp_op_t *op, ibp_cap_t *cap, int timeout)
{
  set_ibp_modify_count_op(op, cap, IBP_DECR, IBP_READCAP, timeout);
}

//*************************************************************
//  new_ibp_remove_op - Generates/Creates a remove allocation operation
//*************************************************************

op_generic_t *new_ibp_remove_op(ibp_context_t *ic, ibp_cap_t *cap, int timeout)
{
  return(new_ibp_modify_count_op(ic, cap, IBP_DECR, IBP_READCAP, timeout));
}

//*************************************************************
//  set_ibp_alias_remove_op - Generates a remove alias allocation operation
//*************************************************************

void set_ibp_alias_remove_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout)
{
  set_ibp_alias_modify_count_op(op, cap, mcap, IBP_DECR, IBP_READCAP, timeout);
}

//*************************************************************
//  new_ibp_alias_remove_op - Generates/Creates a remove alias allocation operation
//*************************************************************

op_generic_t *new_ibp_alias_remove_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout)
{
  return(new_ibp_alias_modify_count_op(ic, cap, mcap, IBP_DECR, IBP_READCAP, timeout));
}

//=============================================================
//  IBP_PROBE routines for IBP_MANAGE
//=============================================================

//*************************************************************

op_status_t probe_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_probe_t *cmd;

  cmd = &(op->probe_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d 0 0 0 %d \n", 
       IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_PROBE, IBP_MANAGECAP, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "probe_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t probe_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int status, fin;
  char buffer[1025];
  op_status_t err;
  char *bstate;
  ibp_capstatus_t *p;

  //** Need to read the depot status info
  log_printf(15, "probe_recv: ns=%d Start", ns->id);

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "probe_recv: after readline ns=%d buffer=%s\n", ns_getid(ns), buffer);

  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if ((status == IBP_OK) && (fin == 0)) {
     p = op->probe_op.probe;
log_printf(15, "probe_recv: p=%p QWERT\n", p);

     p->readRefCount = atoi(string_token(NULL, " ", &bstate, &fin));
     p->writeRefCount = atoi(string_token(NULL, " ", &bstate, &fin));
     sscanf(string_token(NULL, " ", &bstate, &fin), I64T, &(p->currentSize));
     sscanf(string_token(NULL, " ", &bstate, &fin), I64T, &(p->maxSize));
     p->attrib.duration = atol(string_token(NULL, " ", &bstate, &fin)) + time(NULL); //** This is in sec NOT APR time
     p->attrib.reliability = atoi(string_token(NULL, " ", &bstate, &fin));
     p->attrib.type = atoi(string_token(NULL, " ", &bstate, &fin));
  } else {
     process_error(gop, &err, status, -1, &bstate);
  }

  return(err);
}

//*************************************************************
//  set_ibp_probe_op - Generates a new IBP_PROBE command to get
//     information about an existing allocation
//*************************************************************

void set_ibp_probe_op(ibp_op_t *op, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_probe_t *cmd;

log_printf(15, " cctype=%d\n", op->ic->cc[IBP_MANAGE].type);

  init_ibp_base_op(op, "probe", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_PROBE);

log_printf(15, "AFTER cctype=%d\n", op->ic->cc[IBP_MANAGE].type); flush_log();

  cmd = &(op->probe_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
  op->dop.cmd.hostport = strdup(hoststr);

log_printf(15, "set_ibp_probe_op: p=%p QWERT\n", probe);
  cmd->cap = cap;
  cmd->probe = probe;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = probe_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = probe_recv;
}

//*************************************************************
//  new_ibp_probe_op - Creats and generates  a new IBP_PROBE
//     command to get information about an existing allocation
//*************************************************************

op_generic_t *new_ibp_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_probe_op(op, cap, probe, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  IBP_PROBE routines of IBP_ALIAS_MANAGE
//=============================================================

//*************************************************************

op_status_t alias_probe_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_probe_t *cmd;

  cmd = &(op->probe_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d \n",
       IBPv040, IBP_ALIAS_MANAGE, cmd->key, cmd->typekey, IBP_PROBE, IBP_MANAGECAP, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "alias_probe_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t alias_probe_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int status, fin;
  char buffer[1025];
  op_status_t err;
  char *bstate;
  ibp_alias_capstatus_t *p;

  //** Need to read the depot status info
  log_printf(15, "alias_probe_recv: ns=%d Start", ns->id);

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "alias_probe_recv: after readline ns=%d buffer=%s\n", ns_getid(ns), buffer);

  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if ((status == IBP_OK) && (fin == 0)) {
     p = op->probe_op.alias_probe;
     p->read_refcount = atoi(string_token(NULL, " ", &bstate, &fin));
     p->write_refcount = atoi(string_token(NULL, " ", &bstate, &fin));
     p->offset = atol(string_token(NULL, " ", &bstate, &fin));
     p->size = atol(string_token(NULL, " ", &bstate, &fin));
     p->duration = atol(string_token(NULL, " ", &bstate, &fin)) + time(NULL); //** This is in sec NOT APR time
  } else {
     process_error(gop, &err, status, -1, &bstate);
  }

  return(err);
}

//*************************************************************
//  set_ibp_alias_probe_op - Generates a new IBP_PROBE command to get
//     information about an existing ALIAS allocation
//*************************************************************

void set_ibp_alias_probe_op(ibp_op_t *op, ibp_cap_t *cap, ibp_alias_capstatus_t *probe, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_probe_t *cmd;
  
  init_ibp_base_op(op, "alias_probe", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_MANAGE, IBP_PROBE);

  cmd = &(op->probe_op);

  parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_MANAGE]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->cap = cap;
  cmd->alias_probe = probe;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = alias_probe_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = alias_probe_recv;
}

//*************************************************************
//  new_ibp_alias_probe_op - Creats and generates  a new IBP_PROBE
//     command to get information about an existing ALIAS allocation
//*************************************************************

op_generic_t *new_ibp_alias_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_alias_capstatus_t *probe, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);

  set_ibp_alias_probe_op(op, cap, probe, timeout);

  return(ibp_get_gop(op));
}


//=============================================================
// IBP_copyappend routines
//    These routines allow you to copy an allocation between
//    depots.  The offset is only specified for the src cap.
//    The data is *appended* to the dest cap.
//=============================================================

//*************************************************************

op_status_t copyappend_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_copy_t *cmd;

  cmd = &(op->copy_op);

  if (ns_chksum_is_valid(&(op->ncs)) == 0) { 
      snprintf(buffer, sizeof(buffer), "%d %d %s %s %s %s " I64T " " I64T " %d %d %d\n", 
          IBPv040, cmd->ibp_command, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey, cmd->src_offset, cmd->len,
          (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
  } else {
      snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s %s %s " I64T " " I64T " %d %d %d\n",
          IBPv040, cmd->ibp_command, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
          cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey, cmd->src_offset, cmd->len,
          (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
  }

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "copyappend_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t copy_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  int status, fin;
  char buffer[1025];
  op_status_t err;
  ibp_off_t nbytes;
  char *bstate;
  ibp_op_copy_t *cmd;
  double swait;

  cmd = &(op->copy_op);

  //** Need to read the depot status info
  log_printf(15, "copy_recv: ns=%d Start", ns->id);

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "copy_recv: after readline ns=%d buffer=%s\n", ns_getid(ns), buffer);

  status = atoi(string_token(buffer, " ", &bstate, &fin));
  swait = atof(string_token(NULL, " ", &bstate, &fin));  nbytes = swait;
  if ((status != IBP_OK) || (nbytes != cmd->len)) {
     log_printf(0, "copy_recv: (read) ns=%d srccap=%s destcap=%s offset=" I64T " len=" I64T " err=%d Error!  status/nbytes=!%s!\n",
          ns_getid(ns), cmd->srccap, cmd->destcap, cmd->src_offset, cmd->len, err, buffer);
     process_error(gop, &err, status, swait, NULL);
  }

  return(err);
}


//*************************************************************
// set_ibp_copyappend_op - Generates a new depot copy operation
//*************************************************************

void set_ibp_copyappend_op(ibp_op_t *op, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size,
        int src_timeout, int  dest_timeout, int dest_client_timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_copy_t *cmd;

  init_ibp_base_op(op, "copyappend", src_timeout, op->ic->rw_new_command + size, NULL, size, IBP_SEND, IBP_NOP);

  cmd = &(op->copy_op);

  parse_cap(op->ic, srccap, host, &port, cmd->src_key, cmd->src_typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SEND]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->ibp_command = IBP_SEND;
  if (ns_type == NS_TYPE_PHOEBUS) {
     cmd->ibp_command = IBP_PHOEBUS_SEND;
     cmd->path = path;
     if (cmd->path == NULL) cmd->path = "auto";  //** If NULL default to auto
  } else {    //** All other ns types don't use the path
     cmd->path = "\0";
  }

  //** Want chksumming so tweak the command
  if (ns_chksum_is_valid(&(op->ncs)) == 1) {
     if (cmd->ibp_command == IBP_SEND) {
        cmd->ibp_command = IBP_SEND_CHKSUM;
     } else {
        cmd->ibp_command = IBP_PHOEBUS_SEND_CHKSUM;
     }
  }

  cmd->srccap = srccap;
  cmd->destcap = destcap;
  cmd->len = size;
  cmd->src_offset = src_offset;
  cmd->dest_timeout = dest_timeout;
  cmd->dest_client_timeout = dest_client_timeout;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = copyappend_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = copy_recv;
}

//*************************************************************

op_generic_t *new_ibp_copyappend_op(ibp_context_t *ic, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size,
        int src_timeout, int  dest_timeout, int dest_client_timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_copyappend_op(op, ns_type, path, srccap, destcap, src_offset, size, src_timeout, dest_timeout, dest_client_timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// IBP_push routines
// These routines allow you to push an allocation between
// depots.
//=============================================================

//*************************************************************

op_status_t pushpull_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_copy_t *cmd;

  cmd = &(op->copy_op);

  if (ns_chksum_is_valid(&(op->ncs)) == 0) {
     snprintf(buffer, sizeof(buffer), "%d %d %d %s %s %s %s " I64T " " I64T " " I64T " %d %d %d\n",
          IBPv040, cmd->ibp_command, cmd->ctype, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey,
          cmd->src_offset, cmd->dest_offset, cmd->len,
          (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
  } else {
     snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %d %s %s %s %s " I64T " " I64T " " I64T " %d %d %d\n",
          IBPv040, cmd->ibp_command, ns_chksum_type(&(op->ncs)), ns_chksum_blocksize(&(op->ncs)),
          cmd->ctype, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey,
          cmd->src_offset, cmd->dest_offset, cmd->len,
          (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
  }

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "copyappend_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

//*************************************************************
// set_ibp_copy_op - Generates a new depot copy operation
//*************************************************************

void set_ibp_copy_op(ibp_op_t *op, int mode, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap,
        ibp_off_t src_offset, ibp_off_t dest_offset, ibp_off_t size, int src_timeout, int  dest_timeout,
        int dest_client_timeout)
{
  char hoststr[MAX_HOST_SIZE];
  int port;
  char host[MAX_HOST_SIZE];
  ibp_op_copy_t *cmd;

  init_ibp_base_op(op, "copy", src_timeout, op->ic->rw_new_command + size, NULL, size, IBP_SEND, IBP_NOP);

  cmd = &(op->copy_op);

  parse_cap(op->ic, srccap, host, &port, cmd->src_key, cmd->src_typekey);
  set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SEND]));
  op->dop.cmd.hostport = strdup(hoststr);

  cmd->ibp_command = mode;
  if (ns_type == NS_TYPE_PHOEBUS) {
     cmd->ctype = IBP_PHOEBUS;
     cmd->path = path;
     if (cmd->path == NULL) cmd->path = "auto";  //** If NULL default to auto
  } else {    //** All other ns types don't use the path
     cmd->ctype = IBP_TCP;
     cmd->path = "\0";
  }

  //** Want chksumming so tweak the command
  if (ns_chksum_is_valid(&(op->ncs)) == 1) {
     if (cmd->ibp_command == IBP_PUSH) {
        cmd->ibp_command = IBP_PUSH_CHKSUM;
     } else {
        cmd->ibp_command = IBP_PULL_CHKSUM;
     }
  }

  cmd->srccap = srccap;
  cmd->destcap = destcap;
  cmd->len = size;
  cmd->src_offset = src_offset;
  cmd->dest_offset = dest_offset;
  cmd->dest_timeout = dest_timeout;
  cmd->dest_client_timeout = dest_client_timeout;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = pushpull_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = copy_recv;
}

//*************************************************************

op_generic_t *new_ibp_copy_op(ibp_context_t *ic, int mode, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap,
        ibp_off_t src_offset, ibp_off_t dest_offset, ibp_off_t size, int src_timeout,
        int  dest_timeout, int dest_client_timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_copy_op(op, mode, ns_type, path, srccap, destcap, src_offset, dest_offset, size, src_timeout, dest_timeout, dest_client_timeout);

  return(ibp_get_gop(op));
}


//=============================================================
//  routines to handle modifying a depot's resources
//=============================================================

op_status_t depot_modify_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  ibp_op_depot_modify_t *cmd;

  cmd = &(op->depot_modify_op);

  snprintf(buffer, sizeof(buffer), "%d %d %s %d %s %d\n " I64T " " I64T " " TT "\n",
       IBPv040, IBP_STATUS, cmd->depot->rid.name, IBP_ST_CHANGE, cmd->password, (int)apr_time_sec(gop->op->cmd.timeout),
       cmd->max_hard, cmd->max_soft, cmd->max_duration);

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "modify_depot_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************
//  set_ibp_depot_modify - Modify the settings of a depot/RID
//*************************************************************

void set_ibp_depot_modify_op(ibp_op_t *op, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft,
      int duration, int timeout)
{
  ibp_op_depot_modify_t *cmd = &(op->depot_modify_op);

  init_ibp_base_op(op, "depot_modify", timeout, op->ic->other_new_command, NULL,
         op->ic->other_new_command, IBP_STATUS, IBP_ST_CHANGE);

  cmd->depot = depot;
  cmd->password = password;
  cmd->max_hard = hard;
  cmd->max_soft = soft;
  cmd->max_duration = duration;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = depot_modify_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = status_get_recv;
}

//*************************************************************

op_generic_t *new_ibp_depot_modify_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft,
      int duration, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_depot_modify_op(op, depot, password, hard, soft, duration, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
//  routines to handle querying a depot's resource
//=============================================================

op_status_t depot_inq_command(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  ibp_op_depot_inq_t *cmd;

  cmd = &(op->depot_inq_op);

  ns_write_chksum_clear(ns);

  snprintf(buffer, sizeof(buffer), "%d %d %s %d %s %d\n",
       IBPv040, IBP_STATUS, cmd->depot->rid.name, IBP_ST_INQ, cmd->password, (int)apr_time_sec(gop->op->cmd.timeout));

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "depot_inq_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

int process_inq(char *buffer, ibp_depotinfo_t *di)
{
  char *bstate, *bstate2, *p, *key, *d;
  int err;

  memset(di, 0, sizeof(ibp_depotinfo_t));
 
  p = string_token(buffer, " ", &bstate, &err);
  while (err == 0) {
     key = string_token(p, ":", &bstate2, &err);
     d = string_token(NULL, ":", &bstate2, &err);
     
     if (strcmp(key, ST_VERSION) == 0) {
        di->majorVersion = atof(d);
        di->minorVersion = atof(string_token(NULL, ":", &bstate2, &err));
     } else if (strcmp(key, ST_DATAMOVERTYPE) == 0) {
        //*** I just skip this.  IS it used??? ***
     } else if (strcmp(key, ST_RESOURCEID) == 0) {
        di->rid = atol(d);
     } else if (strcmp(key, ST_RESOURCETYPE) == 0) {
        di->type = atol(d);
     } else if (strcmp(key, ST_CONFIG_TOTAL_SZ) == 0) {
        di->TotalConfigured = atoll(d);
     } else if (strcmp(key, ST_SERVED_TOTAL_SZ) == 0) {
        di->TotalServed = atoll(d);     
     } else if (strcmp(key, ST_USED_TOTAL_SZ) == 0) {
        di->TotalUsed = atoll(d);     
     } else if (strcmp(key, ST_USED_HARD_SZ) == 0) {
        di->HardUsed = atoll(d);
     } else if (strcmp(key, ST_SERVED_HARD_SZ) == 0) {
        di->HardServed = atoll(d);
     } else if (strcmp(key, ST_CONFIG_HARD_SZ) == 0) {
        di->HardConfigured = atoll(d);
     } else if (strcmp(key, ST_ALLOC_TOTAL_SZ) == 0) {
        di->SoftAllocable = atoll(d);  //** I have no idea what field this maps to....
     } else if (strcmp(key, ST_ALLOC_HARD_SZ) == 0) {
        di->HardAllocable = atoll(d);
     } else if (strcmp(key, ST_DURATION) == 0) {
        di->Duration = atoll(d);
     } else if (strcmp(key, "RE") == 0) {
       err = 1;
     } else {
       log_printf(1, "process_inq:  Unknown tag:%s key=%s data=%s\n", p, key, d);
     }

     p = string_token(NULL, " ", &bstate, &err);
  }

  return(IBP_OK);
}

//*************************************************************

op_status_t depot_inq_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024];
  op_status_t err;
  int nbytes, status, fin;
  char *bstate;
  ibp_op_depot_inq_t *cmd;

  cmd = &(op->depot_inq_op);

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

  log_printf(15, "depot_inq_recv: after readline ns=%d buffer=%s err=%d\n", ns_getid(ns), buffer, err);

  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if ((status == IBP_OK) && (fin == 0)) {
     nbytes = atoi(string_token(NULL, " ", &bstate, &fin));
//log_printf(15, "depot_inq_recv: nbytes= ns=%d err=%d\n", nbytes, ns_getid(ns), err);

     if (nbytes <= 0) { return(ibp_error_status); }
     if (sizeof(buffer) < nbytes) { return(ibp_error_status); }

     //** Read the next line.  I ignore the size....
     err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
//  log_printf(15, "depot_inq_recv: after 2nd readline ns=%d buffer=%s err=%d\n", ns_getid(ns), buffer, err);
     if (err.op_status != OP_STATE_SUCCESS) return(err);

     status = process_inq(buffer, cmd->di);
     if (status == IBP_OK) {
        err = ibp_success_status;
     } else {
        _op_set_status(err, OP_STATE_FAILURE, status);
     }
  } else {
    process_error(gop, &err, status, -1, NULL);
  }

  return(err);
}

//*************************************************************
//  set_ibp_depot_inq - Inquires about a depots resource
//*************************************************************

void set_ibp_depot_inq_op(ibp_op_t *op, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char pchost[MAX_HOST_SIZE];
  ibp_op_depot_inq_t *cmd = &(op->depot_inq_op);

  ibppc_form_host(op->ic, pchost, sizeof(pchost), depot->host, depot->rid);
  set_hostport(hoststr, sizeof(hoststr), pchost, depot->port, &(op->ic->cc[IBP_STATUS]));

  init_ibp_base_op(op, "depot_inq", timeout, op->ic->other_new_command, strdup(hoststr), 
         op->ic->other_new_command, IBP_STATUS, IBP_ST_INQ);
  
  cmd->depot = depot;
  cmd->password = password;
  cmd->di = di;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = depot_inq_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = depot_inq_recv;
}

//*************************************************************

op_generic_t *new_ibp_depot_inq_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_depot_inq_op(op, depot, password, di, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// Get depot "version" routines
//=============================================================

op_status_t depot_version_command(op_generic_t *gop, NetStream_t *ns)
{
  char buffer[1024];
  op_status_t err;

  snprintf(buffer, sizeof(buffer), "%d %d %d %d\n",IBPv040, IBP_STATUS, IBP_ST_VERSION, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "depot_version_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t depot_version_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024], *bstate; 
  op_status_t err;
  int pos, nmax, status, fin;
  ibp_op_version_t *cmd;

  cmd = &(op->ver_op);

  err = ibp_success_status;
  pos = 0;
  cmd->buffer[0] = '\0';

  ns_read_chksum_clear(ns);

  status = IBP_E_GENERIC;

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status == OP_STATE_SUCCESS) status = atoi(string_token(buffer, " ", &bstate, &fin));

  if (status == IBP_OK) {
     err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);

     while (err.op_status == OP_STATE_SUCCESS)  {
        if (strcmp("END", buffer) == 0) {  //** Got the end so exit
           return(err);
        }

        //** Copy what we can **
        nmax = cmd->buffer_size - pos - 2;
        strncat(cmd->buffer, buffer, nmax);
        strcat(cmd->buffer, "\n");
        if (strlen(buffer) + pos > cmd->buffer_size) {  //** Exit if we are out of space
           _op_set_status(err, OP_STATE_FAILURE, IBP_E_WOULD_EXCEED_LIMIT);
           return(err);
        }

        err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
     }
  } else {
    process_error(gop, &err, status, -1, &bstate);
  }

  return(err);
}


//*************************************************************

void set_ibp_version_op(ibp_op_t *op, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout)
{
  char hoststr[MAX_HOST_SIZE];
  char pchoststr[MAX_HOST_SIZE];
  ibp_op_version_t *cmd = &(op->ver_op);

  ibppc_form_host(op->ic, pchoststr, sizeof(pchoststr), depot->host, depot->rid);
  set_hostport(hoststr, sizeof(hoststr), pchoststr, depot->port, &(op->ic->cc[IBP_STATUS]));

  init_ibp_base_op(op, "depot_version", timeout, op->ic->other_new_command, strdup(hoststr),
         op->ic->other_new_command, IBP_STATUS, IBP_ST_VERSION);
  
  cmd->depot = depot;
  cmd->buffer = buffer;
  cmd->buffer_size = buffer_size;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = depot_version_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = depot_version_recv;
}

//*************************************************************

op_generic_t *new_ibp_version_op(ibp_context_t *ic, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_version_op(op, depot, buffer, buffer_size, timeout);

  return(ibp_get_gop(op));
}

//=============================================================
// routines for getting the list or resources from a depot
//=============================================================

//*************************************************************

op_status_t query_res_command(op_generic_t *gop, NetStream_t *ns)
{
  char buffer[1024]; 
  op_status_t err;

  snprintf(buffer, sizeof(buffer), "%d %d %d %d\n",IBPv040, IBP_STATUS, IBP_ST_RES, (int)apr_time_sec(gop->op->cmd.timeout));

  ns_write_chksum_clear(ns);

  err = send_command(gop, ns, buffer);
  if (err.op_status != OP_STATE_SUCCESS) {
     log_printf(10, "query_res_command: Error with send_command()! ns=%d\n", ns_getid(ns));
  }

  return(err);
}

//*************************************************************

op_status_t query_res_recv(op_generic_t *gop, NetStream_t *ns)
{
  ibp_op_t *op = ibp_get_iop(gop);
  char buffer[1024]; 
  op_status_t err;
  int fin, n, i, status;
  char *p, *bstate;
  ibp_op_rid_inq_t *cmd = &(op->rid_op);

  err = ibp_success_status;

  ns_read_chksum_clear(ns);

  err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
  if (err.op_status != OP_STATE_SUCCESS) return(err);

//  log_printf(0, "query_res_recv: ns=%d buffer=!%s!\n", ns_getid(ns), buffer);

  //** check to make sure the depot supports the command
  status = atoi(string_token(buffer, " ", &bstate, &fin));
  if (err.op_status != OP_STATE_SUCCESS) return(process_error(gop, &err, status, -1, &bstate));

  //** Ok now we just need to process the line **
  Stack_t *list = new_stack();
  p = string_token(NULL, " ", &bstate, &fin);
  while (fin == 0) {
//    log_printf(0, "query_res_recv: ns=%d rid=%s\n", ns_getid(ns), p);
    push(list, p);
    p = string_token(NULL, " ", &bstate, &fin);
  }

  n = stack_size(list);
  ridlist_init(cmd->rlist, n);
  move_to_bottom(list);
  for (i=0; i<n; i++) {
     p = get_ele_data(list);
     cmd->rlist->rl[i] = ibp_str2rid(p);
     move_up(list);
  }

  free_stack(list, 0);

  return(err);
}

//*************************************************************

void set_ibp_query_resources_op(ibp_op_t *op, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout)
{
{
  char hoststr[MAX_HOST_SIZE];
  char pchoststr[MAX_HOST_SIZE];
  ibp_op_rid_inq_t *cmd = &(op->rid_op);

  ibppc_form_host(op->ic, pchoststr, sizeof(pchoststr), depot->host, depot->rid);
  set_hostport(hoststr, sizeof(hoststr), pchoststr, depot->port, &(op->ic->cc[IBP_STATUS]));

  init_ibp_base_op(op, "query_resources", timeout, op->ic->other_new_command, strdup(hoststr),
         op->ic->other_new_command, IBP_STATUS, IBP_ST_RES);
  
  cmd->depot = depot;
  cmd->rlist = rlist;

  op_generic_t *gop = ibp_get_gop(op);
  gop->op->cmd.send_command = query_res_command;
  gop->op->cmd.send_phase = NULL;
  gop->op->cmd.recv_phase = query_res_recv;
}

}

//*************************************************************

op_generic_t *new_ibp_query_resources_op(ibp_context_t *ic, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout)
{
  ibp_op_t *op = new_ibp_op(ic);
  if (op == NULL) return(NULL);

  set_ibp_query_resources_op(op, depot, rlist, timeout);

  return(ibp_get_gop(op));
}
