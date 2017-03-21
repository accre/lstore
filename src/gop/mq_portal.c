/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <apr.h>
#include <apr_base64.h>
#include <apr_errno.h>
#include <assert.h>
#include <gop/mq.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

#include "gop.h"
#include "gop/portal.h"
#include "mq_portal.h"
#include "thread_pool.h"

//** Poll index for connection monitoring
#define PI_CONN 0   //** Actual connection
#define PI_EFD  1   //** Portal event FD for incoming tasks

void _tp_submit_op(void *arg, gop_op_generic_t *gop);
int mq_conn_create(gop_mq_portal_t *p, int dowait);
void gop_mq_conn_teardown(gop_mq_conn_t *c);
void mqc_heartbeat_dec(gop_mq_conn_t *c, gop_mq_heartbeat_entry_t *hb);
void _mq_reap_closed(gop_mq_portal_t *p);
void *mqtp_failure(apr_thread_t *th, void *arg);

//**************************************************************
//  gop_mq_portal_mq_context - Return the MQ context from the portal
//**************************************************************

gop_mq_context_t *gop_mq_portal_mq_context(gop_mq_portal_t *p)
{
  return(p->mqc);
}

//**************************************************************
// gop_mq_id2str - Convert the command id to a printable string
//**************************************************************

char *gop_mq_id2str(char *id, int id_len, char *str, int str_len)
{
   FATAL_UNLESS(str_len > 2*id_len+1);
    apr_base64_encode(str, id, id_len);

    return(str);
}

//**************************************************************
// gop_mq_stats_add - Add command stats together (a = a+b)
//**************************************************************

void gop_mq_stats_add(gop_mq_command_stats_t *a, gop_mq_command_stats_t *b)
{
    int i;

    for (i=0; i<MQS_SIZE; i++) {
        a->incoming[i] += b->incoming[i];
        a->outgoing[i] += b->outgoing[i];
    }
}

//**************************************************************
//  gop_mq_stats_print - Prints the stats
//**************************************************************

void gop_mq_stats_print(int ll, char *tag, gop_mq_command_stats_t *a)
{
    int i;
    char *fmt = "  %12s: %8d    %8d\n";
    char *command[MQS_SIZE] = { "PING", "PONG", "EXEC", "TRACKEXEC", "TRACKADDRESS", "RESPONSE", "HEARTBEAT", "UNKNOWN" };

    log_printf(ll, "----------- Command Stats for %s --------------\n", tag);
    log_printf(ll, "    Command     incoming    outgoing\n");
    for (i=0; i< MQS_SIZE; i++) {
        log_printf(ll, fmt, command[i], a->incoming[i], a->outgoing[i]);
    }

    log_printf(ll, "----------------------------------------------------------------\n");
}


//**************************************************************

gop_mq_command_t *gop_mq_command_new(void *cmd, int cmd_size, void *arg, gop_mq_exec_fn_t fn)
{
    gop_mq_command_t *mqc;

    tbx_type_malloc(mqc, gop_mq_command_t, 1);

    tbx_type_malloc(mqc->cmd, void, cmd_size);
    memcpy(mqc->cmd, cmd, cmd_size);

    mqc->cmd_size = cmd_size;
    mqc->arg = arg;
    mqc->fn = fn;

    return(mqc);
}

//**************************************************************
//  gop_mq_command_set - Adds/removes and RPC call to the local host
//**************************************************************

void gop_mq_command_set(gop_mq_command_table_t *table, void *cmd, int cmd_size, void *arg, gop_mq_exec_fn_t fn)
{
    gop_mq_command_t *mqc;

    log_printf(15, "command key = %d\n", ((char *)cmd)[0]);
    apr_thread_mutex_lock(table->lock);
    if (fn != NULL) {
        mqc = apr_hash_get(table->table, cmd, cmd_size);
        if (mqc != NULL) {
            apr_hash_set(table->table, mqc->cmd, mqc->cmd_size, NULL);
            free(mqc->cmd);
            free(mqc);
        }

        mqc = gop_mq_command_new(cmd, cmd_size, arg, fn);
        apr_hash_set(table->table, mqc->cmd, mqc->cmd_size, mqc);
    } else {
        mqc = apr_hash_get(table->table, cmd, cmd_size);
        if (mqc != NULL) {
            apr_hash_set(table->table, mqc->cmd, mqc->cmd_size, NULL);
            free(mqc->cmd);
            free(mqc);
        }
    }
    apr_thread_mutex_unlock(table->lock);
}

//**************************************************************
//  gop_mq_command_table_new - Creates a new RPC table
//**************************************************************

void gop_mq_command_table_set_default(gop_mq_command_table_t *table, void *arg, gop_mq_exec_fn_t fn_default)
{
    apr_thread_mutex_lock(table->lock);
    table->fn_default = fn_default;
    table->arg_default = arg;
    apr_thread_mutex_unlock(table->lock);

}


//**************************************************************
//  gop_mq_command_table_new - Creates a new RPC table
//**************************************************************

gop_mq_command_table_t *gop_mq_command_table_new(void *arg, gop_mq_exec_fn_t fn_default)
{
    gop_mq_command_table_t *t;

    tbx_type_malloc(t, gop_mq_command_table_t, 1);

    t->fn_default = fn_default;
    t->arg_default = arg;
    apr_pool_create(&(t->mpool), NULL);
    assert_result(apr_thread_mutex_create(&(t->lock), APR_THREAD_MUTEX_DEFAULT,t->mpool),
                  APR_SUCCESS);
    t->table = apr_hash_make(t->mpool);FATAL_UNLESS(t->table != NULL);

    return(t);
}

//**************************************************************
//  gop_mq_command_table_destroy- Destroys an RPC table
//**************************************************************

void gop_mq_command_table_destroy(gop_mq_command_table_t *t)
{
    apr_hash_index_t *hi;
    gop_mq_command_t *cmd;
    void *val;

    for (hi=apr_hash_first(t->mpool, t->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        cmd = (gop_mq_command_t *)val;
        apr_hash_set(t->table, cmd->cmd, cmd->cmd_size, NULL);
        free(cmd->cmd);
        free(cmd);
    }

    apr_pool_destroy(t->mpool);
    free(t);

    return;
}

//**************************************************************
//  gop_mq_command_exec - Executes an RPC call
//**************************************************************

void gop_mq_command_exec(gop_mq_command_table_t *t, gop_mq_task_t *task, void *key, int klen)
{
    gop_mq_command_t *cmd;

    cmd = apr_hash_get(t->table, key, klen);

    log_printf(3, "cmd=%p klen=%d\n", cmd, klen);
    if (cmd == NULL) {
        log_printf(0, "Unknown command!\n");
        if (t->fn_default != NULL)
            t->fn_default(t->arg_default, task);
    } else {
        cmd->fn(cmd->arg, task);
    }
}

//**************************************************************
// gop_mq_submit - Submits a task for processing
//**************************************************************

int gop_mq_submit(gop_mq_portal_t *p, gop_mq_task_t *task)
{
    char c;
    int backlog, err;
    gop_mq_task_t *t;
    apr_thread_mutex_lock(p->lock);

    //** Do a quick check for connections that need to be reaped
    if (tbx_stack_count(p->closed_conn) > 0)
        _mq_reap_closed(p);

    //** Add the task and get the backlog
    tbx_stack_move_to_bottom(p->tasks);
    tbx_stack_insert_below(p->tasks, task);
    backlog = tbx_stack_count(p->tasks);
    log_printf(2, "portal=%s backlog=%d active_conn=%d max_conn=%d total_conn=%d\n", p->host, backlog, p->active_conn, p->max_conn, p->total_conn);
    tbx_log_flush();

    //** Noitify the connections
    c = 1;
    gop_mq_pipe_write(p->efd[1], &c);

    //** Check if we need more connections
    err = 0;
    if (backlog > p->backlog_trigger) {
        if (p->total_conn == 0) { //** No current connections so try and make one
            err = mq_conn_create(p, 1);
            if (err != 0) {  //** Fail everything
                log_printf(1, "Host is dead so failing tasks host=%s\n", p->host);
                while ((t = tbx_stack_pop(p->tasks)) != NULL) {
                    thread_pool_direct(p->tp, mqtp_failure, t);
                }
            }
        } else if (p->total_conn < p->max_conn) {
            err = mq_conn_create(p, 0);
        }
    } else if (p->total_conn == 0) { //** No current connections so try and make one
        err = mq_conn_create(p, 1);
        if (err != 0) {  //** Fail everything
            log_printf(1, "Host is dead so failing tasks host=%s\n", p->host);
            while ((t = tbx_stack_pop(p->tasks)) != NULL) {
                thread_pool_direct(p->tp, mqtp_failure, t);
            }
        }
    }

    log_printf(2, "END portal=%s err=%d backlog=%d active_conn=%d total_conn=%d max_conn=%d\n", p->host, err, backlog, p->active_conn, p->total_conn, p->max_conn);
    tbx_log_flush();

    apr_thread_mutex_unlock(p->lock);

    return(0);
}

//**************************************************************
// mq_task_send - Sends a task for processing
//**************************************************************

int mq_task_send(gop_mq_context_t *mqc, gop_mq_task_t *task)
{
    gop_mq_portal_t *p;
    gop_mq_frame_t *f;
    char *host;
    int size;

    f = gop_mq_msg_first(task->msg);

    if (f == NULL) return(1);

    gop_mq_get_frame(f, (void **)&host, &size);

    //** Look up the portal
    apr_thread_mutex_lock(mqc->lock);
    p = (gop_mq_portal_t *)(apr_hash_get(mqc->client_portals, host, size));
    if (p == NULL) {  //** New host so create the portal
        FATAL_UNLESS(host != NULL);
        log_printf(10, "Creating MQ_CMODE_CLIENT portal for outgoing connections host = %s size = %d\n", host, size);
        p = gop_mq_portal_create(mqc, host, MQ_CMODE_CLIENT);
        apr_hash_set(mqc->client_portals, p->host, APR_HASH_KEY_STRING, p);
    }
    apr_thread_mutex_unlock(mqc->lock);

    return(gop_mq_submit(p, task));
}

//**************************************************************
//  mq_task_destroy - Destroys an MQ task
//**************************************************************

void mq_task_destroy(gop_mq_task_t *task)
{
    if (task->msg != NULL) gop_mq_msg_destroy(task->msg);
    if (task->response != NULL) gop_mq_msg_destroy(task->response);
    if (task->my_arg_free) task->my_arg_free(task->arg);
    free(task);
}

//**************************************************************
// mq_arg_free - Called by GOP routines on destruction
//**************************************************************

void mq_arg_free(void *arg)
{
    gop_mq_task_t *task = (gop_mq_task_t *)arg;

    mq_task_destroy(task);
}


//**************************************************************
// mq_task_set - Initializes a task for use
//**************************************************************

int mq_task_set(gop_mq_task_t *task, gop_mq_context_t *ctx, mq_msg_t *msg, gop_op_generic_t *gop,  void *arg, int dt)
{
    task->ctx = ctx;
    task->msg = msg;
    task->gop = gop;
    task->arg = arg;
    task->timeout = dt;
    task->pass_through = 0; //default value!
    return(0);
}

//**************************************************************
// gop_mq_task_new - Creates and initializes a task for use
//**************************************************************

gop_mq_task_t *gop_mq_task_new(gop_mq_context_t *ctx, mq_msg_t *msg, gop_op_generic_t *gop, void *arg, int dt)
{
    gop_mq_task_t *task;

    tbx_type_malloc_clear(task, gop_mq_task_t, 1);

    mq_task_set(task, ctx, msg, gop, arg, dt);

    return(task);
}

//*************************************************************
// gop_tp_op_new - Allocates space for a new op
//*************************************************************

gop_op_generic_t *gop_mq_op_new(gop_mq_context_t *ctx, mq_msg_t *msg, gop_op_status_t (*fn_response)(void *arg, int id), void *arg, void (*my_arg_free)(void *arg), int dt)
{
    gop_mq_task_t *task;

    task = gop_mq_task_new(ctx, msg, NULL, arg, dt);
    task->gop = gop_tp_op_new(ctx->tp, "mq", fn_response, task, mq_arg_free, 1);
    task->my_arg_free = my_arg_free;
    return(task->gop);
}


//**************************************************************
// mqt_exec - Routine to process exec/trackexec commands
//**************************************************************

void *mqt_exec(apr_thread_t *th, void *arg)
{
    gop_mq_task_t *task = (gop_mq_task_t *)arg;
    gop_mq_portal_t *p = (gop_mq_portal_t *)task->arg;
    gop_mq_frame_t *f;
    char b64[1024];
    void *key;
    int n;

    gop_mq_msg_first(task->msg);    //** Empty frame
    gop_mq_msg_next(task->msg);     //** Version
    gop_mq_msg_next(task->msg);     //** MQ command
    f = gop_mq_msg_next(task->msg);     //** Skip the ID
    gop_mq_get_frame(f, &key, &n);
    log_printf(1, "execing sid=%s\n", gop_mq_id2str(key, n, b64, sizeof(b64)));
    f = gop_mq_msg_next(task->msg); //** and get the user command
    gop_mq_get_frame(f, &key, &n);

    //** Lookup and see if the envelope command is supported.
    gop_mq_command_exec(p->command_table, task, key, n);

    mq_task_destroy(task);

    return(NULL);
}

//**************************************************************
// mqt_success - Routine for successful send of a message
//**************************************************************

void *mqtp_success(apr_thread_t *th, void *arg)
{
    gop_mq_task_t *task = (gop_mq_task_t *)arg;

    gop_mark_completed(task->gop, gop_success_status);

    return(NULL);
}

//**************************************************************
// mqt_fail - Routine for failing a task
//**************************************************************

void *mqtp_failure(apr_thread_t *th, void *arg)
{
    gop_mq_task_t *task = (gop_mq_task_t *)arg;

    gop_mark_completed(task->gop, gop_failure_status);

    return(NULL);
}

//**************************************************************
//  mq_task_complete - Marks a task as complete and destroys it
//**************************************************************

void mq_task_complete(gop_mq_conn_t *c, gop_mq_task_t *task, int status)
{
    if (task->gop == NULL) {
        mq_task_destroy(task);
    } else if (status == OP_STATE_SUCCESS) {
        thread_pool_direct(c->pc->tp, mqtp_success, task);
    } else if (status == OP_STATE_FAILURE) {
        thread_pool_direct(c->pc->tp, mqtp_failure, task);
    }
}

//**************************************************************
// mqc_response - Processes a command response
//**************************************************************

void mqc_response(gop_mq_conn_t *c, mq_msg_t *msg, int do_exec)
{
    gop_mq_frame_t *f;
    int size;
    char *id;
    gop_mq_task_monitor_t *tn;
    char b64[1024];

    log_printf(5, "start\n");
    tbx_log_flush();

    f = gop_mq_msg_next(msg);  //** This should be the task ID
    gop_mq_get_frame(f, (void **)&id, &size);
    log_printf(5, "id_size=%d\n", size);

    //** Find the task
    tn = apr_hash_get(c->waiting, id, size);
    if (tn == NULL) {  //** Nothing matches so drop it
        log_printf(1, "ERROR: No matching ID! sid=%s\n", gop_mq_id2str(id, size, b64, sizeof(b64)));
        tbx_log_flush();
        gop_mq_msg_destroy(msg);
        return;
    }

    //** We have a match if we made it here
    //** Remove us from the waiting table
    apr_hash_set(c->waiting, id, size, NULL);

    //** and also dec the heartbeat entry
    if (tn->tracking != NULL) mqc_heartbeat_dec(c, tn->tracking);

    //** Execute the task in the thread pool
    if(do_exec != 0) {
        log_printf(5, "Submitting repsonse for exec gid=%d\n", gop_id(tn->task->gop));
        tbx_log_flush();
        tn->task->response = msg;
        _tp_submit_op(NULL, tn->task->gop);
    }

    //** Free the tracking number container
    free(tn);

    log_printf(5, "end\n");
    tbx_log_flush();
}

//**************************************************************
// gop_mq_msg_apply_return_address - Converts the raw return address
//  to a "Sender" address o nteh message
//  NOTE: The raw address should have the empty frame!
//        if dup_frames == 0 then raw_address frames are consumed!
//**************************************************************

void gop_mq_msg_apply_return_address(mq_msg_t *msg, mq_msg_t *raw_address, int dup_frames)
{
    gop_mq_frame_t *f;

    f = gop_mq_msg_first(raw_address);
    if (dup_frames == 0) f = mq_msg_pop(raw_address);
    while (f != NULL) {
        if (dup_frames == 1) {
            gop_mq_msg_frame_push(msg, gop_mq_frame_dup(f));
        } else {
            gop_mq_msg_frame_push(msg, f);
        }

        f = (dup_frames == 0) ? mq_msg_pop(raw_address) : gop_mq_msg_next(raw_address);
    }

    return;
}

//**************************************************************
// gop_mq_msg_trackaddress - Forms a track address response
//   This takes the raw address frames from the original email
//   and flips or duplicates them based dup_frames
//   ****NOTE:  The address should start with the EMPTY frame****
//        if dup_frames == 0 then raw_address frames are consumed!
//**************************************************************

mq_msg_t *gop_mq_msg_trackaddress(char *host, mq_msg_t *raw_address, gop_mq_frame_t *fid, int dup_frames)
{
    mq_msg_t *track_response;
    gop_mq_frame_t *f;

    track_response = gop_mq_msg_new();
    gop_mq_msg_append_mem(track_response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(track_response, MQF_TRACKADDRESS_KEY, MQF_TRACKADDRESS_SIZE, MQF_MSG_KEEP_DATA);

    if (dup_frames == 1) {
        gop_mq_msg_append_frame(track_response, gop_mq_frame_dup(fid));
    } else {
        gop_mq_msg_append_frame(track_response, fid);
    }

    //** Add the address. We skip frame 0 (empty) and frame 1 (sender -- he knows who he is)
    gop_mq_msg_first(raw_address);
    gop_mq_msg_next(raw_address);
    while ((f = gop_mq_msg_next(raw_address)) != NULL) {
        gop_mq_msg_append_frame(track_response, gop_mq_frame_dup(f));  //** Always dup frames
    }

    //** Need to add ourselves and the empty frame to the tracking address
    gop_mq_msg_append_mem(track_response, host, strlen(host), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(track_response, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Lastly add the return addres.  We always dup the frames here cause they are used
    //** in the address already if not duped.
    gop_mq_msg_apply_return_address(track_response, raw_address, dup_frames);

    return(track_response);
}

//**************************************************************
// mqc_trackaddress - Processes a track address command
//**************************************************************

void mqc_trackaddress(gop_mq_conn_t *c, mq_msg_t *msg)
{
    gop_mq_frame_t *f;
    int size, n;
    char *id, *address;
    gop_mq_task_monitor_t *tn;
    gop_mq_heartbeat_entry_t *hb;

    f = gop_mq_msg_next(msg);  //** This should be the task ID
    gop_mq_get_frame(f, (void **)&id, &size);

    //** Find the task
    tn = apr_hash_get(c->waiting, id, size);
    log_printf(5, "trackaddress status tn=%p id_size=%d\n", tn, size);
    void *data;
    int i;
    for (f = gop_mq_msg_first(msg), i=0; f != NULL; f = gop_mq_msg_next(msg), i++) {
        gop_mq_get_frame(f, &data, &n);
        log_printf(5, "fsize[%d]=%d\n", i, n);
    }

    if (tn != NULL) {
        log_printf(5, "tn->tracking=%p\n", tn->tracking);
        if (tn->tracking != NULL) goto cleanup;  //** Duplicate so drop and ignore

        //** Form the address key but first strip off the gunk we don't care about to determine the size
        gop_mq_msg_first(msg);
        gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0)); // empty
        gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  // version
        gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  // TRACKADDRESS command
        gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  // id

        //** What's left is the address until an empty frame
        size = gop_mq_msg_total_size(msg);
        log_printf(5, " msg_total_size=%d frames=%d\n", size, tbx_stack_count(msg));
        tbx_type_malloc_clear(address, char, size+1);
        n = 0;
        for (f=gop_mq_msg_first(msg); f != NULL; f=gop_mq_msg_next(msg)) {
            gop_mq_get_frame(f, (void **)&id, &size);
            log_printf(5, "ta element=%d\n", size);
            memcpy(&(address[n]), id, size);
            n = n + size;
            if (size == 0) break;
        }
        address[n] = 0;
        log_printf(5, "full address=%s\n", address);

        //** Remove anything else
        f = gop_mq_msg_next(msg);
        while (f != NULL) {
            f = gop_mq_msg_pluck(msg, 0);
            gop_mq_frame_destroy(f);
            f = gop_mq_msg_current(msg);
        }

        //** Make sure its not already stored
        hb = apr_hash_get(c->heartbeat_dest, address, n);
        if (hb == NULL) {  //** Make the new entry
            tbx_type_malloc_clear(hb, gop_mq_heartbeat_entry_t, 1);
            hb->key = address;
            hb->key_size = n;
            hb->lut_id = tbx_atomic_global_counter();

            log_printf(5, "trackaddress hb_lut=" LU "\n", hb->lut_id);
            //** Form the heartbeat msg
            //** Right now we just have the address which should have an empty last frame
            gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
            gop_mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
            gop_mq_msg_append_mem(msg, &(hb->lut_id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
            gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

            hb->address = msg;
            msg = NULL;  //** Don't want it deleated

            //** Finish creeating the structure
            apr_hash_set(c->heartbeat_dest, hb->key, n, hb);
            apr_hash_set(c->heartbeat_lut, &(hb->lut_id), sizeof(uint64_t), hb);
            tn->last_check = apr_time_now();
            hb->last_check = tn->last_check;
        } else {
            free(address);  //** Alredy exists so just free the key
        }

        //** Store the heartbeat tracking entry
        tn->tracking = hb;
        hb->count++;
    }

cleanup:
//** Clean up
    if (msg != NULL) gop_mq_msg_destroy(msg);
}

//***************************************************************************
// mqc_ping - Processes a ping request
//***************************************************************************

int mqc_ping(gop_mq_conn_t *c, mq_msg_t *msg)
{
    mq_msg_t *pong;
    gop_mq_frame_t *f, *pid;
    int err;

    //** Peel off the top frames and just leave the return address
    gop_mq_msg_first(msg);
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //blank
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //version
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //command

    pid = gop_mq_msg_pluck(msg, 0);  //Ping ID

    pong = gop_mq_msg_new();

    //** Push the address in reverse order (including the empty frame)
    while ((f = mq_msg_pop(msg)) != NULL) {
        gop_mq_msg_frame_push(pong, f);
    }

    gop_mq_msg_destroy(msg);
    //** Now add the command
    gop_mq_msg_append_mem(pong, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(pong, MQF_PONG_KEY, MQF_PONG_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(pong, pid);
    gop_mq_msg_append_mem(pong, NULL, 0, MQF_MSG_KEEP_DATA);

    c->stats.incoming[MQS_PONG_INDEX]++;

    err = gop_mq_send(c->sock, pong, MQ_DONTWAIT);

    gop_mq_msg_destroy(pong);

    return(err);
}




//**************************************************************
// mqc_pong - Processed a pong command
//**************************************************************

void mqc_pong(gop_mq_conn_t *c, mq_msg_t *msg)
{
    gop_mq_frame_t *f;
    int size;
    gop_mq_heartbeat_entry_t *entry;
    void *ptr;

    f = gop_mq_msg_next(msg);  //** This should be the ID which is actually the entry
    gop_mq_get_frame(f, &ptr, &size);

    //** Validate the entry
    entry = apr_hash_get(c->heartbeat_lut, ptr, sizeof(uint64_t));
    if (entry != NULL) {
        entry->last_check = apr_time_now();
    }

    log_printf(5, "pong entry=%p ptr=%p\n", entry, ptr);
    //** Clean up
    gop_mq_msg_destroy(msg);
}


//**************************************************************
// mqc_heartbeat_cleanup - Cleans up all the heartbeat and pending
//     tasks on a close.
//**************************************************************

int mqc_heartbeat_cleanup(gop_mq_conn_t *c)
{
    char *key;
    apr_ssize_t klen;
    apr_hash_index_t *hi, *hit;
    gop_mq_heartbeat_entry_t *entry;
    gop_mq_task_monitor_t *tn;

    //** Clean out the heartbeat info
    //** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
    for (hi = apr_hash_first(NULL, c->heartbeat_dest); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&entry);

        apr_hash_set(c->heartbeat_dest, key, klen, NULL);
        apr_hash_set(c->heartbeat_lut, &(entry->lut_id), sizeof(uint64_t), NULL);
        free(entry->key);
        gop_mq_msg_destroy(entry->address);
        free(entry);
    }

    //** Fail all the commands
    //** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
    for (hit = apr_hash_first(NULL, c->waiting); hit != NULL; hit = apr_hash_next(hit)) {
        apr_hash_this(hit, (const void **)&key, &klen, (void **)&tn);

        //** Clear it out
        apr_hash_set(c->waiting, key, klen, NULL);

        //** Submit the fail task
        log_printf(1, "Failed task uuid=%s\n", c->mq_uuid);
        tbx_log_flush();
        log_printf(1, "Failed task tn->task=%p tn->task->gop=%p\n", tn->task, tn->task->gop);
        tbx_log_flush();
       FATAL_UNLESS(tn->task);
       FATAL_UNLESS(tn->task->gop);
        thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

        //** Free the container. The gop_mq_task_t is handled by the response
        free(tn);
    }

    return(1);
}

//**************************************************************
// mqc_heartbeat_dec - Decrement the hb structure which may result
//    in it's removal.
//**************************************************************

void mqc_heartbeat_dec(gop_mq_conn_t *c, gop_mq_heartbeat_entry_t *hb)
{
    hb->count--;

    if (hb->count <= 0) {  //** Last ref so remove it
        apr_hash_set(c->heartbeat_dest, hb->key, hb->key_size, NULL);
        apr_hash_set(c->heartbeat_lut, &(hb->lut_id), sizeof(uint64_t), NULL);
        free(hb->key);
        gop_mq_msg_destroy(hb->address);
        free(hb);
    }
}


//**************************************************************
// mqc_heartbeat - Do a heartbeat check
//     Scans the destintation table for:
//        1) Dead connections (missed multiple heartbeats)
//        2) Sends heartbeats to inactive connections
//     and sets the next check time
//
//     It also scans the waiting table for NULL address responses
//     If npoll == 2 then it assumes we are winding down the connection
//     and returns 1 when all pending process have been handled or
//     timed out.
//**************************************************************

int mqc_heartbeat(gop_mq_conn_t *c, int npoll)
{
    char *key;
    apr_ssize_t klen;
    apr_hash_index_t *hi, *hit;
    gop_mq_heartbeat_entry_t *entry;
    gop_mq_task_monitor_t *tn;
    apr_time_t dt, dt_fail, dt_check;
    apr_time_t now;
    int n, pending_count, conn_dead, do_conn_hb;
    char b64[1024];

    double dts;
    apr_time_t start = apr_time_now();
    log_printf(6, "START host=%s\n", c->mq_uuid);
    tbx_log_flush();
    dt_fail = apr_time_make(c->pc->heartbeat_failure, 0);
    dt_check = apr_time_make(c->pc->heartbeat_dt, 0);
    pending_count = 0;
    conn_dead = 0;

    do_conn_hb = 0;  //** Keep track of if I'm HBing my direct uplink.

    //** Check the heartbeat dest table
    //** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
    hi = apr_hash_first(NULL, c->heartbeat_dest);
    while (hi != NULL) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&entry);

        now = apr_time_now();
        dt = now - entry->last_check;
        log_printf(7, "hb->key=%s\n", entry->key);
        if (dt > dt_fail) {  //** Dead connection so fail all the commands using it
            if (entry == c->hb_conn) conn_dead = 1;
            klen = apr_time_sec(dt);
            log_printf(8, "hb->key=%s FAIL dt=%zd\n", entry->key, klen);
            log_printf(6, "before waiting size=%d\n", apr_hash_count(c->waiting));
                //** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
                for (hit = apr_hash_first(NULL, c->waiting); hit != NULL; hit = apr_hash_next(hit)) {
                apr_hash_this(hit, (const void **)&key, &klen, (void **)&tn);
                if (tn->tracking == entry) {
                    //** Clear it out
                    apr_hash_set(c->waiting, key, klen, NULL);

                    //** Submit the fail task
                    log_printf(6, "Failed task uuid=%s sid=%s\n", c->mq_uuid, gop_mq_id2str(key, klen, b64, sizeof(b64)));
                    tbx_log_flush();
                    log_printf(6, "Failed task tn->task=%p tn->task->gop=%p\n", tn->task, tn->task->gop);
                    tbx_log_flush();
                   FATAL_UNLESS(tn->task);
                   FATAL_UNLESS(tn->task->gop);
                    thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

                    //** Free the container. The gop_mq_task_t is handled by the response
                    free(tn);
                }
            }

            log_printf(6, "after waiting size=%d\n", apr_hash_count(c->waiting));

            //** Remove the entry and clean up
            apr_hash_set(c->heartbeat_dest, entry->key, entry->key_size, NULL);
            apr_hash_set(c->heartbeat_lut, &(entry->lut_id), sizeof(uint64_t), NULL);
            free(entry->key);
            gop_mq_msg_destroy(entry->address);
            free(entry);
        } else if (dt > dt_check) {  //** Send a heartbeat check
            klen = apr_time_sec(dt);
            log_printf(10, "hb->key=%s CHECK dt=%zd\n", entry->key, klen);
            if ((npoll == 1) && (entry == c->hb_conn)) {
                do_conn_hb = 1;
                goto next;  //** Skip local hb if finished
            }
            c->stats.outgoing[MQS_HEARTBEAT_INDEX]++;
            c->stats.outgoing[MQS_PING_INDEX]++;

            gop_mq_send(c->sock, entry->address, 0);
            pending_count++;
        }

next:
        hi = apr_hash_next(hi);
    }

    //** Do the same for individual commands
    now = apr_time_now();
    //** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
    hi = apr_hash_first(NULL, c->waiting);
    log_printf(6, "before waiting size=%d\n", apr_hash_count(c->waiting));
    while (hi != NULL) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&tn);

        if (now > tn->task->timeout) {  //** Expired command
            if (tn->tracking != NULL) {  //** Tracking so dec the hb handle
                mqc_heartbeat_dec(c, tn->tracking);
            }

            //** Clear it out
            apr_hash_set(c->waiting, key, klen, NULL);

            //** Submit the fail task
            log_printf(6, "Failed task uuid=%s hash_count=%u sid=%s\n", c->mq_uuid, apr_hash_count(c->waiting), gop_mq_id2str(key, klen, b64, sizeof(b64)));
            tbx_log_flush();
            log_printf(6, "Failed task tn->task=%p tn->task->gop=%p gid=%d\n", tn->task, tn->task->gop, gop_id(tn->task->gop));
            tbx_log_flush();
           FATAL_UNLESS(tn->task);
           FATAL_UNLESS(tn->task->gop);
            thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

            //** Free the container. The gop_mq_task_t is handled by the response
            free(tn);
        } else {
            pending_count++;  //** Keep track of pending responses
        }

        hi = apr_hash_next(hi);
    }

    log_printf(6, "after waiting size=%d\n", apr_hash_count(c->waiting));

    if (do_conn_hb == 1) {    //** Check if we HB the main uplink
        if ( ((pending_count == 0) && (npoll > 1)) ||
                (pending_count > 0) ) {
            c->stats.outgoing[MQS_HEARTBEAT_INDEX]++;
            c->stats.outgoing[MQS_PING_INDEX]++;

            gop_mq_send(c->sock, c->hb_conn->address, 0);
            pending_count++;
        }
    }

    //** Determine if it's time to exit
    n = 0;
    if (npoll == 1) {
        if (pending_count == 0) n = 1;
    }

    dts = apr_time_now() - start;
    dts /= APR_USEC_PER_SEC;
    log_printf(10, "pending_count=%d npoll=%d conn_dead=%d do_conn_hb=%d n=%d dt=%lf\n", pending_count, npoll, conn_dead, do_conn_hb, n, dts);
    return(n+conn_dead);
}

//**************************************************************
// mqc_process_incoming - Processes an incoming task
//**************************************************************

int mqc_process_incoming(gop_mq_conn_t *c, int *nproc)
{
    int n, count;
    mq_msg_t *msg;
    gop_mq_frame_t *f;
    gop_mq_task_t *task;
    char *data;
    int size;

    log_printf(5, "processing incoming start\n");
    //** Process all that are on the wire
    msg = gop_mq_msg_new();
    count = 0;
    while ((n = gop_mq_recv(c->sock, msg, MQ_DONTWAIT)) == 0) {
        count++;
        log_printf(5, "Got a message count=%d\n", count);
        //** verify we have an empty frame
        f = gop_mq_msg_first(msg);
        gop_mq_get_frame(f, (void **)&data, &size);
        if (size != 0) {
            log_printf(0, "ERROR: Missing empty frame!\n");
            task = gop_mq_task_new(c->pc->mqc, msg, NULL, c->pc, -1);
            mqt_exec(NULL, task);
            goto skip;
        }

        //** and the correct version
        f = gop_mq_msg_next(msg);
        gop_mq_get_frame(f, (void **)&data, &size);
        if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
            log_printf(0, "ERROR: Invalid version!\n");
            gop_mq_msg_destroy(msg);
            goto skip;
        }

        //** This is the command frame
        f = gop_mq_msg_next(msg);
        gop_mq_get_frame(f, (void **)&data, &size);
        if (mq_data_compare(MQF_PING_KEY, MQF_PING_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_PING_KEY\n");
            tbx_log_flush();
            c->stats.incoming[MQS_PING_INDEX]++;
            mqc_ping(c, msg);
        } else if (mq_data_compare(MQF_PONG_KEY, MQF_PONG_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_PONG_KEY\n");
            tbx_log_flush();
            c->stats.incoming[MQS_PONG_INDEX]++;
            mqc_pong(c, msg);
        } else if (mq_data_compare(MQF_TRACKADDRESS_KEY, MQF_TRACKADDRESS_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_TRACKADDRESS_KEY\n");
            tbx_log_flush();
            c->stats.incoming[MQS_TRACKADDRESS_INDEX]++;
            mqc_trackaddress(c, msg);
        } else if (mq_data_compare(MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_RESPONSE_KEY\n");
            tbx_log_flush();
            c->stats.incoming[MQS_RESPONSE_INDEX]++;
            mqc_response(c, msg, 1);
        } else if ((mq_data_compare(MQF_EXEC_KEY, MQF_EXEC_SIZE, data, size) == 0) ||
                   (mq_data_compare(MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, data, size) == 0)) {

            if (mq_data_compare(MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, data, size) == 0) {
                log_printf(15, "Processing MQF_TRACKEXEC_KEY\n");
                tbx_log_flush();
                c->stats.incoming[MQS_TRACKEXEC_INDEX]++;
            } else {
                log_printf(15, "Processing MQF_EXEC_KEY\n");
                tbx_log_flush();
                c->stats.incoming[MQS_EXEC_INDEX]++;

            }

            //** It's up to the task to send any tracking information back.
            log_printf(5, "Submiting task for execution\n");
            task = gop_mq_task_new(c->pc->mqc, msg, NULL, c->pc, -1);
            thread_pool_direct(c->pc->tp, mqt_exec, task);
        } else {   //** Unknwon command so drop it
            log_printf(5, "ERROR: Unknown command.  Dropping\n");
            c->stats.incoming[MQS_UNKNOWN_INDEX]++;
            gop_mq_msg_destroy(msg);
            goto skip;
        }
skip:
        msg = gop_mq_msg_new(); //**  The old one is destroyed after it's consumed
        if (count > 10) break;  //** Kick out for other processing
    }

    gop_mq_msg_destroy(msg);  //** Clean up

    *nproc += count;  //** Inc processed commands
    log_printf(5, "processing incoming end n=%d\n", n);
    tbx_log_flush();

    return(0);
}

//**************************************************************
// mqc_process_task - Sends the new task
//   npoll -- When processing the task if c->pc->n_close > 0
//   then no tasks is processed but instead n_close is decremented
//   and npoll set to 1 to stop monitoring the incoming task port
//**************************************************************

int mqc_process_task(gop_mq_conn_t *c, int *npoll, int *nproc)
{
    gop_mq_task_t *task = NULL;
    gop_mq_frame_t *f;
    gop_mq_task_monitor_t *tn;
    char b64[1024];
    char *data, v;
    int i, size, tracking;

    //** Read an event
    i = gop_mq_pipe_read(c->pc->efd[0], &v);

    //** Get the new task or start a wind down if requested
    apr_thread_mutex_lock(c->pc->lock);
    if (c->pc->n_close > 0) { //** Wind down request
        c->pc->n_close--;
        *npoll = 1;
    } else {  //** Got a new task
        task = tbx_stack_pop(c->pc->tasks);
    }
    apr_thread_mutex_unlock(c->pc->lock);

    if (i == -1) {
        log_printf(1, "OOPS! read=-1 task=%p!\n", task);
    }

    //** Wind down triggered so return
    if (*npoll == 1) return(0);

    if (task == NULL) {
        log_printf(0, "Nothing to do\n");
        return(0);
    }

    (*nproc)++;  //** Inc processed commands

    //** Convert the MAx exec time in sec to an abs timeout in usec
    task->timeout = apr_time_now() + apr_time_from_sec(task->timeout);


    //** Check if we expect a response
    //** Skip over the address
    f = gop_mq_msg_first(task->msg);
    gop_mq_get_frame(f, (void **)&data, &size);
    log_printf(10, "address length = %d\n", size);
    while ((f != NULL) && (size != 0)) {
        f = gop_mq_msg_next(task->msg);
        gop_mq_get_frame(f, (void **)&data, &size);
        log_printf(10, "length = %d\n", size);
    }
    if (f == NULL) { //** Bad command
        log_printf(0, "Invalid command!\n");
        return(1);
    }

    //** Verify the version
    f = gop_mq_msg_next(task->msg);
    gop_mq_get_frame(f, (void **)&data, &size);
    if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {  //** Bad version number
        log_printf(0, "Invalid version!\n");
        log_printf(0, "length = %d\n", size);
        return(1);
    }

    log_printf(10, "MQF_VERSION_KEY found\n");
    log_printf(5, "task pass_through = %d\n", task->pass_through);
    //** This is the command
    f = gop_mq_msg_next(task->msg);
    gop_mq_get_frame(f, (void **)&data, &size);
    tracking = 0;
    if ( (mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0) && (task->pass_through == 0) ) { //** We track it - But only if it is not a pass-through task
        //** Get the ID here.  The send will munge my frame position
        f = gop_mq_msg_next(task->msg);
        gop_mq_get_frame(f, (void **)&data, &size);
        tracking = 1;

        log_printf(5, "tracking enabled id_size=%d\n", size);

        c->stats.outgoing[MQS_TRACKEXEC_INDEX]++;
    } else if (mq_data_compare(data, size, MQF_EXEC_KEY, MQF_EXEC_SIZE) == 0) { //** We track it
        c->stats.outgoing[MQS_EXEC_INDEX]++;
        log_printf(10, "MQF_EXEC_KEY found, num outgoing EXEC = %d\n", c->stats.outgoing[MQS_EXEC_INDEX]);
    } else if (mq_data_compare(data, size, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE) == 0) { //** Response
        c->stats.outgoing[MQS_RESPONSE_INDEX]++;
        log_printf(10, "MQF_RESPONSE_KEY found, num outgoing RESPONSE = %d\n", c->stats.outgoing[MQS_RESPONSE_INDEX]);
    } else if (mq_data_compare(data, size, MQF_PING_KEY, MQF_PING_SIZE) == 0) {
        c->stats.outgoing[MQS_PING_INDEX]++;
        log_printf(10, "MQF_PING_KEY found, num outgoing PING = %d\n", c->stats.outgoing[MQS_PING_INDEX]);
    } else if (mq_data_compare(data, size, MQF_PONG_KEY, MQF_PONG_SIZE) == 0) {
        c->stats.outgoing[MQS_PONG_INDEX]++;
        log_printf(10, "MQF_PONG_KEY found, num outgoing PONG = %d\n", c->stats.outgoing[MQS_PONG_INDEX]);
    } else {
        c->stats.outgoing[MQS_UNKNOWN_INDEX]++;
        log_printf(10, "Unknown key found! key = %s\n", data);
    }

    //** Send it on
    i = gop_mq_send(c->sock, task->msg, 0);
    if (i == -1) {
        log_printf(0, "Error sending msg! errno=%d\n", errno);
        mq_task_complete(c, task, OP_STATE_FAILURE);
        return(1);
    }

    if (tracking == 0) {     //** Exec the callback if not tracked
        mq_task_complete(c, task, OP_STATE_SUCCESS);
    } else {                 //** Track the task
        log_printf(1, "TRACKING id_size=%d sid=%s\n", size, gop_mq_id2str(data, size, b64, sizeof(b64)));
        if (task->gop != NULL) log_printf(1, "TRACKING gid=%d\n", gop_id(task->gop));
        //** Insert it in the monitoring table
        tbx_type_malloc_clear(tn, gop_mq_task_monitor_t, 1);
        tn->task = task;
        tn->id = data;
        tn->id_size = size;
        tn->last_check = apr_time_now();
        apr_hash_set(c->waiting,  tn->id, tn->id_size, tn);
    }

    return(0);
}

//**************************************************************
// mq_conn_make - Makes the actual connection.  Returns 0 for
//    success and 1 for failure.
//**************************************************************

int mq_conn_make(gop_mq_conn_t *c)
{
    gop_mq_pollitem_t pfd;
    int err, n, frame;
    mq_msg_t *msg = NULL;
    gop_mq_frame_t *f;
    char *data;
    apr_time_t start, dt;
    gop_mq_heartbeat_entry_t *hb;

    log_printf(5, "START host=%s\n", c->pc->host);

    //** Determing the type of socket to make based on
    //** the gop_mq_conn_t* passed in
    //** Old version:
    //** c->sock = gop_mq_socket_new(c->pc->ctx, MQ_TRACE_ROUTER);
    //** Hardcoded MQ_TRACE_ROUTER socket type
    c->sock = gop_mq_socket_new(c->pc->ctx, c->pc->socket_type);
    log_printf(0, "host = %s, connect_mode = %d\n", c->pc->host, c->pc->connect_mode);
    if (c->pc->connect_mode == MQ_CMODE_CLIENT) {
        err = gop_mq_connect(c->sock, c->pc->host);
    } else {
        err = gop_mq_bind(c->sock, c->pc->host);
    }

    size_t s;
    zmq_getsockopt(c->sock->arg, ZMQ_IDENTITY, &c->mq_uuid, &s);
    if (s <= 0) c->mq_uuid = "ERROR_GETTING_IDENTITY";

    if (err != 0) return(1);
    if (c->pc->connect_mode == MQ_CMODE_SERVER) return(0);  //** Nothing else to do

    err = 1; //** Defaults to failure
    frame = -1;

    //** Form the ping message and make the base hearbeat message
    tbx_type_malloc_clear(hb, gop_mq_heartbeat_entry_t, 1);
    hb->key = strdup(c->pc->host);
    hb->key_size = strlen(c->pc->host);
    hb->lut_id = tbx_atomic_global_counter();
    hb->count = 1;

    //** This is the ping message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, c->pc->host, strlen(c->pc->host), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, &(hb->lut_id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    hb->address = msg;
    c->hb_conn = hb;

    msg = gop_mq_msg_new();  //** This is for the pong response

    //** Finish creating the structure
    apr_hash_set(c->heartbeat_dest, hb->key, hb->key_size, hb);
    apr_hash_set(c->heartbeat_lut, &(hb->lut_id), sizeof(uint64_t), hb);
    hb->last_check = apr_time_now();

    //** Send it
    pfd.socket = gop_mq_poll_handle(c->sock);
    pfd.events = MQ_POLLOUT;

    start = apr_time_now();
    c->stats.outgoing[MQS_PING_INDEX]++;
    c->stats.outgoing[MQS_HEARTBEAT_INDEX]++;
    while (gop_mq_send(c->sock, hb->address, MQ_DONTWAIT) != 0) {
        dt = apr_time_now() - start;
        dt = apr_time_sec(dt);
        if (dt > 5) {
            log_printf(0, "ERROR: Failed sending task to host=%s\n", c->pc->host);
            goto fail;
        }

        gop_mq_poll(&pfd, 1, 1000);
    }

    //** Wait for a connection
    pfd.socket = gop_mq_poll_handle(c->sock);
    pfd.events = MQ_POLLIN;

    start = apr_time_now();
    dt = 0;
    frame = -1;
    while (dt < 10) {
        gop_mq_poll(&pfd, 1, 1000);
        if (gop_mq_recv(c->sock, msg, MQ_DONTWAIT) == 0) {
            f = gop_mq_msg_first(msg);
            frame = 1;
            gop_mq_get_frame(f, (void **)&data, &n);
            if (n != 0) goto fail;

            f = gop_mq_msg_next(msg);
            frame = 1;
            gop_mq_get_frame(f, (void **)&data, &n);
            if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) goto fail;

            f = gop_mq_msg_next(msg);
            frame = 2;
            gop_mq_get_frame(f, (void **)&data, &n);
            if (mq_data_compare(data, n, MQF_PONG_KEY, MQF_PONG_SIZE) != 0) goto fail;

            f = gop_mq_msg_next(msg);
            frame = 3;
            gop_mq_get_frame(f, (void **)&data, &n);
            if (mq_data_compare(data, n, &(hb->lut_id), sizeof(uint64_t)) != 0) goto fail;

            err = 0;  //** Good pong response
            frame = 0;
            hb->last_check = apr_time_now();
            c->stats.incoming[MQS_PONG_INDEX]++;
            break;
        }

        dt = apr_time_now() - start;
        dt = apr_time_sec(dt);
    }

fail:
    log_printf(5, "END status=%d dt=%" APR_TIME_T_FMT " frame=%d\n", err, dt, frame);
    gop_mq_msg_destroy(msg);
    return(err);
}

//**************************************************************
// gop_mq_conn_thread - Connection thread
//**************************************************************

void *gop_mq_conn_thread(apr_thread_t *th, void *data)
{
    gop_mq_conn_t *c = (gop_mq_conn_t *)data;
    int k, npoll, err, finished, nprocessed, nproc, nincoming, slow_exit, oops;
    long int heartbeat_ms;
    int64_t total_proc, total_incoming;
    gop_mq_pollitem_t pfd[3];
    apr_time_t next_hb_check, last_check;
    double proc_rate, dt;
    char v;

    log_printf(2, "START: host=%s heartbeat_dt=%d\n", c->pc->host, c->pc->heartbeat_dt);
    //** Try and make the connection
    //** Right now the portal is locked so this routine can assume that.
    oops = err = mq_conn_make(c);
    log_printf(2, "START(2): uuid=%s oops=%d\n", c->mq_uuid, oops);


    //** Notify the parent about the connections status via c->cefd
    //** It is then safe to manipulate c->pc->lock
    v = (err == 0) ? 1 : 2;  //** Make 1 success and 2 failure

    log_printf(5, "after conn_make err=%d\n", err);

    if (c->cefd[0] != -1) write(c->cefd[1], &v, 1);

    total_proc = total_incoming = 0;
    slow_exit = 0;
    nprocessed = 0;

    if (err != 0) goto cleanup;  //** if no connection shutdown

    //**Make the poll structure
    memset(pfd, 0, sizeof(pfd));
    gop_mq_pipe_poll_store(&(pfd[PI_EFD]), c->pc->efd[0], MQ_POLLIN);
    pfd[PI_CONN].socket = gop_mq_poll_handle(c->sock);
    pfd[PI_CONN].events = MQ_POLLIN;

    //** Main processing loop
    finished = 0;
    heartbeat_ms = c->pc->heartbeat_dt * 1000;
    npoll = 2;
    next_hb_check = apr_time_now() + apr_time_from_sec(1);
    last_check = apr_time_now();

    do {
        k = gop_mq_poll(pfd, npoll, heartbeat_ms);
        log_printf(5, "pfd[EFD]=%d pdf[CONN]=%d npoll=%d n=%d errno=%d\n", pfd[PI_EFD].revents, pfd[PI_CONN].revents, npoll, k, errno);

        //k=1; //FIXME
        if (k > 0) {  //** Got an event so process it
            nproc = 0;
            if ((npoll == 2) && (pfd[PI_EFD].revents != 0)) finished += mqc_process_task(c, &npoll, &nproc);
            nprocessed += nproc;
            total_proc += nproc;
            //finished += mqc_process_task(c, &npoll, &nprocessed);
            log_printf(5, "after process_task finished=%d\n", finished);
            nincoming = 0;
            if (pfd[PI_CONN].revents != 0) finished += mqc_process_incoming(c, &nincoming);
            //finished += mqc_process_incoming(c, &nincoming);
            nprocessed += nincoming;
            total_incoming += nincoming;
            log_printf(5, "after process_incoming finished=%d\n", finished);
        } else if (k < 0) {
            log_printf(0, "ERROR on socket uuid=%s errno=%d\n", c->mq_uuid, errno);
            tbx_log_flush();
            goto cleanup;
        }

        if ((apr_time_now() > next_hb_check) || (npoll == 1)) {
            finished += mqc_heartbeat(c, npoll);
            log_printf(5, "after heartbeat finished=%d\n", finished);

            log_printf(5, "hb_old=%" APR_TIME_T_FMT "\n", next_hb_check);
            next_hb_check = apr_time_now() + apr_time_from_sec(1);
            log_printf(5, "hb_new=%" APR_TIME_T_FMT "\n", next_hb_check);

            //** Check if we've been busy enough to stay open
            dt = apr_time_now() - last_check;
            dt = dt / APR_USEC_PER_SEC;
            proc_rate = (1.0*nprocessed) / dt;
            log_printf(5, "processing rate=%lf nproc=%d dt=%lf\n", proc_rate, nprocessed, dt);
            if ((proc_rate < c->pc->min_ops_per_sec) && (slow_exit == 0)) {
                apr_thread_mutex_lock(c->pc->lock);
                if (c->pc->active_conn > 1) {
                    log_printf(5, "processing rate=%lf curr_con=%d\n", proc_rate, c->pc->active_conn);
                    slow_exit = 1;
                    npoll = 1;  //** Don't get any new commands.  Just process the ones I already have
                    c->pc->active_conn--;  //** We do this hear so any other threads see me exiting
                }
                apr_thread_mutex_unlock(c->pc->lock);
            }

            nprocessed = 0;
            last_check = apr_time_now();

            if ((finished == 0) && (npoll == 1)) sleep(1);  //** Wantto exit but have some commands pending
        }
    } while (finished == 0);


cleanup:
    //** Cleanup my struct but don'r free(c).
    //** This is done on portal cleanup
    gop_mq_stats_print(2, c->mq_uuid, &(c->stats));
    log_printf(2, "END: uuid=%s total_incoming=" I64T " total_processed=" I64T " oops=%d\n", c->mq_uuid, total_incoming, total_proc, oops);
    tbx_log_flush();

    //** Make sure the creating parent thread has read the pipe before tearing down
    if (c->cefd[0] != -1) {
        struct pollfd pfd2;
        pfd2.fd = c->cefd[0]; pfd2.events = POLLIN;
        while (poll(&pfd2, 1, 0) == 1) {
            usleep(1000);
        }
    }

    gop_mq_conn_teardown(c);

    //** Update the conn_count, stats and place mysealf on the reaper stack
    apr_thread_mutex_lock(c->pc->lock);
    gop_mq_stats_add(&(c->pc->stats), &(c->stats));
    //** We only update the connection counts if we actually made a connection.  The original thread that created us was already notified
    //** if we made a valid connection and it increments the connection counts.
    if (oops == 0) {
        if (slow_exit == 0) c->pc->active_conn--;
        c->pc->total_conn--;
    }
    if (c->pc->total_conn == 0) apr_thread_cond_signal(c->pc->cond);
    tbx_stack_push(c->pc->closed_conn, c);
    apr_thread_mutex_unlock(c->pc->lock);

    log_printf(2, "END: final\n");
    tbx_log_flush();


    return(NULL);
}


//**************************************************************
// mq_conn_create_actual - This routine does the actual connection creation
//      and optionally waits for the connection to complete if dowait=1.
//
//   NOTE:  Assumes p->lock is set on entry.
//**************************************************************

int mq_conn_create_actual(gop_mq_portal_t *p, int dowait)
{
    gop_mq_conn_t *c;
    int err;
    char v;

    tbx_type_malloc_clear(c, gop_mq_conn_t, 1);

    c->pc = p;
    assert_result(apr_pool_create(&(c->mpool), NULL), APR_SUCCESS);
    assert_result_not_null(c->waiting = apr_hash_make(c->mpool));
    assert_result_not_null(c->heartbeat_dest = apr_hash_make(c->mpool));
    assert_result_not_null(c->heartbeat_lut = apr_hash_make(c->mpool));

    //** This is just used in the initial handshake
    if (dowait == 1) {
        assert_result(pipe(c->cefd), 0);
    } else {
        c->cefd[0] = -1; c->cefd[1] = -1;
    }

    //** Spawn the thread
    //** USe the parent mpool so I can do the teardown
    tbx_thread_create_assert(&(c->thread), NULL, gop_mq_conn_thread, (void *)c, p->mpool);
    err = 0;
    if (dowait == 1) {  //** If needed wait until connected
        read(c->cefd[0], &v, 1);
        //** n==1 is a success anything else is an error
        err = (v == 1) ? 0 : 1;
    }

    if (err == 0) {
        p->active_conn++; //** Inc the number of connections on success
        p->total_conn++;
    }

    return(err);
}

//**************************************************************
// mq_conn_create - Creates a new connection and optionally waits
//     for the connection to complete if dowait=1.
//     This is a wrapper around the actual connection creation
//
//   NOTE:  Assumes p->lock is set on entry.
//**************************************************************

int mq_conn_create(gop_mq_portal_t *p, int dowait)
{
    int err, retry;

    for (retry=0; retry<3; retry++) {
        err = mq_conn_create_actual(p, dowait);
        log_printf(1, "retry=%d err=%d host=%s\n", retry, err, p->host);

        if (err == 0) break;  //** Kick out if we got a good connection
        apr_sleep(apr_time_from_sec(2));
    }

    return(err);
}

//**************************************************************
// gop_mq_conn_teardown - Tearsdown the MQ connection structures
//    Does not destroy the gop_mq_conn_t structure itself.  This is
//    handled when the connection is reaped.
//**************************************************************

void gop_mq_conn_teardown(gop_mq_conn_t *c)
{
    mqc_heartbeat_cleanup(c);

    apr_hash_clear(c->waiting);
    apr_hash_clear(c->heartbeat_dest);
    apr_hash_clear(c->heartbeat_lut);
    apr_pool_destroy(c->mpool);
    if (c->cefd[0] != -1) {
        close(c->cefd[0]), close(c->cefd[0]);
    }
    if (c->sock != NULL) gop_mq_socket_destroy(c->pc->ctx, c->sock);
}

//**************************************************************
// _mq_reap_closed - Reaps closed connections.
//    NOTE:  Assumes the portal is already locked!
//**************************************************************

void _mq_reap_closed(gop_mq_portal_t *p)
{
    gop_mq_conn_t *c;
    apr_status_t dummy;

    while ((c = tbx_stack_pop(p->closed_conn)) != NULL) {
        apr_thread_join(&dummy, c->thread);
        free(c);
    }
}

//**************************************************************
// gop_mq_portal_destroy - Destroys the MQ portal
//**************************************************************

void gop_mq_portal_destroy(gop_mq_portal_t *p)
{
    int i, n;
    char c;

    //** Tell how many connections to close
    apr_thread_mutex_lock(p->lock);
    log_printf(2, "host=%s active_conn=%d total_conn=%d\n", p->host, p->active_conn, p->total_conn);
    tbx_log_flush();
    p->n_close = p->active_conn;
    n = p->n_close;
    apr_thread_mutex_unlock(p->lock);

    //** Signal them
    c = 1;
    for (i=0; i<n; i++) gop_mq_pipe_write(p->efd[1], &c);

    //** Wait for them all to complete
    apr_thread_mutex_lock(p->lock);
    while (p->total_conn > 0) {
        apr_thread_cond_wait(p->cond, p->lock);
    }
    apr_thread_mutex_unlock(p->lock);

    log_printf(2, "host=%s closed_size=%d total_conn=%d\n", p->host, tbx_stack_count(p->closed_conn), p->total_conn);
    tbx_log_flush();

    //** Clean up
    //** Don;t have to worry about locking cause no one else exists


    _mq_reap_closed(p);
    //** Destroy the command table
    gop_mq_command_table_destroy(p->command_table);

    //** Update the stats
    apr_thread_mutex_lock(p->mqc->lock);
    gop_mq_stats_add(&(p->mqc->stats), &(p->stats));
    apr_thread_mutex_unlock(p->mqc->lock);

    gop_mq_stats_print(2, p->host, &(p->stats));

    apr_thread_mutex_destroy(p->lock);
    apr_thread_cond_destroy(p->cond);
    apr_pool_destroy(p->mpool);

    gop_mq_pipe_destroy(p->ctx, p->efd);
    if (p->ctx != NULL) gop_mq_socket_context_destroy(p->ctx);

    tbx_stack_free(p->closed_conn, 0);
    tbx_stack_free(p->tasks, 0);

    free(p->host);
    free(p);
}

//**************************************************************
// gop_mq_portal_lookup - Looks up a portal context
//**************************************************************

gop_mq_portal_t *gop_mq_portal_lookup(gop_mq_context_t *mqc, char *hostname, gop_mq_cmode_t connect_mode)
{
    apr_hash_t *ptable;
    gop_mq_portal_t *p;

    apr_thread_mutex_lock(mqc->lock);
    ptable = (connect_mode == MQ_CMODE_CLIENT) ? mqc->client_portals : mqc->server_portals;
    p = (gop_mq_portal_t *)(apr_hash_get(ptable, hostname, APR_HASH_KEY_STRING));
    apr_thread_mutex_unlock(mqc->lock);

    return(p);
}

//**************************************************************
// gop_mq_portal_command_table - Retrieves the portal command table
//**************************************************************

gop_mq_command_table_t *gop_mq_portal_command_table(gop_mq_portal_t *portal)
{
    return(portal->command_table);
}

//**************************************************************
// gop_mq_portal_remove - Removes a server portal in the context
//**************************************************************

void gop_mq_portal_remove(gop_mq_context_t *mqc, gop_mq_portal_t *p)
{
    apr_thread_mutex_lock(mqc->lock);
    apr_hash_set(mqc->server_portals, p->host, APR_HASH_KEY_STRING, NULL);
    apr_thread_mutex_unlock(mqc->lock);
}

//**************************************************************
// gop_mq_portal_install - Installs a server portal into the context
//**************************************************************

int gop_mq_portal_install(gop_mq_context_t *mqc, gop_mq_portal_t *p)
{

    gop_mq_portal_t *p2;
    int err;
    apr_hash_t *ptable;

    err = 0;

    apr_thread_mutex_lock(mqc->lock);
    ptable = (p->connect_mode == MQ_CMODE_CLIENT) ? mqc->client_portals : mqc->server_portals;
    p2 = (gop_mq_portal_t *)(apr_hash_get(ptable, p->host, APR_HASH_KEY_STRING));
    if (p2 != NULL) {
        apr_thread_mutex_unlock(mqc->lock);
        return(1);
    }

    //** Make a connection if non exists
    apr_thread_mutex_lock(p->lock);

    apr_hash_set(ptable, p->host, APR_HASH_KEY_STRING, p);
    if (p->active_conn == 0) {
        err = mq_conn_create(p, 1);
    }

    apr_thread_mutex_unlock(p->lock);
    apr_thread_mutex_unlock(mqc->lock);

    return(err);
}

//**************************************************************
// gop_mq_portal_create - Creates a new MQ portal
//**************************************************************

gop_mq_portal_t *gop_mq_portal_create(gop_mq_context_t *mqc, char *host, gop_mq_cmode_t connect_mode)
{
    gop_mq_portal_t *p;

    log_printf(15, "New portal host=%s\n", host);

    tbx_type_malloc_clear(p, gop_mq_portal_t, 1);

    p->mqc = mqc;
    p->host = strdup(host);
    p->command_table = gop_mq_command_table_new(NULL, NULL);

    if (connect_mode == MQ_CMODE_CLIENT) {
        p->min_conn = mqc->min_conn;
        p->max_conn = mqc->max_conn;
    } else {
        p->min_conn = 1;
        p->max_conn = 1;
    }

    p->heartbeat_dt = mqc->heartbeat_dt;
    p->heartbeat_failure = mqc->heartbeat_failure;
    p->backlog_trigger = mqc->backlog_trigger;
    p->min_ops_per_sec = mqc->min_ops_per_sec;
    p->socket_type = mqc->socket_type;                   // socket type
    p->connect_mode = connect_mode;
    p->tp = mqc->tp;

    p->ctx = gop_mq_socket_context_new();

    apr_pool_create(&(p->mpool), NULL);
    apr_thread_mutex_create(&(p->lock), APR_THREAD_MUTEX_DEFAULT, p->mpool);
    apr_thread_cond_create(&(p->cond), p->mpool);

    gop_mq_pipe_create(p->ctx, p->efd);

    p->tasks = tbx_stack_new();
    p->closed_conn = tbx_stack_new();

    return(p);
}

//**************************************************************
// gop_mq_destroy_context - Destroys the MQ context
//**************************************************************

void gop_mq_destroy_context(gop_mq_context_t *mqc)
{
    apr_hash_index_t *hi;
    gop_mq_portal_t *p;
    void *val;

    log_printf(5, "Shutting down client_portals\n");
    tbx_log_flush();
    for (hi=apr_hash_first(mqc->mpool, mqc->client_portals); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        p = (gop_mq_portal_t *)val;
        apr_hash_set(mqc->client_portals, p->host, APR_HASH_KEY_STRING, NULL);
        log_printf(5, "destroying p->host=%s\n", p->host);
        tbx_log_flush();
        gop_mq_portal_destroy(p);
    }
    log_printf(5, "Shutting down server_portals\n");
    tbx_log_flush();
    for (hi=apr_hash_first(mqc->mpool, mqc->server_portals); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        p = (gop_mq_portal_t *)val;
        apr_hash_set(mqc->server_portals, p->host, APR_HASH_KEY_STRING, NULL);
        log_printf(5, "destroying p->host=%s\n", p->host);
        tbx_log_flush();
        gop_mq_portal_destroy(p);
    }
    log_printf(5, "Completed portal shutdown\n");
    tbx_log_flush();

    gop_mq_stats_print(2, "Portal total", &(mqc->stats));

    apr_hash_clear(mqc->client_portals);

    gop_tp_context_destroy(mqc->tp);

    apr_thread_mutex_destroy(mqc->lock);
    apr_pool_destroy(mqc->mpool);

    free(mqc);

    log_printf(5, "AFTER SLEEP2\n");
    tbx_log_flush();
}

//**************************************************************
// _gop_mq_submit - GOP submit routine for MQ objects
//**************************************************************

void _gop_mq_submit_op(void *arg, gop_op_generic_t *gop)
{
    gop_thread_pool_op_t *op = gop_get_tp(gop);
    gop_mq_task_t *task = (gop_mq_task_t *)op->arg;

    log_printf(15, "gid=%d\n", gop_id(gop));

    mq_task_send(task->ctx, task);
}

//**************************************************************
//  gop_mq_create_context - Creates a new MQ pool
//**************************************************************

gop_mq_context_t *gop_mq_create_context(tbx_inip_file_t *ifd, char *section)
{
    gop_mq_context_t *mqc;

    tbx_type_malloc_clear(mqc, gop_mq_context_t, 1);

    mqc->min_conn = tbx_inip_get_integer(ifd, section, "min_conn", 1);
    mqc->max_conn = tbx_inip_get_integer(ifd, section, "max_conn", 3);
    mqc->min_threads = tbx_inip_get_integer(ifd, section, "min_threads", 2);
    mqc->max_threads = tbx_inip_get_integer(ifd, section, "max_threads", 20);
    mqc->max_recursion = tbx_inip_get_integer(ifd, section, "max_recursion", 5);
    mqc->backlog_trigger = tbx_inip_get_integer(ifd, section, "backlog_trigger", 100);
    mqc->heartbeat_dt = tbx_inip_get_integer(ifd, section, "heartbeat_dt", 5);
    mqc->heartbeat_failure = tbx_inip_get_integer(ifd, section, "heartbeat_failure", 60);
    mqc->min_ops_per_sec = tbx_inip_get_integer(ifd, section, "min_ops_per_sec", 100);

    // New socket_type parameter
    mqc->socket_type = tbx_inip_get_integer(ifd, section, "socket_type", MQ_TRACE_ROUTER);

    apr_pool_create(&(mqc->mpool), NULL);
    apr_thread_mutex_create(&(mqc->lock), APR_THREAD_MUTEX_DEFAULT, mqc->mpool);

    //** Make the thread pool.  All GOP commands run through here.  We replace
    //**  the TP submit routine with our own.
    mqc->tp = gop_tp_context_create("mq", mqc->min_threads, mqc->max_threads, mqc->max_recursion);
    mqc->pcfn = *(mqc->tp->pc->fn);
    mqc->pcfn.submit = _gop_mq_submit_op;
    mqc->pcfn.sync_exec = NULL;
    mqc->tp->pc->fn = &(mqc->pcfn);
    assert_result_not_null(mqc->client_portals = apr_hash_make(mqc->mpool));
    assert_result_not_null(mqc->server_portals = apr_hash_make(mqc->mpool));

    tbx_atomic_set(mqc->n_ops, 0);

    return(mqc);
}

