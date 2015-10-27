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

#include "log.h"
#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "type_malloc.h"
#include "apr_base64.h"
#include "apr_wrapper.h"
#include "random.h"
#include "fmttypes.h"

//** Poll index for connection monitoring
#define PI_CONN 0   //** Actual connection
#define PI_EFD  1   //** Portal event FD for incoming tasks

int mq_conn_create(mq_portal_t *p, int dowait);
void mq_conn_teardown(mq_conn_t *c);
void mqc_heartbeat_dec(mq_conn_t *c, mq_heartbeat_entry_t *hb);
void _mq_reap_closed(mq_portal_t *p);
void *mqtp_failure(apr_thread_t *th, void *arg);


//--------------------------------------------------------------
//  Code for performing MQ portal <-> client communication.
//  There are 2 methods for this.  The most efficient is to use
//  pipe() and do simple write/read/poll communications with both
//  normal file descriptors and network sockets.  But this only works
//  on systems that have BSD style sockets.  Which is all the *nix
//  variants.
//
//  On MS windows we instead use a ZMQ PAIR type socket.
//--------------------------------------------------------------

#ifndef MQ_PIPE_COMM
//---------------Use ZMQ_PAIR for MS Windows--------------------

void mq_pipe_create(mq_socket_context_t *ctx, mq_socket_t **pfd)
{
    char hname[257];
    uint64_t r = 0;

    int result = (pfd[0] = mq_socket_new(ctx, MQ_PAIR));
    assert(result != NULL);
    int result = (pfd[1] = mq_socket_new(ctx, MQ_PAIR));
    assert(result != NULL);
 
    //** Connect them together
    get_random(&r, sizeof(r));
    snprintf(hname, sizeof(hname), "inproc://" LU, r);
    result = mq_bind(pfd[1], hname);
    assert(result == 0);
    result = mq_connect(pfd[0], hname);
    assert(result == 0);
}

void mq_pipe_poll_store(mq_pollitem_t *pfd, mq_socket_t *sock, int mode)
{
    pfd->socket = mq_poll_handle(sock);
    pfd->events = mode;
}

void mq_pipe_destroy(mq_socket_context_t *ctx, mq_socket_t **pfd)
{
    if (pfd[0] != NULL) mq_socket_destroy(ctx, pfd[0]);
    if (pfd[1] != NULL) mq_socket_destroy(ctx, pfd[1]);
}

int mq_pipe_read(mq_socket_t *sock, char *buf)
{
    int nbytes = 0;
    int len = 1;
    mq_msg_t *msg = mq_msg_new();
    mq_frame_t *f;

    if (mq_recv(sock, msg, 0) == 0) {  //** Got something
        f = mq_msg_first(msg);
        nbytes = (f->len > len) ? len : f->len;
        if (nbytes > 0) memcpy(buf, f->data, nbytes);
    }

    mq_msg_destroy(msg);

    return(nbytes);
}

int mq_pipe_write(mq_socket_t *sock, char *buf)
{
    int nbytes = 0;
    int len = 1;
    mq_msg_t *msg = mq_msg_new();
    mq_frame_t *f = mq_frame_new(buf, len, MQF_MSG_KEEP_DATA);

    mq_msg_append_frame(msg, f);
    if (mq_send(sock, msg, 0) == 0) nbytes = len;

    mq_msg_destroy(msg);

    return(nbytes);
}

#endif

//------------------- mq_pipe_*() end ------------------------------

//**************************************************************
// mq_id2str - Convert the command id to a printable string
//**************************************************************

char *mq_id2str(char *id, int id_len, char *str, int str_len)
{
    assert(str_len > 2*id_len+1);
    apr_base64_encode(str, id, id_len);

    return(str);
}

//**************************************************************
// mq_stats_add - Add command stats together (a = a+b)
//**************************************************************

void mq_stats_add(mq_command_stats_t *a, mq_command_stats_t *b)
{
    int i;

    for (i=0; i<MQS_SIZE; i++) {
        a->incoming[i] += b->incoming[i];
        a->outgoing[i] += b->outgoing[i];
    }
}

//**************************************************************
//  mq_stats_print - Prints the stats
//**************************************************************

void mq_stats_print(int ll, char *tag, mq_command_stats_t *a)
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

mq_command_t *mq_command_new(void *cmd, int cmd_size, void *arg, mq_fn_exec_t *fn)
{
    mq_command_t *mqc;

    type_malloc(mqc, mq_command_t, 1);

    type_malloc(mqc->cmd, void, cmd_size);
    memcpy(mqc->cmd, cmd, cmd_size);

    mqc->cmd_size = cmd_size;
    mqc->arg = arg;
    mqc->fn = fn;

    return(mqc);
}

//**************************************************************
//  mq_command_set - Adds/removes and RPC call to the local host
//**************************************************************

void mq_command_set(mq_command_table_t *table, void *cmd, int cmd_size, void *arg, mq_fn_exec_t *fn)
{
    mq_command_t *mqc;

    log_printf(15, "command key = %d\n", ((char *)cmd)[0]);
    apr_thread_mutex_lock(table->lock);
    if (fn != NULL) {
        mqc = apr_hash_get(table->table, cmd, cmd_size);
        if (mqc != NULL) {
            apr_hash_set(table->table, mqc->cmd, mqc->cmd_size, NULL);
            free(mqc->cmd);
            free(mqc);
        }

        mqc = mq_command_new(cmd, cmd_size, arg, fn);
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
//  mq_command_table_new - Creates a new RPC table
//**************************************************************

void mq_command_table_set_default(mq_command_table_t *table, void *arg, mq_fn_exec_t *fn_default)
{
    apr_thread_mutex_lock(table->lock);
    table->fn_default = fn_default;
    table->arg_default = arg;
    apr_thread_mutex_unlock(table->lock);

}


//**************************************************************
//  mq_command_table_new - Creates a new RPC table
//**************************************************************

mq_command_table_t *mq_command_table_new(void *arg, mq_fn_exec_t *fn_default)
{
    mq_command_table_t *t;
    
    type_malloc(t, mq_command_table_t, 1);
    
    t->fn_default = fn_default;
    t->arg_default = arg;
    apr_pool_create(&(t->mpool), NULL);
    int result = apr_thread_mutex_create(&(t->lock), APR_THREAD_MUTEX_DEFAULT, t->mpool);
    assert(result == APR_SUCCESS);
    result = (t->table = apr_hash_make(t->mpool)); assert(result != NULL); }
    
    return(t);
}

//**************************************************************
//  mq_command_table_destroy- Destroys an RPC table
//**************************************************************

void mq_command_table_destroy(mq_command_table_t *t)
{
    apr_hash_index_t *hi;
    mq_command_t *cmd;
    void *val;

    for (hi=apr_hash_first(t->mpool, t->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        cmd = (mq_command_t *)val;
        apr_hash_set(t->table, cmd->cmd, cmd->cmd_size, NULL);
        free(cmd->cmd);
        free(cmd);
    }

    apr_pool_destroy(t->mpool);
    free(t);

    return;
}

//**************************************************************
//  mq_command_exec - Executes an RPC call
//**************************************************************

void mq_command_exec(mq_command_table_t *t, mq_task_t *task, void *key, int klen)
{
    mq_command_t *cmd;

    cmd = apr_hash_get(t->table, key, klen);

    log_printf(3, "cmd=%p klen=%d\n", cmd, klen);
    if (cmd == NULL) {
        log_printf(0, "Unknown command!\n");
//display_msg_frames(task->msg); //testing to see if the worker is returning a message and it's getting dropped here
        if (t->fn_default != NULL) t->fn_default(t->arg_default, task);
    } else {
        cmd->fn(cmd->arg, task);
    }
}

//**************************************************************
// mq_submit - Submits a task for processing
//**************************************************************

int mq_submit(mq_portal_t *p, mq_task_t *task)
{
    char c;
    int backlog, err;
    mq_task_t *t;
    apr_thread_mutex_lock(p->lock);

//** Do a quick check for connections that need to be reaped
    if (stack_size(p->closed_conn) > 0) _mq_reap_closed(p);

//** Add the task and get the backlog
    move_to_bottom(p->tasks);
    insert_below(p->tasks, task);
    backlog = stack_size(p->tasks);
    log_printf(2, "portal=%s backlog=%d active_conn=%d max_conn=%d total_conn=%d\n", p->host, backlog, p->active_conn, p->max_conn, p->total_conn);
    flush_log();

//** Noitify the connections
    c = 1;
    mq_pipe_write(p->efd[1], &c);

//** Check if we need more connections
    err = 0;
    if (backlog > p->backlog_trigger) {
        if (p->total_conn == 0) { //** No current connections so try and make one
            err = mq_conn_create(p, 1);
            if (err != 0) {  //** Fail everything
                log_printf(1, "Host is dead so failing tasks host=%s\n", p->host);
                while ((t = pop(p->tasks)) != NULL) {
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
            while ((t = pop(p->tasks)) != NULL) {
                thread_pool_direct(p->tp, mqtp_failure, t);
            }
        }
    }

    log_printf(2, "END portal=%s err=%d backlog=%d active_conn=%d total_conn=%d max_conn=%d\n", p->host, err, backlog, p->active_conn, p->total_conn, p->max_conn);
    flush_log();

    apr_thread_mutex_unlock(p->lock);

    return(0);
}

//**************************************************************
// mq_task_send - Sends a task for processing
//**************************************************************

int mq_task_send(mq_context_t *mqc, mq_task_t *task)
{
    mq_portal_t *p;
    mq_frame_t *f;
    char *host;
    int size;

    f = mq_msg_first(task->msg);

    if (f == NULL) return(1);

    mq_get_frame(f, (void **)&host, &size);

//** Look up the portal
    apr_thread_mutex_lock(mqc->lock);
    p = (mq_portal_t *)(apr_hash_get(mqc->client_portals, host, size));
    if (p == NULL) {  //** New host so create the portal
        log_printf(10, "Creating MQ_CMODE_CLIENT portal for outgoing connections host = %s size = %d\n", host, size);
        p = mq_portal_create(mqc, host, MQ_CMODE_CLIENT);
        apr_hash_set(mqc->client_portals, p->host, APR_HASH_KEY_STRING, p);
    }
    apr_thread_mutex_unlock(mqc->lock);

    return(mq_submit(p, task));
}

//**************************************************************
//  mq_task_destroy - Destroys an MQ task
//**************************************************************

void mq_task_destroy(mq_task_t *task)
{
    if (task->msg != NULL) mq_msg_destroy(task->msg);
    if (task->response != NULL) mq_msg_destroy(task->response);
    if (task->my_arg_free) task->my_arg_free(task->arg);
    free(task);
}

//**************************************************************
// mq_arg_free - Called by GOP routines on destruction
//**************************************************************

void mq_arg_free(void *arg)
{
    mq_task_t *task = (mq_task_t *)arg;

    mq_task_destroy(task);
}


//**************************************************************
// mq_task_set - Initializes a task for use
//**************************************************************

int mq_task_set(mq_task_t *task, mq_context_t *ctx, mq_msg_t *msg, op_generic_t *gop,  void *arg, int dt)
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
// mq_task_new - Creates and initializes a task for use
//**************************************************************

mq_task_t *mq_task_new(mq_context_t *ctx, mq_msg_t *msg, op_generic_t *gop, void *arg, int dt)
{
    mq_task_t *task;

    type_malloc_clear(task, mq_task_t, 1);

    mq_task_set(task, ctx, msg, gop, arg, dt);

    return(task);
}

//*************************************************************
// new_thread_pool_op - Allocates space for a new op
//*************************************************************

op_generic_t *new_mq_op(mq_context_t *ctx, mq_msg_t *msg, op_status_t (*fn_response)(void *arg, int id), void *arg, void (*my_arg_free)(void *arg), int dt)
{
    mq_task_t *task;

    task = mq_task_new(ctx, msg, NULL, arg, dt);
    task->gop = new_thread_pool_op(ctx->tp, "mq", fn_response, task, mq_arg_free, 1);
    task->my_arg_free = my_arg_free;
    return(task->gop);
}


//**************************************************************
// mqt_exec - Routine to process exec/trackexec commands
//**************************************************************

void *mqt_exec(apr_thread_t *th, void *arg)
{
    mq_task_t *task = (mq_task_t *)arg;
    mq_portal_t *p = (mq_portal_t *)task->arg;
    mq_frame_t *f;
    char b64[1024];
    void *key;
    int n;

    mq_msg_first(task->msg);    //** Empty frame
    mq_msg_next(task->msg);     //** Version
    mq_msg_next(task->msg);     //** MQ command
    f = mq_msg_next(task->msg);     //** Skip the ID
    mq_get_frame(f, &key, &n);
//log_printf(1, "execing sid=%s  EXEC SUBMIT now=" TT "\n", mq_id2str(key, n, b64, sizeof(b64)), apr_time_sec(apr_time_now()));  flush_log();
    log_printf(1, "execing sid=%s\n", mq_id2str(key, n, b64, sizeof(b64)));
    f = mq_msg_next(task->msg); //** and get the user command
    mq_get_frame(f, &key, &n);

//** Lookup and see if the envelope command is supported.
    mq_command_exec(p->command_table, task, key, n);

    mq_task_destroy(task);

    return(NULL);
}

//**************************************************************
// mqt_success - Routine for successful send of a message
//**************************************************************

void *mqtp_success(apr_thread_t *th, void *arg)
{
    mq_task_t *task = (mq_task_t *)arg;

    gop_mark_completed(task->gop, op_success_status);

    return(NULL);
}

//**************************************************************
// mqt_fail - Routine for failing a task
//**************************************************************

void *mqtp_failure(apr_thread_t *th, void *arg)
{
    mq_task_t *task = (mq_task_t *)arg;

    gop_mark_completed(task->gop, op_failure_status);

    return(NULL);
}

//**************************************************************
//  mq_task_complete - Marks a task as complete and destroys it
//**************************************************************

void mq_task_complete(mq_conn_t *c, mq_task_t *task, int status)
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

void mqc_response(mq_conn_t *c, mq_msg_t *msg, int do_exec)
{
    mq_frame_t *f;
    int size;
    char *id;
    mq_task_monitor_t *tn;
    char b64[1024];

    log_printf(5, "start\n");
    flush_log();

    f = mq_msg_next(msg);  //** This should be the task ID
    mq_get_frame(f, (void **)&id, &size);
    log_printf(5, "id_size=%d\n", size);

//** Find the task
    tn = apr_hash_get(c->waiting, id, size);
    if (tn == NULL) {  //** Nothing matches so drop it
        log_printf(1, "ERROR: No matching ID! sid=%s\n", mq_id2str(id, size, b64, sizeof(b64)));
        flush_log();
        mq_msg_destroy(msg);
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
        flush_log();
        tn->task->response = msg;
        thread_pool_direct(c->pc->tp, thread_pool_exec_fn, tn->task->gop);
    }

//** Free the tracking number container
    free(tn);

    log_printf(5, "end\n");
    flush_log();
}

//**************************************************************
// mq_apply_return_address_msg - Converts the raw return address
//  to a "Sender" address o nteh message
//  NOTE: The raw address should have the empty frame!
//        if dup_frames == 0 then raw_address frames are consumed!
//**************************************************************

void mq_apply_return_address_msg(mq_msg_t *msg, mq_msg_t *raw_address, int dup_frames)
{
    mq_frame_t *f;

    f = mq_msg_first(raw_address);
    if (dup_frames == 0) f = mq_msg_pop(raw_address);
    while (f != NULL) {
        if (dup_frames == 1) {
            mq_msg_push_frame(msg, mq_frame_dup(f));
        } else {
            mq_msg_push_frame(msg, f);
        }

        f = (dup_frames == 0) ? mq_msg_pop(raw_address) : mq_msg_next(raw_address);
    }

    return;
}

//**************************************************************
// mq_trackaddress_msg - Forms a track address response
//   This takes the raw address frames from the original email
//   and flips or duplicates them based dup_frames
//   ****NOTE:  The address should start with the EMPTY frame****
//        if dup_frames == 0 then raw_address frames are consumed!
//**************************************************************

mq_msg_t *mq_trackaddress_msg(char *host, mq_msg_t *raw_address, mq_frame_t *fid, int dup_frames)
{
    mq_msg_t *track_response;
    mq_frame_t *f;

    track_response = mq_msg_new();
    mq_msg_append_mem(track_response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(track_response, MQF_TRACKADDRESS_KEY, MQF_TRACKADDRESS_SIZE, MQF_MSG_KEEP_DATA);

    if (dup_frames == 1) {
        mq_msg_append_frame(track_response, mq_frame_dup(fid));
    } else {
        mq_msg_append_frame(track_response, fid);
    }

//** Add the address. We skip frame 0 (empty) and frame 1 (sender -- he knows who he is)
    mq_msg_first(raw_address);
    mq_msg_next(raw_address);
    while ((f = mq_msg_next(raw_address)) != NULL) {
        mq_msg_append_frame(track_response, mq_frame_dup(f));  //** Always dup frames
    }

//** Need to add ourselves and the empty frame to the tracking address
    mq_msg_append_mem(track_response, host, strlen(host), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(track_response, NULL, 0, MQF_MSG_KEEP_DATA);

//** Lastly add the return addres.  We always dup the frames here cause they are used
//** in the address already if not duped.
    mq_apply_return_address_msg(track_response, raw_address, dup_frames);

    return(track_response);
}

//**************************************************************
// mqc_trackaddress - Processes a track address command
//**************************************************************

void mqc_trackaddress(mq_conn_t *c, mq_msg_t *msg)
{
    mq_frame_t *f;
    int size, n;
    char *id, *address;
    mq_task_monitor_t *tn;
    mq_heartbeat_entry_t *hb;

    f = mq_msg_next(msg);  //** This should be the task ID
    mq_get_frame(f, (void **)&id, &size);

//** Find the task
    tn = apr_hash_get(c->waiting, id, size);
    log_printf(5, "trackaddress status tn=%p id_size=%d\n", tn, size);
    void *data;
    int i;
    for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
        mq_get_frame(f, &data, &n);
        log_printf(5, "fsize[%d]=%d\n", i, n);
    }

    if (tn != NULL) {
        log_printf(5, "tn->tracking=%p\n", tn->tracking);
        if (tn->tracking != NULL) goto cleanup;  //** Duplicate so drop and ignore

//** Form the address key but first strip off the gunk we don't care about to determine the size
        f = mq_msg_first(msg);
        mq_frame_destroy(mq_msg_pluck(msg, 0)); // empty
        mq_frame_destroy(mq_msg_pluck(msg, 0));  // version
        mq_frame_destroy(mq_msg_pluck(msg, 0));  // TRACKADDRESS command
        mq_frame_destroy(mq_msg_pluck(msg, 0));  // id
//QWERTY     mq_frame_destroy(mq_msg_pluck(msg, 0));  // <empty>

//** What's left is the address until an empty frame
        size = mq_msg_total_size(msg);
        log_printf(5, " msg_total_size=%d frames=%d\n", size, stack_size(msg));
        type_malloc_clear(address, char, size+1);
        n = 0;
        for (f=mq_msg_first(msg); f != NULL; f=mq_msg_next(msg)) {
            mq_get_frame(f, (void **)&id, &size);
            log_printf(5, "ta element=%d\n", size);
            memcpy(&(address[n]), id, size);
            n = n + size;
            if (size == 0) break;
        }
        address[n] = 0;
        log_printf(5, "full address=%s\n", address);

//** Remove anything else
        f = mq_msg_next(msg);
        while (f != NULL) {
            f = mq_msg_pluck(msg, 0);
            mq_frame_destroy(f);
            f = mq_msg_current(msg);
        }

//** Make sure its not already stored
        hb = apr_hash_get(c->heartbeat_dest, address, n);
        if (hb == NULL) {  //** Make the new entry
            type_malloc_clear(hb, mq_heartbeat_entry_t, 1);
            hb->key = address;
            hb->key_size = n;
            hb->lut_id = atomic_global_counter();

            log_printf(5, "trackaddress hb_lut=" LU "\n", hb->lut_id);
//** Form the heartbeat msg
//** Right now we just have the address which should have an empty last frame
            mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
            mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
            mq_msg_append_mem(msg, &(hb->lut_id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
            mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

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
    if (msg != NULL) mq_msg_destroy(msg);
}

//***************************************************************************
// mqc_ping - Processes a ping request
//***************************************************************************

int mqc_ping(mq_conn_t *c, mq_msg_t *msg)
{
    mq_msg_t *pong;
    mq_frame_t *f, *pid;
    int err;

//void *data;
//for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
//  mq_get_frame(f, &data, &err);
//  log_printf(5, "fsize[%d]=%d\n", i, err);
//}

//** Peel off the top frames and just leave the return address
    f = mq_msg_first(msg);
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //blank
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //version
    mq_frame_destroy(mq_msg_pluck(msg,0));  //command

    pid = mq_msg_pluck(msg, 0);  //Ping ID

    pong = mq_msg_new();

//** Push the address in reverse order (including the empty frame)
    while ((f = mq_msg_pop(msg)) != NULL) {
        mq_msg_push_frame(pong, f);
    }

    mq_msg_destroy(msg);
//** Now add the command
    mq_msg_append_mem(pong, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(pong, MQF_PONG_KEY, MQF_PONG_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_frame(pong, pid);
    mq_msg_append_mem(pong, NULL, 0, MQF_MSG_KEEP_DATA);

    c->stats.incoming[MQS_PONG_INDEX]++;

    err = mq_send(c->sock, pong, MQ_DONTWAIT);

    mq_msg_destroy(pong);

    return(err);
}




//**************************************************************
// mqc_pong - Processed a pong command
//**************************************************************

void mqc_pong(mq_conn_t *c, mq_msg_t *msg)
{
    mq_frame_t *f;
    int size;
    mq_heartbeat_entry_t *entry;
    void *ptr;

    f = mq_msg_next(msg);  //** This should be the ID which is actually the entry
    mq_get_frame(f, &ptr, &size);

//** Validate the entry
    entry = apr_hash_get(c->heartbeat_lut, ptr, sizeof(uint64_t));
    if (entry != NULL) {
        entry->last_check = apr_time_now();
    }

    log_printf(5, "pong entry=%p ptr=%p\n", entry, ptr);
//log_printf(5, "pong entry->key=%.10s\n", entry->key);
//** Clean up
    mq_msg_destroy(msg);
}


//**************************************************************
// mqc_heartbeat_cleanup - Cleans up all the heartbeat and pending
//     tasks on a close.
//**************************************************************

int mqc_heartbeat_cleanup(mq_conn_t *c)
{
    char *key;
    apr_ssize_t klen;
    apr_hash_index_t *hi, *hit;
    mq_heartbeat_entry_t *entry;
    mq_task_monitor_t *tn;

//** Clean out the heartbeat info
//** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
    for (hi = apr_hash_first(NULL, c->heartbeat_dest); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&entry);

        apr_hash_set(c->heartbeat_dest, key, klen, NULL);
        apr_hash_set(c->heartbeat_lut, &(entry->lut_id), sizeof(uint64_t), NULL);
        free(entry->key);
        mq_msg_destroy(entry->address);
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
        flush_log();
        log_printf(1, "Failed task tn->task=%p tn->task->gop=%p\n", tn->task, tn->task->gop);
        flush_log();
        assert(tn->task);
        assert(tn->task->gop);
        thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

//** Free the container. The mq_task_t is handled by the response
        free(tn);
    }

    return(1);
}

//**************************************************************
// mqc_heartbeat_dec - Decrement the hb structure which may result
//    in it's removal.
//**************************************************************

void mqc_heartbeat_dec(mq_conn_t *c, mq_heartbeat_entry_t *hb)
{
    hb->count--;

    if (hb->count <= 0) {  //** Last ref so remove it
        apr_hash_set(c->heartbeat_dest, hb->key, hb->key_size, NULL);
        apr_hash_set(c->heartbeat_lut, &(hb->lut_id), sizeof(uint64_t), NULL);
        free(hb->key);
        mq_msg_destroy(hb->address);
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

int mqc_heartbeat(mq_conn_t *c, int npoll)
{
    char *key;
    apr_ssize_t klen;
    apr_hash_index_t *hi, *hit;
    mq_heartbeat_entry_t *entry;
    mq_task_monitor_t *tn;
    apr_time_t dt, dt_fail, dt_check;
    apr_time_t now;
    int n, pending_count, conn_dead, do_conn_hb;
    char b64[1024];

    double dts;
    apr_time_t start = apr_time_now();
    log_printf(6, "START host=%s\n", c->mq_uuid);
    flush_log();
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
            log_printf(8, "hb->key=%s FAIL dt=%d\n", entry->key, klen);
            log_printf(6, "before waiting size=%d\n", apr_hash_count(c->waiting));
//** NOTE: using internal non-threadsafe iterator.  Should be ok in this case
            for (hit = apr_hash_first(NULL, c->waiting); hit != NULL; hit = apr_hash_next(hit)) {
                apr_hash_this(hit, (const void **)&key, &klen, (void **)&tn);
                if (tn->tracking == entry) {
//** Clear it out
                    apr_hash_set(c->waiting, key, klen, NULL);

//** Submit the fail task
                    log_printf(6, "Failed task uuid=%s sid=%s\n", c->mq_uuid, mq_id2str(key, klen, b64, sizeof(b64)));
                    flush_log();
                    log_printf(6, "Failed task tn->task=%p tn->task->gop=%p\n", tn->task, tn->task->gop);
                    flush_log();
                    assert(tn->task);
                    assert(tn->task->gop);
                    thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

//** Free the container. The mq_task_t is handled by the response
                    free(tn);
                }
            }

            log_printf(6, "after waiting size=%d\n", apr_hash_count(c->waiting));

//** Remove the entry and clean up
            apr_hash_set(c->heartbeat_dest, entry->key, entry->key_size, NULL);
            apr_hash_set(c->heartbeat_lut, &(entry->lut_id), sizeof(uint64_t), NULL);
            free(entry->key);
            mq_msg_destroy(entry->address);
            free(entry);
        } else if (dt > dt_check) {  //** Send a heartbeat check
            klen = apr_time_sec(dt);
            log_printf(10, "hb->key=%s CHECK dt=%d\n", entry->key, klen);
            if ((npoll == 1) && (entry == c->hb_conn)) {
                do_conn_hb = 1;
                goto next;  //** Skip local hb if finished
            }
            c->stats.outgoing[MQS_HEARTBEAT_INDEX]++;
            c->stats.outgoing[MQS_PING_INDEX]++;

            mq_send(c->sock, entry->address, 0);
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
            log_printf(6, "Failed task uuid=%s hash_count=%u sid=%s\n", c->mq_uuid, apr_hash_count(c->waiting), mq_id2str(key, klen, b64, sizeof(b64)));
            flush_log();
            log_printf(6, "Failed task tn->task=%p tn->task->gop=%p gid=%d\n", tn->task, tn->task->gop, gop_id(tn->task->gop));
            flush_log();
            assert(tn->task);
            assert(tn->task->gop);
            thread_pool_direct(c->pc->tp, mqtp_failure, tn->task);

//** Free the container. The mq_task_t is handled by the response
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

            mq_send(c->sock, c->hb_conn->address, 0);
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

int mqc_process_incoming(mq_conn_t *c, int *nproc)
{
    int n, count;
    mq_msg_t *msg;
    mq_frame_t *f;
    mq_task_t *task;
    char *data;
    int size;

    log_printf(5, "processing incoming start\n");
//** Process all that are on the wire
    msg = mq_msg_new();
    n = 0;
    count = 0;
    while ((n = mq_recv(c->sock, msg, MQ_DONTWAIT)) == 0) {
        count++;
        log_printf(5, "Got a message count=%d\n", count);
//** verify we have an empty frame
        f = mq_msg_first(msg);
        mq_get_frame(f, (void **)&data, &size);
        if (size != 0) {
            log_printf(0, "ERROR: Missing empty frame!\n");
//mq_msg_destroy(msg);
            task = mq_task_new(c->pc->mqc, msg, NULL, c->pc, -1);
            mqt_exec(NULL, task);
            goto skip;
        }

//log_printf(5, "111111111111111111111\n"); flush_log();
//** and the correct version
        f = mq_msg_next(msg);
        mq_get_frame(f, (void **)&data, &size);
        if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
            log_printf(0, "ERROR: Invalid version!\n");
            mq_msg_destroy(msg);
            goto skip;
        }

//log_printf(5, "222222222222222222\n"); flush_log();

//** This is the command frame
        f = mq_msg_next(msg);
        mq_get_frame(f, (void **)&data, &size);
        if (mq_data_compare(MQF_PING_KEY, MQF_PING_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_PING_KEY\n");
            flush_log();
            c->stats.incoming[MQS_PING_INDEX]++;
            mqc_ping(c, msg);
        } else if (mq_data_compare(MQF_PONG_KEY, MQF_PONG_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_PONG_KEY\n");
            flush_log();
            c->stats.incoming[MQS_PONG_INDEX]++;
            mqc_pong(c, msg);
        } else if (mq_data_compare(MQF_TRACKADDRESS_KEY, MQF_TRACKADDRESS_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_TRACKADDRESS_KEY\n");
            flush_log();
            c->stats.incoming[MQS_TRACKADDRESS_INDEX]++;
            mqc_trackaddress(c, msg);
        } else if (mq_data_compare(MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, data, size) == 0) {
            log_printf(15, "Processing MQF_RESPONSE_KEY\n");
            flush_log();
            c->stats.incoming[MQS_RESPONSE_INDEX]++;
            mqc_response(c, msg, 1);
        } else if ((mq_data_compare(MQF_EXEC_KEY, MQF_EXEC_SIZE, data, size) == 0) ||
                   (mq_data_compare(MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, data, size) == 0)) {

            if (mq_data_compare(MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, data, size) == 0) {
                log_printf(15, "Processing MQF_TRACKEXEC_KEY\n");
                flush_log();
                c->stats.incoming[MQS_TRACKEXEC_INDEX]++;
            } else {
                log_printf(15, "Processing MQF_EXEC_KEY\n");
                flush_log();
                c->stats.incoming[MQS_EXEC_INDEX]++;

            }

//** It's up to the task to send any tracking information back.
            log_printf(5, "Submiting task for execution\n");
            task = mq_task_new(c->pc->mqc, msg, NULL, c->pc, -1);
            thread_pool_direct(c->pc->tp, mqt_exec, task);
        } else {   //** Unknwon command so drop it
            log_printf(5, "ERROR: Unknown command.  Dropping\n");
            c->stats.incoming[MQS_UNKNOWN_INDEX]++;
            mq_msg_destroy(msg);
            goto skip;
        }
skip:
        msg = mq_msg_new(); //**  The old one is destroyed after it's consumed
        if (count > 10) break;  //** Kick out for other processing
    }

    mq_msg_destroy(msg);  //** Clean up

    *nproc += count;  //** Inc processed commands
    log_printf(5, "processing incoming end n=%d\n", n);
    flush_log();

    return(0);
}

//**************************************************************
// mqc_process_task - Sends the new task
//   npoll -- When processing the task if c->pc->n_close > 0
//   then no tasks is processed but instead n_close is decremented
//   and npoll set to 1 to stop monitoring the incoming task port
//**************************************************************

int mqc_process_task(mq_conn_t *c, int *npoll, int *nproc)
{
    mq_task_t *task;
    mq_frame_t *f;
    mq_task_monitor_t *tn;
    char b64[1024];
    char *data, v;
    int i, size, tracking;

//** Read an event
    i = mq_pipe_read(c->pc->efd[0], &v);

//** Get the new task or start a wind down if requested
    apr_thread_mutex_lock(c->pc->lock);
    if (c->pc->n_close > 0) { //** Wind down request
        c->pc->n_close--;
        *npoll = 1;
    } else {  //** Got a new task
        task = pop(c->pc->tasks);
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
    f = mq_msg_first(task->msg);
    mq_get_frame(f, (void **)&data, &size);
    log_printf(10, "address length = %d\n", size);
    while ((f != NULL) && (size != 0)) {
        f = mq_msg_next(task->msg);
        mq_get_frame(f, (void **)&data, &size);
        log_printf(10, "length = %d\n", size);
    }
    if (f == NULL) { //** Bad command
        log_printf(0, "Invalid command!\n");
        return(1);
    }

//** Verify the version
    f = mq_msg_next(task->msg);
    mq_get_frame(f, (void **)&data, &size);
    if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {  //** Bad version number
        log_printf(0, "Invalid version!\n");
        log_printf(0, "length = %d\n", size);
        return(1);
    }

    log_printf(10, "MQF_VERSION_KEY found\n");
    log_printf(5, "task pass_through = %d\n", task->pass_through);
//** This is the command
    f = mq_msg_next(task->msg);
    mq_get_frame(f, (void **)&data, &size);
    tracking = 0;
    if ( (mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0) && (task->pass_through == 0) ) { //** We track it - But only if it is not a pass-through task
//** Get the ID here.  The send will munge my frame position
        f = mq_msg_next(task->msg);
        mq_get_frame(f, (void **)&data, &size);
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
        log_printf(10, "Unknown key found! key = %d\n", data);
    }

//** Send it on
    i = mq_send(c->sock, task->msg, 0);
    if (i == -1) {
        log_printf(0, "Error sending msg! errno=%d\n", errno);
        mq_task_complete(c, task, OP_STATE_FAILURE);
        return(1);
    }

    if (tracking == 0) {     //** Exec the callback if not tracked
        mq_task_complete(c, task, OP_STATE_SUCCESS);
    } else {                 //** Track the task
        log_printf(1, "TRACKING id_size=%d sid=%s\n", size, mq_id2str(data, size, b64, sizeof(b64)));
        if (task->gop != NULL) log_printf(1, "TRACKING gid=%d\n", gop_id(task->gop));
//** Insert it in the monitoring table
        type_malloc_clear(tn, mq_task_monitor_t, 1);
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

int mq_conn_make(mq_conn_t *c)
{
    mq_pollitem_t pfd;
    int err, n, frame;
    mq_msg_t *msg;
    mq_frame_t *f;
    char *data;
    apr_time_t start, dt;
    mq_heartbeat_entry_t *hb;

    log_printf(5, "START host=%s\n", c->pc->host);

//** Determing the type of socket to make based on
//** the mq_conn_t* passed in
//** Old version:
//** c->sock = mq_socket_new(c->pc->ctx, MQ_TRACE_ROUTER);
//** Hardcoded MQ_TRACE_ROUTER socket type
    c->sock = mq_socket_new(c->pc->ctx, c->pc->socket_type);
    log_printf(0, "host = %s, connect_mode = %d\n", c->pc->host, c->pc->connect_mode);
    if (c->pc->connect_mode == MQ_CMODE_CLIENT) {
        err = mq_connect(c->sock, c->pc->host);
    } else {
        err = mq_bind(c->sock, c->pc->host);
    }

    c->mq_uuid = zsocket_identity(c->sock->arg);  //** Kludge

    if (err != 0) return(1);
    if (c->pc->connect_mode == MQ_CMODE_SERVER) return(0);  //** Nothing else to do

    err = 1; //** Defaults to failure
    dt = 0;
    frame = -1;

//** Form the ping message and make the base hearbeat message
    type_malloc_clear(hb, mq_heartbeat_entry_t, 1);
    hb->key = strdup(c->pc->host);
    hb->key_size = strlen(c->pc->host);
    hb->lut_id = atomic_global_counter();
    hb->count = 1;

//** This is the ping message
    msg = mq_msg_new();
    mq_msg_append_mem(msg, c->pc->host, strlen(c->pc->host), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, &(hb->lut_id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    hb->address = msg;
    c->hb_conn = hb;

    msg = mq_msg_new();  //** This is for the pong response

//** Finish creating the structure
    apr_hash_set(c->heartbeat_dest, hb->key, hb->key_size, hb);
    apr_hash_set(c->heartbeat_lut, &(hb->lut_id), sizeof(uint64_t), hb);
    hb->last_check = apr_time_now();

//** Send it
    pfd.socket = mq_poll_handle(c->sock);
    pfd.events = MQ_POLLOUT;

    start = apr_time_now();
    c->stats.outgoing[MQS_PING_INDEX]++;
    c->stats.outgoing[MQS_HEARTBEAT_INDEX]++;
    while (mq_send(c->sock, hb->address, MQ_DONTWAIT) != 0) {
        dt = apr_time_now() - start;
        dt = apr_time_sec(dt);

        if (dt > 5) {
            log_printf(0, "ERROR: Failed sending task to host=%s\n", c->pc->host);
            goto fail;
        }

        mq_poll(&pfd, 1, 1000);
    }

//** Wait for a connection
    pfd.socket = mq_poll_handle(c->sock);
    pfd.events = MQ_POLLIN;

    start = apr_time_now();
    dt = 0;
    frame = -1;
    while (dt < 10) {
        mq_poll(&pfd, 1, 1000);
        if (mq_recv(c->sock, msg, MQ_DONTWAIT) == 0) {
            f = mq_msg_first(msg);
            frame = 1;
            mq_get_frame(f, (void **)&data, &n);
            if (n != 0) goto fail;

            f = mq_msg_next(msg);
            frame = 1;
            mq_get_frame(f, (void **)&data, &n);
            if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) goto fail;

            f = mq_msg_next(msg);
            frame = 2;
            mq_get_frame(f, (void **)&data, &n);
            if (mq_data_compare(data, n, MQF_PONG_KEY, MQF_PONG_SIZE) != 0) goto fail;

            f = mq_msg_next(msg);
            frame = 3;
            mq_get_frame(f, (void **)&data, &n);
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
    log_printf(5, "END status=%d dt=%d frame=%d\n", err, dt, frame);
    mq_msg_destroy(msg);
    return(err);
}

//**************************************************************
// mq_conn_thread - Connection thread
//**************************************************************

void *mq_conn_thread(apr_thread_t *th, void *data)
{
    mq_conn_t *c = (mq_conn_t *)data;
    int k, npoll, err, finished, nprocessed, nproc, nincoming, slow_exit, oops;
    long int heartbeat_ms;
    int64_t total_proc, total_incoming;
    mq_pollitem_t pfd[3];
    apr_time_t next_hb_check, last_check;
    double proc_rate, dt;
    char v;

//log_printf(2, "START: uuid=%s heartbeat_dt=%d\n", c->mq_uuid, c->pc->heartbeat_dt);
    log_printf(2, "START: host=%s heartbeat_dt=%d\n", c->pc->host, c->pc->heartbeat_dt);
//** Try and make the connection
//** Right now the portal is locked so this routine can assume that.
    oops = err = mq_conn_make(c);
    log_printf(2, "START(2): uuid=%s oops=%d\n", c->mq_uuid, oops);


//** Notify the parent about the connections status via c->cefd
//** It is then safe to manipulate c->pc->lock
    v = (err == 0) ? 1 : 2;  //** Make 1 success and 2 failure

    log_printf(5, "after conn_make err=%d\n", err);

    write(c->cefd[1], &v, 1);

    total_proc = total_incoming = 0;
    slow_exit = 0;
    nprocessed = 0;

    if (err != 0) goto cleanup;  //** if no connection shutdown

//**Make the poll structure
    memset(pfd, 0, sizeof(pfd));
    mq_pipe_poll_store(&(pfd[PI_EFD]), c->pc->efd[0], MQ_POLLIN);
    pfd[PI_CONN].socket = mq_poll_handle(c->sock);
    pfd[PI_CONN].events = MQ_POLLIN;

//** Main processing loop
    finished = 0;
    heartbeat_ms = c->pc->heartbeat_dt * 1000;
    npoll = 2;
    next_hb_check = apr_time_now() + apr_time_from_sec(1);
    last_check = apr_time_now();

    do {
        k = mq_poll(pfd, npoll, heartbeat_ms);
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
            flush_log();
            goto cleanup;
        }

        if ((apr_time_now() > next_hb_check) || (npoll == 1)) {
            finished += mqc_heartbeat(c, npoll);
            log_printf(5, "after heartbeat finished=%d\n", finished);

            log_printf(5, "hb_old=" LU "\n", next_hb_check);
            next_hb_check = apr_time_now() + apr_time_from_sec(1);
            log_printf(5, "hb_new=" LU "\n", next_hb_check);

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
    mq_stats_print(2, c->mq_uuid, &(c->stats));
    log_printf(2, "END: uuid=%s total_incoming=" I64T " total_processed=" I64T " oops=%d\n", c->mq_uuid, total_incoming, total_proc, oops);
    flush_log();

    mq_conn_teardown(c);

    //** Update the conn_count, stats and place mysealf on the reaper stack
    apr_thread_mutex_lock(c->pc->lock);
    mq_stats_add(&(c->pc->stats), &(c->stats));
//** We only update the connection counts if we actually made a connection.  The original thread that created us was already notified
//** if we made a valid connection and it increments the connection counts.
    if (oops == 0) {
        if (slow_exit == 0) c->pc->active_conn--;
        c->pc->total_conn--;
    }
    if (c->pc->total_conn == 0) apr_thread_cond_signal(c->pc->cond);
    push(c->pc->closed_conn, c);
    apr_thread_mutex_unlock(c->pc->lock);

    log_printf(2, "END: final\n");
    flush_log();


    return(NULL);
}


//**************************************************************
// mq_conn_create_actual - This routine does the actual connection creation
//      and optionally waits for the connection to complete if dowait=1.
//
//   NOTE:  Assumes p->lock is set on entry.
//**************************************************************

int mq_conn_create_actual(mq_portal_t *p, int dowait)
{
    mq_conn_t *c;
    int err;
    char v;
    
    type_malloc_clear(c, mq_conn_t, 1);
    
    c->pc = p;
    int result;
    result = apr_pool_create(&(c->mpool), NULL);
    assert(result == APR_SUCCESS); }
    result = (c->waiting = apr_hash_make(c->mpool));
    assert(result != NULL);
    result = (c->heartbeat_dest = apr_hash_make(c->mpool));
    assert(result != NULL);
    result = (c->heartbeat_lut = apr_hash_make(c->mpool));
    assert(result != NULL);
    
    //** This is just used in the initial handshake
    result = pipe(c->cefd);
    assert(result == 0);
    
    //** Spawn the thread
    //** USe the parent mpool so I can do the teardown
    thread_create_assert(&(c->thread), NULL, mq_conn_thread, (void *)c, p->mpool);
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

int mq_conn_create(mq_portal_t *p, int dowait)
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
// mq_conn_teardown - Tearsdown the MQ connection structures
//    Does not destroy the mq_conn_t structure itself.  This is
//    handled when the connection is reaped.
//**************************************************************

void mq_conn_teardown(mq_conn_t *c)
{
    mqc_heartbeat_cleanup(c);

    apr_hash_clear(c->waiting);
    apr_hash_clear(c->heartbeat_dest);
    apr_hash_clear(c->heartbeat_lut);
    apr_pool_destroy(c->mpool);
    if (c->cefd[0] != -1) {
        close(c->cefd[0]), close(c->cefd[0]);
    }
    if (c->sock != NULL) mq_socket_destroy(c->pc->ctx, c->sock);
    if (c->mq_uuid != NULL) free(c->mq_uuid);
}

//**************************************************************
// _mq_reap_closed - Reaps closed connections.
//    NOTE:  Assumes the portal is already locked!
//**************************************************************

void _mq_reap_closed(mq_portal_t *p)
{
    mq_conn_t *c;
    apr_status_t dummy;

    while ((c = pop(p->closed_conn)) != NULL) {
        apr_thread_join(&dummy, c->thread);
        free(c);
    }
}

//**************************************************************
// mq_portal_destroy - Destroys the MQ portal
//**************************************************************

void mq_portal_destroy(mq_portal_t *p)
{
    int i, n;
    char c;

//** Tell how many connections to close
    apr_thread_mutex_lock(p->lock);
    log_printf(2, "host=%s active_conn=%d total_conn=%d\n", p->host, p->active_conn, p->total_conn);
    flush_log();
    p->n_close = p->active_conn;
    n = p->n_close;
    apr_thread_mutex_unlock(p->lock);

//** Signal them
    c = 1;
    for (i=0; i<n; i++) mq_pipe_write(p->efd[1], &c);

    //** Wait for them all to complete
    apr_thread_mutex_lock(p->lock);
    while (p->total_conn > 0) {
        apr_thread_cond_wait(p->cond, p->lock);
    }
    apr_thread_mutex_unlock(p->lock);

    log_printf(2, "host=%s closed_size=%d total_conn=%d\n", p->host, stack_size(p->closed_conn), p->total_conn);
    flush_log();

    //** Clean up 
    //** Don;t have to worry about locking cause no one else exists


    _mq_reap_closed(p); 
    //** Destroy the command table
    mq_command_table_destroy(p->command_table);

    //** Update the stats
    apr_thread_mutex_lock(p->mqc->lock);
    mq_stats_add(&(p->mqc->stats), &(p->stats));
    apr_thread_mutex_unlock(p->mqc->lock);

    mq_stats_print(2, p->host, &(p->stats));

    apr_thread_mutex_destroy(p->lock);
    apr_thread_cond_destroy(p->cond);
    apr_pool_destroy(p->mpool);

    mq_pipe_destroy(p->ctx, p->efd);
    if (p->ctx != NULL) mq_socket_context_destroy(p->ctx);

    free_stack(p->closed_conn, 0);
    free_stack(p->tasks, 0);

    free(p->host);
    free(p);
}

//**************************************************************
// mq_portal_lookup - Looks up a portal context
//**************************************************************

mq_portal_t *mq_portal_lookup(mq_context_t *mqc, char *hostname, int connect_mode)
{
    apr_hash_t *ptable;
    mq_portal_t *p;

    apr_thread_mutex_lock(mqc->lock);
    ptable = (connect_mode == MQ_CMODE_CLIENT) ? mqc->client_portals : mqc->server_portals;
    p = (mq_portal_t *)(apr_hash_get(ptable, hostname, APR_HASH_KEY_STRING));
    apr_thread_mutex_unlock(mqc->lock);

    return(p);
}

//**************************************************************
// mq_portal_command_table - Retrieves the portal command table
//**************************************************************

mq_command_table_t *mq_portal_command_table(mq_portal_t *portal)
{
    return(portal->command_table);
}

//**************************************************************
// mq_portal_remove - Removes a server portal in the context
//**************************************************************

void mq_portal_remove(mq_context_t *mqc, mq_portal_t *p)
{
    apr_thread_mutex_lock(mqc->lock);
    apr_hash_set(mqc->server_portals, p->host, APR_HASH_KEY_STRING, NULL);
    apr_thread_mutex_unlock(mqc->lock);
}

//**************************************************************
// mq_portal_install - Installs a server portal into the context
//**************************************************************

int mq_portal_install(mq_context_t *mqc, mq_portal_t *p)
{

    mq_portal_t *p2;
    int err;
    apr_hash_t *ptable;

    err = 0;

    apr_thread_mutex_lock(mqc->lock);
    ptable = (p->connect_mode == MQ_CMODE_CLIENT) ? mqc->client_portals : mqc->server_portals;
    p2 = (mq_portal_t *)(apr_hash_get(ptable, p->host, APR_HASH_KEY_STRING));
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
// mq_portal_create - Creates a new MQ portal
//**************************************************************

mq_portal_t *mq_portal_create(mq_context_t *mqc, char *host, int connect_mode)
{
    mq_portal_t *p;

    log_printf(15, "New portal host=%s\n", host);

    type_malloc_clear(p, mq_portal_t, 1);

    p->mqc = mqc;
    p->host = strdup(host);
    p->command_table = mq_command_table_new(NULL, NULL);

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

    p->ctx = mq_socket_context_new();

    apr_pool_create(&(p->mpool), NULL);
    apr_thread_mutex_create(&(p->lock), APR_THREAD_MUTEX_DEFAULT, p->mpool);
    apr_thread_cond_create(&(p->cond), p->mpool);

    mq_pipe_create(p->ctx, p->efd);

    p->tasks = new_stack();
    p->closed_conn = new_stack();

    return(p);
}

//**************************************************************
// mq_destroy_context - Destroys the MQ context
//**************************************************************

void mq_destroy_context(mq_context_t *mqc)
{
    apr_hash_index_t *hi;
    mq_portal_t *p;
    void *val;

    log_printf(5, "Shutting down client_portals\n");
    flush_log();
    for (hi=apr_hash_first(mqc->mpool, mqc->client_portals); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        p = (mq_portal_t *)val;
        apr_hash_set(mqc->client_portals, p->host, APR_HASH_KEY_STRING, NULL);
        log_printf(5, "destroying p->host=%s\n", p->host);
        flush_log();
        mq_portal_destroy(p);
    }
    log_printf(5, "Shutting down server_portals\n");
    flush_log();
    for (hi=apr_hash_first(mqc->mpool, mqc->server_portals); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        p = (mq_portal_t *)val;
        apr_hash_set(mqc->server_portals, p->host, APR_HASH_KEY_STRING, NULL);
        log_printf(5, "destroying p->host=%s\n", p->host);
        flush_log();
        mq_portal_destroy(p);
    }
    log_printf(5, "Completed portal shutdown\n");
    flush_log();
    //sleep(1);
    //log_printf(5, "AFTER SLEEP\n"); flush_log();

    mq_stats_print(2, "Portal total", &(mqc->stats));

    apr_hash_clear(mqc->client_portals);

    thread_pool_destroy_context(mqc->tp);

    apr_thread_mutex_destroy(mqc->lock);
    apr_pool_destroy(mqc->mpool);

    free(mqc);

    //sleep(1);
    log_printf(5, "AFTER SLEEP2\n");
    flush_log();
}

//**************************************************************
// _mq_submit - GOP submit routine for MQ objects
//**************************************************************

void _mq_submit_op(void *arg, op_generic_t *gop)
{
    thread_pool_op_t *op = gop_get_tp(gop);
    mq_task_t *task = (mq_task_t *)op->arg;

    log_printf(15, "gid=%d\n", gop_id(gop));

    mq_task_send(task->ctx, task);
}

//**************************************************************
//  mq_create_context - Creates a new MQ pool
//**************************************************************

mq_context_t *mq_create_context(inip_file_t *ifd, char *section)
{
    mq_context_t *mqc;

    type_malloc_clear(mqc, mq_context_t, 1);

    mqc->min_conn = inip_get_integer(ifd, section, "min_conn", 1);
    mqc->max_conn = inip_get_integer(ifd, section, "max_conn", 3);
    mqc->min_threads = inip_get_integer(ifd, section, "min_threads", 2);
    mqc->max_threads = inip_get_integer(ifd, section, "max_threads", 4);
    mqc->backlog_trigger = inip_get_integer(ifd, section, "backlog_trigger", 100);
    mqc->heartbeat_dt = inip_get_integer(ifd, section, "heartbeat_dt", 5);
    mqc->heartbeat_failure = inip_get_integer(ifd, section, "heartbeat_failure", 60);
    mqc->min_ops_per_sec = inip_get_integer(ifd, section, "min_ops_per_sec", 100);

    // New socket_type parameter
    mqc->socket_type = inip_get_integer(ifd, section, "socket_type", MQ_TRACE_ROUTER);

    apr_pool_create(&(mqc->mpool), NULL);
    apr_thread_mutex_create(&(mqc->lock), APR_THREAD_MUTEX_DEFAULT, mqc->mpool);

    //** Make the thread pool.  All GOP commands run through here.  We replace
    //**  the TP submit routine with our own.
    mqc->tp = thread_pool_create_context("mq", mqc->min_threads, mqc->max_threads);
    mqc->pcfn = *(mqc->tp->pc->fn);
    mqc->pcfn.submit = _mq_submit_op;
    mqc->pcfn.sync_exec = NULL;
    mqc->tp->pc->fn = &(mqc->pcfn);
    int result;   
    result = (mqc->client_portals = apr_hash_make(mqc->mpool));
    assert(result != NULL);
    result = (mqc->server_portals = apr_hash_make(mqc->mpool));
    assert(result != NULL);
    
    atomic_set(mqc->n_ops, 0);
    
    return(mqc);
}

