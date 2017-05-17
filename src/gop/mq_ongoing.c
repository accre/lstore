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

//***********************************************************************
// MQ Ongoing task implementation
//***********************************************************************

#define _log_module_index 222

#include <apr.h>
#include <apr_errno.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "gop/gop.h"
#include "gop/opque.h"
#include "gop/types.h"
#include "mq_helpers.h"
#include "mq_ongoing.h"

//***********************************************************************
// ongoing_response_status - Handles a response that just returns the status
//***********************************************************************

gop_op_status_t ongoing_response_status(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    gop_op_status_t status;

    log_printf(2, "START\n");

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);
    log_printf(2, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}

//***********************************************************************
// ongoing_heartbeat_thread - Sends renewal heartbeats for ongoing objects
//***********************************************************************

void *ongoing_heartbeat_thread(apr_thread_t *th, void *data)
{
    gop_mq_ongoing_t *on = (gop_mq_ongoing_t *)data;
    apr_time_t timeout = apr_time_make(on->check_interval, 0);
    gop_op_generic_t *gop;
    mq_msg_t *msg;
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    apr_hash_index_t *hi, *hit;
    gop_opque_t *q;
    char *id;
    gop_mq_msg_hash_t *remote_hash;
    apr_time_t now;
    apr_ssize_t id_len;
    int n, k;
    char *remote_host_string;

    apr_thread_mutex_lock(on->lock);
    do {
        now = apr_time_now() - apr_time_from_sec(5);  //** Give our selves a little buffer
        log_printf(5, "Loop Start now=" TT "\n", apr_time_now());
        q = gop_opque_new();
//     opque_start_execution(q);
        for (hit = apr_hash_first(NULL, on->table); hit != NULL; hit = apr_hash_next(hit)) {
            apr_hash_this(hit, (const void **)&remote_hash, &id_len, (void **)&table);

            k = apr_hash_count(table->table);
            if (tbx_log_level() > 1) {
                remote_host_string = mq_address_to_string(table->remote_host);
                log_printf(1, "host=%s count=%d\n", remote_host_string, k);
                free(remote_host_string);
            }

            for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
                log_printf(1, "id=%s now=" TT " next_check=" TT "\n", oh->id,
                                    ((apr_time_t) apr_time_sec(apr_time_now())),
                                    ((apr_time_t) apr_time_sec(oh->next_check)));
                if (now > oh->next_check) {
                    log_printf(1, "id=%s sending HEARTBEAT EXEC SUBMIT nows=" TT "hb=%d\n",
                                        oh->id,
                                        ((apr_time_t) apr_time_sec(apr_time_now())),
                                        oh->heartbeat);
                    tbx_log_flush();
                    //** Form the message
                    msg = gop_mq_make_exec_core_msg(table->remote_host, 1);
                    gop_mq_msg_append_mem(msg, ONGOING_KEY, ONGOING_SIZE, MQF_MSG_KEEP_DATA);
                    gop_mq_msg_append_mem(msg, oh->id, oh->id_len, MQF_MSG_KEEP_DATA);
                    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

                    //** Make the gop
                    gop = gop_mq_op_new(on->mqc, msg, ongoing_response_status, NULL, NULL, oh->heartbeat);
                    gop_set_private(gop, table);
                    gop_opque_add(q, gop);

                    oh->in_progress = 1; //** Flag it as in progress so it doesn't get deleted behind the scenes
                }
            }

        }
        log_printf(5, "Loop end now=" TT "\n", apr_time_now());

        //** Wait for it to complete
        apr_thread_mutex_unlock(on->lock);
        opque_waitall(q);
        apr_thread_mutex_lock(on->lock);

        //** Dec the counters
        while ((gop = opque_waitany(q)) != NULL) {
            log_printf(0, "gid=%d gotone status=%d now=" TT "\n", gop_id(gop), (gop_get_status(gop)).op_status,
                    ((apr_time_t) apr_time_sec(apr_time_now())));
            table = gop_get_private(gop);
            table->count--;

            //** Update the next check
            for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
                oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);

                //** Check if we get rid of it
                oh->in_progress = 0;
                if (oh->count <= 0) {  //** Need to delete it
                    apr_hash_set(table->table, id, id_len, NULL);
                    free(oh->id);
                    free(oh);
                }
            }

            gop_free(gop, OP_DESTROY);
        }
        gop_opque_free(q, OP_DESTROY);

        now = apr_time_now();
        log_printf(2, "sleeping %d now=%" APR_TIME_T_FMT "\n", on->check_interval, now);

        //** Sleep until time for the next heartbeat or time to exit
        if (on->shutdown == 0) apr_thread_cond_timedwait(on->cond, on->lock, timeout);
        n = on->shutdown;

        now = apr_time_now() - now;
        log_printf(2, "main loop bottom n=%d dt=%" APR_TIME_T_FMT " sec=" TT "\n", n, now,
                        ((apr_time_t) apr_time_sec(now)));
    } while (n == 0);

    log_printf(2, "CLEANUP\n");

    for (hit = apr_hash_first(NULL, on->table); hit != NULL; hit = apr_hash_next(hit)) {
        apr_hash_this(hit, (const void **)&remote_hash, &id_len, (void **)&table);

        for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
            apr_hash_set(table->table, id, id_len, NULL);
            free(oh->id);
            free(oh);
        }

        apr_hash_set(on->table, &(table->remote_host_hash), sizeof(gop_mq_msg_hash_t), NULL);
        gop_mq_msg_destroy(table->remote_host);
        free(table);
    }

    log_printf(2, "EXITING\n");

    apr_thread_mutex_unlock(on->lock);

    return(NULL);
}


//***********************************************************************
// gop_mq_ongoing_host_inc - Adds a host for ongoing heartbeats
//***********************************************************************

void gop_mq_ongoing_host_inc(gop_mq_ongoing_t *on, mq_msg_t *remote_host, char *my_id, int id_len, int heartbeat)
{
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    gop_mq_msg_hash_t hash;
    char *remote_host_string;

    apr_thread_mutex_lock(on->lock);

    char *str = mq_address_to_string(remote_host);
    log_printf(5, "remote_host=%s\n", str);
    free(str);

    hash = gop_mq_msg_hash(remote_host);
    table = apr_hash_get(on->table, &hash, sizeof(gop_mq_msg_hash_t));  //** Look up the remote host

    if (tbx_log_level() > 5) {
        remote_host_string = mq_address_to_string(remote_host);
        log_printf(5, "remote_host=%s hb=%d table=%p\n", remote_host_string, heartbeat, table);
        free(remote_host_string);
    }
    if (table == NULL) { //** New host so add it
        tbx_type_malloc_clear(table, gop_ongoing_table_t, 1);
        table->table = apr_hash_make(on->mpool);FATAL_UNLESS(table->table != NULL);
        table->remote_host = gop_mq_msg_new();
        gop_mq_msg_append_msg(table->remote_host, remote_host, MQF_MSG_AUTO_FREE);
        table->remote_host_hash = hash;
        apr_hash_set(on->table, &(table->remote_host_hash), sizeof(gop_mq_msg_hash_t), table);
    }

    table->count++;

    oh = apr_hash_get(table->table, my_id, id_len);  //** Look up the id
    if (oh == NULL) { //** New host so add it
        tbx_type_malloc_clear(oh, gop_ongoing_hb_t, 1);
        tbx_type_malloc(oh->id, char, id_len);
        memcpy(oh->id, my_id, id_len);
        oh->id_len = id_len;
        oh->heartbeat = heartbeat / on->send_divisor;
        if (oh->heartbeat < 1) oh->heartbeat = 1;

        if (tbx_log_level() > 5) {
            remote_host_string = mq_address_to_string(remote_host);
            log_printf(5, "remote_host=%s final hb=%d \n", remote_host_string, oh->heartbeat);
            free(remote_host_string);
        }
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        apr_hash_set(table->table, oh->id, id_len, oh);
    }

    oh->count++;
    apr_thread_mutex_unlock(on->lock);
}

//***********************************************************************
// gop_mq_ongoing_host_dec - Decrements the tracking count to a host for ongoing heartbeats
//***********************************************************************

void gop_mq_ongoing_host_dec(gop_mq_ongoing_t *on, mq_msg_t *remote_host, char *id, int id_len)
{
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    gop_mq_msg_hash_t hash;

    hash = gop_mq_msg_hash(remote_host);
    apr_thread_mutex_lock(on->lock);
    table = apr_hash_get(on->table, &hash, sizeof(gop_mq_msg_hash_t));  //** Look up the host
    if (table == NULL) goto fail;

    table->count--;

    oh = apr_hash_get(table->table, id, id_len);  //** Look up the host
    if (oh != NULL) {
        oh->count--;
        if ((oh->count <= 0) && (oh->in_progress == 0)) {  //** Can delete the entry
            apr_hash_set(table->table, id, id_len, NULL);
            free(oh->id);
            free(oh);
        }
    }

fail:
    apr_thread_mutex_unlock(on->lock);
}

//***********************************************************************
// gop_mq_ongoing_add - Adds an onging object to the tracking tables
//***********************************************************************

gop_mq_ongoing_object_t *gop_mq_ongoing_add(gop_mq_ongoing_t *mqon, bool auto_clean, char *id, int id_len, void *handle, gop_mq_ongoing_fail_fn_t on_fail, void *on_fail_arg)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;

    tbx_type_malloc(ongoing, gop_mq_ongoing_object_t, 1);
    ongoing->handle = handle;
    ongoing->key = (intptr_t)handle;
    ongoing->on_fail = on_fail;
    ongoing->on_fail_arg = on_fail_arg;
    ongoing->count = 0;
    ongoing->auto_clean = auto_clean;

    log_printf(5, "host=%s len=%d handle=%p key=%" PRIdPTR "\n", id, id_len, handle, ongoing->key);

    apr_thread_mutex_lock(mqon->lock);

    //** Make sure the host entry exists
    oh = apr_hash_get(mqon->id_table, id, id_len);
    if (oh == NULL) {
        log_printf(5, "new host=%s\n", id);

        tbx_type_malloc(oh, gop_mq_ongoing_host_t, 1);
        tbx_type_malloc(oh->id, char, id_len+1);
        memcpy(oh->id, id, id_len);
        oh->id[id_len] = 0;  //** NULL terminate the string
        oh->id_len = id_len;

        oh->heartbeat = 60;
        sscanf(id, "%d:", &(oh->heartbeat));
        log_printf(5, "heartbeat interval=%d\n", oh->heartbeat);
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        assert_result(apr_pool_create(&(oh->mpool), NULL), APR_SUCCESS);
        oh->table = apr_hash_make(oh->mpool);

        apr_hash_set(mqon->id_table, oh->id, id_len, oh);
    }

    //** Add the object
    apr_hash_set(oh->table, &(ongoing->key), sizeof(intptr_t), ongoing);

    apr_thread_mutex_unlock(mqon->lock);

    return(ongoing);
}


//***********************************************************************
// gop_mq_ongoing_get - Retreives the handle if it's active
//***********************************************************************

void *gop_mq_ongoing_get(gop_mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;
    void *ptr = NULL;

    apr_thread_mutex_lock(mqon->lock);

    //** Get the host entry
    oh = apr_hash_get(mqon->id_table, id, id_len);
    log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
    if (oh != NULL) {
        ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
        if (ongoing != NULL) {
            log_printf(6, "Found!\n");
            ptr = ongoing->handle;
            ongoing->count++;
            oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        }
    }

    apr_thread_mutex_unlock(mqon->lock);

    return(ptr);
}

//***********************************************************************
// gop_mq_ongoing_release - Releases the handle active handle
//***********************************************************************

void gop_mq_ongoing_release(gop_mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;

    apr_thread_mutex_lock(mqon->lock);

    //** Get the host entry
    oh = apr_hash_get(mqon->id_table, id, id_len);
    log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
    if (oh != NULL) {
        ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
        if (ongoing != NULL) {
            log_printf(6, "Found!\n");
            ongoing->count--;
            oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);

            log_printf(2, "Updating heartbeat for %s hb=%d expire=" TT "\n",
                            id,
                            oh->heartbeat,
                            ((apr_time_t) apr_time_sec(oh->next_check)));

            apr_thread_cond_broadcast(mqon->cond); //** Let everyone know it;'s been released
        }
    }

    apr_thread_mutex_unlock(mqon->lock);

    return;
}

//***********************************************************************
// gop_mq_ongoing_remove - Removes an onging object from the tracking table
//***********************************************************************

void *gop_mq_ongoing_remove(gop_mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;
    void *ptr = NULL;

    apr_thread_mutex_lock(mqon->lock);

    //** Get the host entry
    oh = apr_hash_get(mqon->id_table, id, id_len);
    log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
    if (oh != NULL) {
        log_printf(6, "Found host=%s key=%" PRIdPTR "\n", oh->id, key);

        ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
        if (ongoing != NULL) {
            log_printf(6, "Found handle\n");
            ptr = ongoing->handle;
            while (ongoing->count > 0) {
                apr_thread_cond_wait(mqon->cond, mqon->lock);
            }
            apr_hash_set(oh->table, &key, sizeof(intptr_t), NULL);
            free(ongoing);
        }
    }

    apr_thread_mutex_unlock(mqon->lock);

    return(ptr);
}


//***********************************************************************
// mq_ongoing_cb - Processes a heartbeat for clients with ongoing open handles
//***********************************************************************

void mq_ongoing_cb(void *arg, gop_mq_task_t *task)
{
    gop_mq_ongoing_t *mqon = (gop_mq_ongoing_t *)arg;
    gop_mq_frame_t *fid, *fuid;
    char *id;
    gop_mq_ongoing_host_t *oh;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(1, "Processing incoming request. EXEC START now=" TT "\n",
                    ((apr_time_t) apr_time_sec(apr_time_now())));
    tbx_log_flush();

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    log_printf(2, "looking up %s\n", id);

    //** Do the lookup and update the heartbeat timeout
    apr_thread_mutex_lock(mqon->lock);
    oh = apr_hash_get(mqon->id_table, id, fsize);
    if (oh != NULL) {
        status = gop_success_status;
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        log_printf(2, "Updating heartbeat for %s hb=%d expire=" TT "\n", id, oh->heartbeat, oh->next_check);
    } else {
        status = gop_failure_status;
    }
    apr_thread_mutex_unlock(mqon->lock);

    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(mqon->server_portal, gop_mq_task_new(mqon->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// _mq_ongoing_close - Closes all ongoing objects associated with the connection
//     NOTE:  Assumes the ongoing_lock is held by the calling process
//***********************************************************************

int _mq_ongoing_close(gop_mq_ongoing_t *mqon, gop_mq_ongoing_host_t *oh, gop_opque_t *q)
{
    apr_hash_index_t *hi;
    char *key;
    apr_ssize_t klen;
    gop_mq_ongoing_object_t *oo;
    gop_op_generic_t *gop;
    int ntasks;

    int n = apr_hash_count(oh->table);
    log_printf(2, "closing host=%s task_count=%d now=" TT " next_check=" TT " hb=%d\n", oh->id, n, apr_time_now(), oh->next_check, oh->heartbeat);

    ntasks = 0;

    for (hi = apr_hash_first(NULL, oh->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&oo);

        if (oo->auto_clean) apr_hash_set(oh->table, key, klen, NULL);  //** I'm cleaning up so remove it from the table

        gop = oo->on_fail(oo->on_fail_arg, oo->handle);
        gop_set_private(gop, (oo->auto_clean == 1) ? oo : NULL);
        gop_opque_add(q, gop);

        ntasks++;
    }

    return(ntasks);
}

//***********************************************************************
// mq_ongoing_server_thread - Checks to make sure heartbeats are being
//     being received from clients with open handles
//***********************************************************************

void *mq_ongoing_server_thread(apr_thread_t *th, void *data)
{
    gop_mq_ongoing_t *mqon = (gop_mq_ongoing_t *)data;
    apr_hash_index_t *hi;
    gop_mq_ongoing_host_t *oh;
    gop_mq_ongoing_object_t *oo;
    char *key;
    apr_ssize_t klen;
    apr_time_t now, timeout;
    int n, ntasks;
    gop_opque_t *q;
    gop_op_generic_t *gop;

    timeout = apr_time_make(mqon->check_interval, 0);

    q = gop_opque_new();
    opque_start_execution(q);

    apr_thread_mutex_lock(mqon->lock);
    do {
        //** Cycle through checking each host's heartbeat
        now = apr_time_now();
        n = apr_hash_count(mqon->id_table);
        log_printf(10, "now=" TT " heartbeat table size=%d\n", apr_time_now(), n);
        ntasks = 0;
        for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);

            log_printf(10, "host=%s now=" TT " next_check=" TT "\n", oh->id,
                    ((apr_time_t) apr_time_sec(apr_time_now())),
                    ((apr_time_t) apr_time_sec(oh->next_check)));
            if ((oh->next_check < now) && (oh->next_check > 0)) { //** Expired heartbeat so shut everything associated with the connection
                ntasks += _mq_ongoing_close(mqon, oh, q);
                oh->next_check = 0;  //** Skip next time around
            } else if (oh->next_check == 0) { //** See if everything has cleaned up
                if (apr_hash_count(oh->table) == 0) { //** Safe to clean up
                    apr_hash_set(mqon->id_table, key, klen, NULL);
                    free(oh->id);
                    apr_pool_destroy(oh->mpool);
                    free(oh);
                }
            }
        }

        if (ntasks > 0) { //** Close some connections so wait for the tasks to complete
            while ((gop = opque_waitany(q)) != NULL) {
                oo = gop_get_private(gop);
                if (oo) free(oo);
                gop_free(gop, OP_DESTROY);
            }
        }

        //** Sleep until time for the next heartbeat or time to exit
        apr_thread_cond_timedwait(mqon->cond, mqon->lock, timeout);
        n = mqon->shutdown;

        log_printf(15, "loop end n=%d\n", n);
    } while (n == 0);

    log_printf(15, "CLEANUP\n");

    ntasks = 0;
    for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);
        if (oh->next_check > 0) ntasks += _mq_ongoing_close(mqon, oh, q);  //** Only shut down pending hosts

        while (apr_hash_count(oh->table) > 0) {
            n = apr_hash_count(oh->table);
            log_printf(5, "waiting on host=%s nleft=%d\n", oh->id, n);
            apr_thread_mutex_unlock(mqon->lock);
            usleep(10000);  //** Sleep and see if if clears up
            apr_thread_mutex_lock(mqon->lock);
        }

        apr_hash_set(mqon->id_table, key, klen, NULL);
        free(oh->id);
        apr_pool_destroy(oh->mpool);
        free(oh);
    }

    log_printf(15, "FINISHED\n");

    apr_thread_mutex_unlock(mqon->lock);

    if (ntasks > 0) { //** Closed some connections with tasks so wait for them to complete
        while ((gop = opque_waitany(q)) != NULL) {
            oo = gop_get_private(gop);
            if (oo) free(oo);
            gop_free(gop, OP_DESTROY);
        }
    }

    log_printf(15, "EXITING\n");

    gop_opque_free(q, OP_DESTROY);

    return(NULL);
}

//***********************************************************************
// gop_mq_ongoing_destroy - Destroys an ongoing task tracker
//***********************************************************************

void gop_mq_ongoing_destroy(gop_mq_ongoing_t *mqon)
{
    apr_status_t value;
    gop_mq_command_table_t *ctable;

    //** Wake up the ongoing thread
    apr_thread_mutex_lock(mqon->lock);
    mqon->shutdown = 1;
    apr_thread_cond_broadcast(mqon->cond);
    apr_thread_mutex_unlock(mqon->lock);

    //** Wait for it to shutdown
    if (mqon->ongoing_server_thread) {
        ctable = gop_mq_portal_command_table(mqon->server_portal);
        gop_mq_command_set(ctable, ONGOING_KEY, ONGOING_SIZE, mqon, NULL);

        apr_thread_join(&value, mqon->ongoing_server_thread);
    }

    if (mqon->ongoing_heartbeat_thread) apr_thread_join(&value, mqon->ongoing_heartbeat_thread);

    apr_pool_destroy(mqon->mpool);
    free(mqon);
}

//***********************************************************************
// gop_mq_ongoing_create - Creates an object to handle ongoing tasks
//***********************************************************************

gop_mq_ongoing_t *gop_mq_ongoing_create(gop_mq_context_t *mqc, gop_mq_portal_t *server_portal, int check_interval, int mode)
{
    gop_mq_ongoing_t *mqon;
    gop_mq_command_table_t *ctable;

    tbx_type_malloc_clear(mqon, gop_mq_ongoing_t, 1);

    mqon->mqc = mqc;
    mqon->server_portal = server_portal;
    mqon->check_interval = check_interval;
    mqon->send_divisor = 4;

    assert_result(apr_pool_create(&(mqon->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(mqon->lock), APR_THREAD_MUTEX_DEFAULT, mqon->mpool);
    apr_thread_cond_create(&(mqon->cond), mqon->mpool);

    if (mode & ONGOING_SERVER) {
        mqon->id_table = apr_hash_make(mqon->mpool);
       FATAL_UNLESS(mqon->id_table != NULL);

        ctable = gop_mq_portal_command_table(server_portal);
        gop_mq_command_set(ctable, ONGOING_KEY, ONGOING_SIZE, mqon, mq_ongoing_cb);
        tbx_thread_create_assert(&(mqon->ongoing_server_thread), NULL, mq_ongoing_server_thread, (void *)mqon, mqon->mpool);
    }

    if (mode & ONGOING_CLIENT) {
        mqon->table = apr_hash_make(mqon->mpool);
       FATAL_UNLESS(mqon->table != NULL);

        tbx_thread_create_assert(&(mqon->ongoing_heartbeat_thread), NULL, ongoing_heartbeat_thread, (void *)mqon, mqon->mpool);
    }
    return(mqon);
}


