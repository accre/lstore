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
// MQ Streaming data implementation
//***********************************************************************

#define _log_module_index 223

#include <apr_errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/packer.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#include "gop.h"
#include "mq_helpers.h"
#include "mq_stream.h"

//***********************************************************************
// mqs_response_client_more - Handles a response for more data from the server
//***********************************************************************

gop_op_status_t mqs_response_client_more(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    gop_mq_stream_t *mqs = (gop_mq_stream_t *)task->arg;
    gop_op_status_t status = gop_success_status;

    log_printf(5, "START msid=%d\n", mqs->msid);

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    //** Wait for a notification that the data can be processed
    apr_thread_mutex_lock(mqs->lock);
    log_printf(5, "INIT STATUS msid=%d waiting=%d processed=%d\n", mqs->msid, mqs->waiting, mqs->processed);
    mqs->transfer_packets++;
    mqs->waiting = 0;
    apr_thread_cond_broadcast(mqs->cond);

    //** Now wait until the application is ready
    while (mqs->waiting == 0) {
        apr_thread_cond_wait(mqs->cond, mqs->lock);
    }

    //** Can accept the data
    gop_mq_get_frame(gop_mq_msg_first(task->response), (void **)&(mqs->data), &(mqs->len));
    tbx_pack_read_new_data(mqs->pack, &(mqs->data[MQS_HEADER]), mqs->len-MQS_HEADER);

    //** Notify the consumer it's available
    mqs->waiting = -2;
    apr_thread_cond_broadcast(mqs->cond);

    //** Wait until it's consumed
    while (mqs->processed == 0) {
        apr_thread_cond_wait(mqs->cond, mqs->lock);
    }

    mqs->data = NULL;    //** Nullify the data so it's not accidentally accessed
    mqs->processed = 0;  //** and reset the processed flag back to 0

    apr_thread_mutex_unlock(mqs->lock);

    log_printf(5, "END msid=%d status=%d %d\n", mqs->msid, status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
// gop_mq_stream_read_request - Places a request for more data
//***********************************************************************

void gop_mq_stream_read_request(gop_mq_stream_t *mqs)
{
    mq_msg_t *msg;

    //** If 1st time make all the variables
    if (mqs->mpool == NULL) {
        apr_pool_create(&mqs->mpool, NULL);
        apr_thread_mutex_create(&(mqs->lock), APR_THREAD_MUTEX_DEFAULT, mqs->mpool);
        apr_thread_cond_create(&(mqs->cond), mqs->mpool);
    }

    log_printf(5, "msid=%d want_more=%c\n", mqs->msid, mqs->want_more);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(mqs->remote_host, 1);
    gop_mq_msg_append_mem(msg, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, mqs->host_id, mqs->hid_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, mqs->stream_id, mqs->sid_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, &(mqs->want_more), 1, MQF_MSG_KEEP_DATA);  //** Want more data
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    mqs->gop_waiting = gop_mq_op_new(mqs->mqc, msg, mqs_response_client_more, mqs, NULL, mqs->timeout);

    //** Start executing it

    apr_thread_mutex_lock(mqs->lock);
    mqs->waiting = 1;
    apr_thread_mutex_unlock(mqs->lock);
    gop_start_execution(mqs->gop_waiting);
}

//***********************************************************************
// gop_mq_stream_read_wait - Waits for data to become available
//***********************************************************************

int gop_mq_stream_read_wait(gop_mq_stream_t *mqs)
{
    int err = 0;
    apr_interval_time_t dt;
    gop_op_status_t status;

    //** If 1st time make all the variables
    if (mqs->mpool == NULL) {
        apr_pool_create(&mqs->mpool, NULL);
        apr_thread_mutex_create(&(mqs->lock), APR_THREAD_MUTEX_DEFAULT, mqs->mpool);
        apr_thread_cond_create(&(mqs->cond), mqs->mpool);
    }

    dt = apr_time_from_sec(1);


    //** Flag the just processed gop to clean up
    apr_thread_mutex_lock(mqs->lock);
    log_printf(5, "START msid=%d waiting=%d processed=%d gop_processed=%p\n", mqs->msid, mqs->waiting, mqs->processed, mqs->gop_processed);
    if (mqs->gop_processed != NULL) mqs->processed = 1;
    apr_thread_cond_broadcast(mqs->cond);

    if (mqs->data) {
        if (mqs->data[MQS_STATE_INDEX] != MQS_MORE) err = 1;
    }
    apr_thread_mutex_unlock(mqs->lock);

    if (mqs->gop_processed != NULL) {
        gop_waitany(mqs->gop_processed);
        gop_free(mqs->gop_processed, OP_DESTROY);
        mqs->gop_processed = NULL;
    }

    if (err != 0) {
        log_printf(2, "ERROR no more data available!\n");
        return(-1);
    }

    //** Now handle the waiting gop
    apr_thread_mutex_lock(mqs->lock);
    log_printf(5, "before loop msid=%d waiting=%d processed=%d\n", mqs->msid, mqs->waiting, mqs->processed);

    while (mqs->waiting == 1) {
        log_printf(5, "LOOP msid=%d waiting=%d processed=%d\n", mqs->msid, mqs->waiting, mqs->processed);
        if (gop_will_block(mqs->gop_waiting) == 0)  { //** Oops!  failed request
            status = gop_get_status(mqs->gop_waiting);
            log_printf(2, "msid=%d gid=%d status=%d\n", mqs->msid, gop_id(mqs->gop_waiting), status.op_status);
            if (status.op_status != OP_STATE_SUCCESS) {
                mqs->waiting = -3;
                err = 1;
            } else {
                apr_thread_cond_timedwait(mqs->cond, mqs->lock, dt);
            }
        } else {
            apr_thread_cond_timedwait(mqs->cond, mqs->lock, dt);
        }
    }

    if (mqs->waiting == 0) {  //** Flag the receiver to accept the data
        mqs->waiting = -1;
        apr_thread_cond_broadcast(mqs->cond);  //** Let the receiver know we're ready to accept the data

        //** Wait for the data to be accepted
        while (mqs->waiting != -2) {
            apr_thread_cond_wait(mqs->cond, mqs->lock);
        }
    } else if (mqs->waiting == -3) { //**error occured
        err = 1;
    }
    apr_thread_mutex_unlock(mqs->lock);

    //** Flip states
    mqs->gop_processed = mqs->gop_waiting;
    mqs->gop_waiting = NULL;

    //** This shouldn't get triggered but just in case lets throw an error.
    if ((mqs->gop_processed == NULL) && (mqs->data != NULL)) {
        if ((mqs->data[MQS_STATE_INDEX] == MQS_MORE) && (mqs->want_more == MQS_MORE)) {
            err = 3;
            log_printf(0, "ERROR: MQS gop processed=waiting=NULL  want_more set!!!!!! err=%d\n", err);
            fprintf(stderr, "ERROR: MQS gop processed=waiting=NULL want_more set!!!!!! err=%d\n", err);
        }
    }

    //** Check if we need to fire off the next request
    if (mqs->data != NULL) {
        if ((mqs->data[MQS_STATE_INDEX] == MQS_MORE) && (mqs->want_more == MQS_MORE)) {
            gop_mq_stream_read_request(mqs);
        }
    }

    log_printf(5, "err=%d\n", err);
    return(err);
}

//***********************************************************************
// gop_mq_stream_read - Reads data from the stream
//***********************************************************************

int gop_mq_stream_read(gop_mq_stream_t *mqs, void *rdata, int len)
{
    int nleft, dpos, nbytes, err;
    unsigned char *data = rdata;

    if (len == 0) return(0);

    nleft = len;
    dpos = 0;
    err = 0;
    do {
        nbytes = tbx_pack_read(mqs->pack, &(data[dpos]), nleft);
        log_printf(2, "msid=%d len=%d nleft=%d dpos=%d nbytes=%d err=%d\n", mqs->msid, len, nleft, dpos, nbytes, err);

        if (nbytes >= 0) {  //** Read some data
            nleft -= nbytes;
            dpos += nbytes;

            if (nleft > 0) {  //** Need to wait for more data
                err = gop_mq_stream_read_wait(mqs);
                log_printf(2, "msid=%d after read_wait len=%d nleft=%d dpos=%d nbytes=%d err=%d\n", mqs->msid, len, nleft, dpos, nbytes, err);
            }

        } else {
            log_printf(0, "ERROR got %i from tbx_pack_read\n", nbytes);
            err = nbytes;
        }
    } while ((nleft > 0) && (err == 0));

    return(err);
}

//***********************************************************************
// gop_mq_stream_read_varint - Reads a zigzag encoded varint from the stream
//***********************************************************************

int64_t gop_mq_stream_read_varint(gop_mq_stream_t *mqs, int *error)
{
    unsigned char buffer[16];
    int64_t value;
    int i, err;

    for (i=0; i<16; i++) {
        err = gop_mq_stream_read(mqs, &(buffer[i]), 1);
        if (err != 0) break;
        if (tbx_varint_need_more(buffer[i]) == 0) break;
    }

    if (err == 0) {
        *error = 0;
        tbx_zigzag_decode(buffer, i+1, &value);
    } else {
        value = 0;
        *error = err;
    }

    log_printf(10, "value=" I64T "\n", value);

    return(value);
}

//***********************************************************************
// gop_mq_stream_read_destroy - Destroys an MQ reading stream
//***********************************************************************

void gop_mq_stream_read_destroy(gop_mq_stream_t *mqs)
{

    log_printf(1, "START msid=%d\n", mqs->msid);

    if (mqs->mpool == NULL) {  //** Nothing to do
        tbx_pack_destroy(mqs->pack);
        if (mqs->stream_id != NULL) free(mqs->stream_id);
        free(mqs);
        return;
    }

    //** Change the flag which signals we don't want anything else
    apr_thread_mutex_lock(mqs->lock);
    mqs->want_more = MQS_ABORT;

    //** Consume all the current data and request the pending
    while ((mqs->gop_processed != NULL) || (mqs->gop_waiting != NULL)) {
        log_printf(1, "Clearing pending processed=%p waiting=%p msid=%d\n", mqs->gop_processed, mqs->gop_waiting, mqs->msid);
        if (mqs->gop_processed != NULL) log_printf(1, "processed gid=%d\n", gop_id(mqs->gop_processed));
        if (mqs->gop_waiting != NULL) log_printf(1, "waiting gid=%d\n", gop_id(mqs->gop_waiting));
        mqs->want_more = MQS_ABORT;
        apr_thread_mutex_unlock(mqs->lock);
        gop_mq_stream_read_wait(mqs);
        apr_thread_mutex_lock(mqs->lock);
    }

    if (tbx_log_level() >= 15) {
        char *rhost = mq_address_to_string(mqs->remote_host);
        log_printf(15, "remote_host as string = %s\n", rhost);
        if (rhost) free(rhost);
    }
    if (mqs->remote_host != NULL) gop_mq_ongoing_host_dec(mqs->ongoing, mqs->remote_host, mqs->host_id, mqs->hid_len);

    apr_thread_mutex_unlock(mqs->lock);

    log_printf(2, "msid=%d transfer_packets=%d\n", mqs->msid, mqs->transfer_packets);

    //** Clean up
    if (mqs->stream_id != NULL) free(mqs->stream_id);
    tbx_pack_destroy(mqs->pack);
    apr_thread_mutex_destroy(mqs->lock);
    apr_thread_cond_destroy(mqs->cond);
    apr_pool_destroy(mqs->mpool);
    if (mqs->remote_host != NULL) gop_mq_msg_destroy(mqs->remote_host);
    free(mqs);

    return;
}


//***********************************************************************
// gop_mq_stream_read_create - Creates an MQ stream for reading
//***********************************************************************

gop_mq_stream_t *gop_mq_stream_read_create(gop_mq_context_t *mqc, gop_mq_ongoing_t *on, char *host_id, int hid_len, gop_mq_frame_t *fdata, mq_msg_t *remote_host, int to)
{
    gop_mq_stream_t *mqs;
    int ptype;

    tbx_type_malloc_clear(mqs, gop_mq_stream_t, 1);

    mqs->mqc = mqc;
    mqs->ongoing = on;
    mqs->type = MQS_READ;
    mqs->want_more = MQS_MORE;
    mqs->host_id = host_id;
    mqs->hid_len = hid_len;
    mqs->timeout = to;
    mqs->msid = tbx_atomic_global_counter();

    if (tbx_log_level() > 5) {
        char *str = mq_address_to_string(remote_host);
        log_printf(5, "remote_host=%s\n", str);
        if (str) free(str);
    }

    gop_mq_get_frame(fdata, (void **)&(mqs->data), &(mqs->len));

    mqs->sid_len = mqs->data[MQS_HANDLE_SIZE_INDEX];
    tbx_type_malloc(mqs->stream_id, char, mqs->sid_len);
    memcpy(mqs->stream_id, &(mqs->data[MQS_HANDLE_INDEX]), mqs->sid_len);

    ptype = (mqs->data[MQS_PACK_INDEX] == MQS_PACK_COMPRESS) ? PACK_COMPRESS : PACK_NONE;
    log_printf(1, "msid=%d ptype=%d tbx_pack_type=%c\n", mqs->msid, ptype, mqs->data[MQS_PACK_INDEX]);
    mqs->pack = tbx_pack_create(ptype, PACK_READ, &(mqs->data[MQS_HEADER]), mqs->len - MQS_HEADER);

    log_printf(5, "data_len=%d more=%c MQS_HEADER=%lu\n", mqs->len, mqs->data[MQS_STATE_INDEX], MQS_HEADER);

    unsigned char buffer[1024];
    int n = (50 > mqs->len) ? mqs->len : 50;
    log_printf(5, "printing 1st 50 bytes mqsbuf=%s\n", gop_mq_id2str((char *)mqs->data, n, (char *)buffer, 1024));

    if (mqs->data[MQS_STATE_INDEX] == MQS_MORE) { //** More data coming so ask for it
        log_printf(5, "issuing read request\n");

        mqs->remote_host = gop_mq_msg_new();
        gop_mq_msg_append_msg(mqs->remote_host, remote_host, MQF_MSG_AUTO_FREE);

        if (tbx_log_level() >=15) {
            char *rhost = mq_address_to_string(mqs->remote_host);
            log_printf(15, "remote_host as string = %s\n", rhost);
            if (rhost) free(rhost);
        }

        log_printf(5, "before ongoing_inc\n");
        gop_mq_ongoing_host_inc(mqs->ongoing, mqs->remote_host, mqs->host_id, mqs->hid_len, mqs->timeout);
        log_printf(5, "after ongoing_inc\n");
        gop_mq_stream_read_request(mqs);
    }

    log_printf(5, "END\n");

    return(mqs);
}

//***********************************************************************
//-----------------------------------------------------------------------
//***********************************************************************

//***********************************************************************
// mqs_write_send - Forms and sends a write response
//  **NOTE: Assumes mqs is locked!!!! ***
//***********************************************************************

int mqs_write_send(gop_mq_stream_t *mqs, mq_msg_t *address, gop_mq_frame_t *fid)
{
    int err;
    unsigned char *new_data;
    mq_msg_t *response;

    mqs->sent_data = 1;

    if (mqs->data == NULL) return(-1);

    log_printf(1, "msid=%d address frame count=%d state_index=%c\n", mqs->msid, tbx_stack_count(address), mqs->data[MQS_STATE_INDEX]);
    response = gop_mq_make_response_core_msg(address, fid);
    gop_mq_msg_append_mem(response, mqs->data, MQS_HEADER + tbx_pack_used(mqs->pack), MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(2, "nbytes=%d more=%c\n", tbx_pack_used(mqs->pack), mqs->data[MQS_STATE_INDEX]);

    mqs->unsent_data = 0;
    if (mqs->data[MQS_STATE_INDEX] == MQS_MORE) {
        tbx_type_malloc(new_data, unsigned char, mqs->len);
        memcpy(new_data, mqs->data, MQS_HEADER);
        mqs->data = new_data;
        mqs->unsent_data = 1;

        //** Reset the packer to use the new buffer
        tbx_pack_consumed(mqs->pack);
        tbx_pack_write_resized(mqs->pack, &(mqs->data[MQS_HEADER]), mqs->len-MQS_HEADER);
    } else {
        tbx_pack_consumed(mqs->pack);
        tbx_pack_write_resized(mqs->pack, NULL, 0);
        mqs->data = NULL;  //** Make sure it's not accidentally referenced
    }

    mqs->transfer_packets++;

    err = gop_mq_submit(mqs->server_portal, gop_mq_task_new(mqs->mqc, response, NULL, NULL, 30));
    if (err != 0) {
        log_printf(5, "ERROR with gop_mq_submit=%d\n", err);
        mqs->want_more = MQS_ABORT;
    }

    return(err);
}

//***********************************************************************
// mqs_flusher_thread - Makes sure the write sends a response before the timeout
//***********************************************************************

void *mqs_flusher_thread(apr_thread_t *th, void *arg)
{
    gop_mq_stream_t *mqs = (gop_mq_stream_t *)arg;
    apr_time_t wakeup;

    log_printf(1, "START: msid=%d\n", mqs->msid);

    //** Figure out when to flush
    if (mqs->timeout > 60) {
        wakeup = apr_time_from_sec(mqs->timeout - 20);
    } else if (mqs->timeout > 5) {
        wakeup = apr_time_from_sec(mqs->timeout - 5);
    } else {
        wakeup = apr_time_from_sec(1);
    }

    //** Sleep until needed
    apr_thread_mutex_lock(mqs->lock);
    mqs->waiting = 1;
    if (mqs->ready == 0) apr_thread_cond_timedwait(mqs->cond, mqs->lock, wakeup);

    log_printf(5, "msid=%d want_more=%c state_index=%c data=%p\n", mqs->msid, mqs->want_more, mqs->data[MQS_STATE_INDEX], mqs->data);
    //** Form the message. NOTE that the rest of the message is the address
    if (mqs->want_more == MQS_ABORT) {
        if (mqs->data != NULL) mqs->data[MQS_STATE_INDEX] = mqs->want_more;
    } else if (mqs->data != NULL) {
        if (mqs->data[MQS_STATE_INDEX] != MQS_MORE) mqs->want_more = mqs->data[MQS_STATE_INDEX];
    }

    mqs_write_send(mqs, mqs->address, mqs->fid);
    tbx_pack_consumed(mqs->pack);
    mqs->expire = 0;  //** The new stream request will set this
    mqs->ready = 0;
    mqs->waiting = 0;
    tbx_pack_consumed(mqs->pack);
    apr_thread_cond_broadcast(mqs->cond);

    apr_thread_mutex_unlock(mqs->lock);

    log_printf(1, "END: msid=%d\n", mqs->msid);

    //** We can exit now cause it's up to the caller to send a new request
    //** Which will be handled by the callback
    return(NULL);
}

//***********************************************************************
// gop_mqs_server_more_cb - Sends more data to the client
//***********************************************************************

void gop_mqs_server_more_cb(void *arg, gop_mq_task_t *task)
{
    gop_mq_ongoing_t *ongoing = (gop_mq_ongoing_t *)arg;
    gop_mq_stream_t *mqs;
    mq_msg_t *msg;
    gop_mq_frame_t *f, *fid, *fmqs, *fuid;
    unsigned char *data, *id, mode;
    apr_time_t wakeup;
    intptr_t key;
    int len, id_size, err;
    int64_t timeout;


    log_printf(5, "START\n");

    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level
    err = -1;

    //** Parse the response
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for the client response
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID for ongoing lookup
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    fmqs = mq_msg_pop(msg);  //** This is the MQS handle
    gop_mq_get_frame(fmqs, (void **)&data, &len);
    log_printf(5, "id_size=%d handle_len=%d\n", id_size, len);
    key = *(intptr_t *)data;
    if ((mqs = gop_mq_ongoing_get(ongoing, (char *)id, id_size, key)) == NULL) {
        log_printf(5, "Invalid handle!\n");
        goto fail;
    }

    log_printf(1, "msid=%d\n", mqs->msid);

    f = mq_msg_pop(msg);     //** This is the mode MQS_MORE or MQS_ABORT along with the timeout
    gop_mq_get_frame(f, (void **)&data, &len);
    if (len == 1) {
        if (data[0] == MQS_MORE) {
            mode = MQS_MORE;
        } else if (data[0] == MQS_ABORT) {
            mode = MQS_ABORT;
        } else {
            log_printf(5, "Invalid mode! Triggering an abort. mode=%c\n", data[0]);
            mode = MQS_ABORT;
        }
    } else {
        log_printf(5, "Invalid mode size=%d! Triggering an abort.\n", len);
        mode = MQS_ABORT;
    }

    //** Now read the timeout
    timeout = mqs->timeout;

    gop_mq_frame_destroy(f);

    //** Notify the streamer that we can move data
    apr_thread_mutex_lock(mqs->lock);
    mqs->waiting = 1;
    mqs->expire = apr_time_now() + apr_time_from_sec(timeout);
    apr_thread_cond_broadcast(mqs->cond);

    mqs->want_more = mode;

    //** Now wait until the application is ready or we are going to timeout
    if (timeout > 60) {
        wakeup = apr_time_from_sec(timeout - 20);
    } else if (mqs->timeout > 5) {
        wakeup = apr_time_from_sec(timeout - 5);
    } else {
        wakeup = apr_time_from_sec(1);
    }
    wakeup += apr_time_now();

    log_printf(1, "Waiting for application to consume data msid=%d\n", mqs->msid);
    err = 0;
    while ((mqs->ready == 0) && (mqs->want_more == MQS_MORE)) {
        apr_thread_cond_timedwait(mqs->cond, mqs->lock, apr_time_from_sec(1));
        if (apr_time_now() > wakeup) {
            err = 1;     //** Kick out and send whatever's there
            mqs->ready = 1;
        }
        log_printf(1, "msid=%d err=%d ready=%d want_more=%c\n", mqs->msid, err, mqs->ready, mqs->want_more);
    }
    log_printf(1, "Application has consumed the data msid=%d err=%d\n", mqs->msid, err);

    //** Form the message. NOTE that the rest of the message is the address
    if (mqs->want_more == MQS_ABORT) {
        if (mqs->data != NULL) mqs->data[MQS_STATE_INDEX] = mqs->want_more;
    } else if (mqs->data != NULL) {
        if (mqs->data[MQS_STATE_INDEX] != MQS_MORE) mqs->want_more = mqs->data[MQS_STATE_INDEX];
    }

    err = mqs_write_send(mqs, msg, fid);

    if (err != 0) {
        log_printf(5, "ERROR during send! Triggering an abort.\n");
        mqs->want_more = MQS_ABORT;
    }

    //** Let caller know it's sent and reset the size
    mqs->ready = 0;
    mqs->waiting = 0;
    mqs->expire = 0;
    err = mqs->msid;
    tbx_pack_consumed(mqs->pack);
    apr_thread_cond_broadcast(mqs->cond);
    apr_thread_mutex_unlock(mqs->lock);

    gop_mq_ongoing_release(ongoing, (char *)id, id_size, key);  //** Do this to avoiud a deadlock on failure in mqs_on_fail()

fail:
    log_printf(1, "END msid=%d\n", err);

    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fmqs);

    return;
}


//***********************************************************************
// gop_mq_stream_write_flush - Flushs pending data
//***********************************************************************

int gop_mq_stream_write_flush(gop_mq_stream_t *mqs)
{
    int err = 0;
    apr_interval_time_t dt, expire;

    dt = apr_time_from_sec(1);

    //** Let a pending call know we have data ready
    apr_thread_mutex_lock(mqs->lock);
    expire = apr_time_now() + apr_time_from_sec(mqs->timeout);
    mqs->ready = 1;
    apr_thread_cond_broadcast(mqs->cond);

    //** Now wait for the pending call acknowledgement
    log_printf(1, "Flushing stream msid=%d now=" TT " timeout(s)=%d waiting=%d\n", mqs->msid, apr_time_now(), mqs->timeout, mqs->waiting);
    while (mqs->waiting == 0) {
        if (((mqs->want_more == MQS_ABORT) || (mqs->data == NULL) || mqs->dead_connection == 1))  { //** Oops! No client request or abort flagged
            if (apr_time_now() > expire) log_printf(0, "EXPIRED msid=%d now=" TT " expire= " TT " timeout(s)=%d\n", mqs->msid, apr_time_now(), expire, mqs->timeout);
            mqs->waiting = -3;
            err = 1;
        } else {
            apr_thread_cond_timedwait(mqs->cond, mqs->lock, dt);
        }
    }

    while ((mqs->ready == 1) && (mqs->waiting != -3)) {
        apr_thread_cond_timedwait(mqs->cond, mqs->lock, dt);
        log_printf(1, "waiting for send to complete msid=%d ready=%d now=" TT " timeout(s)=%d\n", mqs->msid, mqs->ready, apr_time_now(), mqs->timeout);
    }

    log_printf(1, "Stream flush completed.  mqs->waiting=%d msid=%d want_more=%c dead=%d data=%p now=" TT "\n", mqs->waiting, mqs->msid, mqs->want_more, mqs->dead_connection, mqs->data, apr_time_now());

    if ((err == 0) && (mqs->waiting < -1)) err = 1;  //** Check if sending had an error

    apr_thread_mutex_unlock(mqs->lock);

    return(err);
}

//***********************************************************************
// mqs_write_on_fail - Handles a client write failure
//***********************************************************************

gop_op_generic_t *mqs_write_on_fail(void *arg, void *handle)
{
    gop_mq_stream_t *mqs = (gop_mq_stream_t *)handle;

    apr_thread_mutex_lock(mqs->lock);
    mqs->want_more = MQS_ABORT;
    mqs->dead_connection = 1;
    apr_thread_cond_broadcast(mqs->cond);
    apr_thread_mutex_unlock(mqs->lock);

    return(gop_dummy(gop_success_status));
}

//***********************************************************************
// gop_mq_stream_write - Writes data to the stream
//***********************************************************************

int gop_mq_stream_write(gop_mq_stream_t *mqs, void *vdata, int len)
{
    int nleft, dpos, nbytes, err, grew_space;
    unsigned char *data = vdata;

    nleft = len;
    dpos = 0;
    err = 0;
    if (mqs->mpool != NULL) {
        apr_thread_mutex_lock(mqs->lock);
    }

    log_printf(5, "START bpos=%d len=%d msid=%d\n", tbx_pack_used(mqs->pack), len, mqs->msid);

    do {
        grew_space = 0;
        nbytes = mqs->len - tbx_pack_used(mqs->pack) - MQS_HEADER;
        log_printf(5, "nbytes=%d mqs->len=%d mqs->max_size=%d\n", nbytes, mqs->len, mqs->max_size);
        if (nbytes < nleft) { //** See if we can grow the space
            if (mqs->len < mqs->max_size) {
                nbytes = 2 * mqs->len + nleft;
                if (nbytes > mqs->max_size) nbytes = mqs->max_size;

                log_printf(5, "growing space=%d\n", nbytes);
                tbx_type_realloc(mqs->data, unsigned char, nbytes);
                mqs->len = nbytes;
                tbx_pack_write_resized(mqs->pack, &(mqs->data[MQS_HEADER]), mqs->len - MQS_HEADER);
                grew_space = 1;
            }
        }

        nbytes = tbx_pack_write(mqs->pack, &(data[dpos]), nleft);
        log_printf(5, "nbytes_packed=%d nleft=%d mqs->len=%d\n", nbytes, nleft, mqs->len);
        if (nbytes > 0) {  //** Stored some data
            nleft -= nbytes;
            dpos += nbytes;
        } else if (nbytes == PACK_ERROR) {
            tbx_pack_consumed(mqs->pack);    //** Rest so garbage data isn't sent
        }

        if ((nleft > 0) && (grew_space == 0)) {  //** Need to flush the data
            if (mqs->mpool == NULL) {  //** Got to configure everything
                apr_pool_create(&mqs->mpool, NULL);
                apr_thread_mutex_create(&(mqs->lock), APR_THREAD_MUTEX_DEFAULT, mqs->mpool);
                apr_thread_cond_create(&(mqs->cond), mqs->mpool);
                apr_thread_mutex_lock(mqs->lock);
                mqs->oo = gop_mq_ongoing_add(mqs->ongoing, 0, mqs->host_id, mqs->hid_len, mqs, mqs_write_on_fail, NULL);

                if (nleft > 0) mqs->data[MQS_STATE_INDEX] = MQS_MORE;
                mqs_write_send(mqs, mqs->address, mqs->fid);
                mqs->expire = 0;  //** The new stream request will set this
                mqs->waiting = 0;
                //apr_thread_mutex_unlock(mqs->lock);
            } else {
                apr_thread_mutex_unlock(mqs->lock);
                err = gop_mq_stream_write_flush(mqs);
                apr_thread_mutex_lock(mqs->lock);
            }
        } else if (mqs->shutdown == 1) {  //** Last write so signal it
            log_printf(5, "Doing final flush sent_data=%d msid=%d\n", mqs->msid, mqs->sent_data);
            err = 0;
            do {
                //** Last set of writes so flush everything
                nbytes = tbx_pack_write_flush(mqs->pack);
                log_printf(5, "msid=%d pack_write_flush=%d\n", mqs->msid, nbytes);
                if ((nbytes == PACK_FINISHED) || (nbytes == PACK_ERROR)) {
                    if (nbytes == PACK_ERROR) tbx_pack_consumed(mqs->pack);  //** Remove garbage data
                    mqs->want_more = MQS_FINISHED;
                    if (mqs->data) mqs->data[MQS_STATE_INDEX] = MQS_FINISHED;
                } else {
                    if (mqs->mpool == NULL) {  //** Got to configure everything
                        apr_pool_create(&mqs->mpool, NULL);
                        apr_thread_mutex_create(&(mqs->lock), APR_THREAD_MUTEX_DEFAULT, mqs->mpool);
                        apr_thread_cond_create(&(mqs->cond), mqs->mpool);
                        apr_thread_mutex_lock(mqs->lock);
                        mqs->oo = gop_mq_ongoing_add(mqs->ongoing, 0, mqs->host_id, mqs->hid_len, mqs, mqs_write_on_fail, NULL);
                    }

                    mqs->want_more = MQS_MORE;
                    if (mqs->data) mqs->data[MQS_STATE_INDEX] = MQS_MORE;
                }

                if (mqs->sent_data == 0) { //** 1st send
                    mqs_write_send(mqs, mqs->address, mqs->fid);
                    mqs->sent_data = 1;
                    tbx_pack_consumed(mqs->pack);
                } else {  //** Got a pending request so just do a flush
                    if (mqs->mpool != NULL) apr_thread_mutex_unlock(mqs->lock);
                    err = gop_mq_stream_write_flush(mqs);
                    if (mqs->mpool != NULL) apr_thread_mutex_lock(mqs->lock);
                    if (err != 0) goto fail;
                }
            } while (nbytes != PACK_FINISHED);
fail:
            log_printf(5, "msid=%d after final flush want_more=%c pack=%d err=%d\n", mqs->msid, mqs->want_more, nbytes, err);

        }
    } while ((nleft > 0) && (err == 0));

    log_printf(5, "END msid=%d bpos=%d\n", mqs->msid, tbx_pack_used(mqs->pack));
    if (mqs->mpool != NULL) {
        apr_thread_mutex_unlock(mqs->lock);
    }

    return(err);
}

//***********************************************************************
// gop_mq_stream_write_varint - Writes a zigzag encoded varint to the stream
//***********************************************************************

int gop_mq_stream_write_varint(gop_mq_stream_t *mqs, int64_t value)
{
    unsigned char buffer[16];
    int i;

    i = tbx_zigzag_encode(value, buffer);

    return(gop_mq_stream_write(mqs, buffer, i));
}


//***********************************************************************
// gop_mq_stream_write_destroy - Destroys a writing MQ stream
//***********************************************************************

void gop_mq_stream_write_destroy(gop_mq_stream_t *mqs)
{
    apr_status_t status;
    int abort_error = 0;

    log_printf(1, "Destroying stream msid=%d\n", mqs->msid);

    //** Change the flag which signals we don't want anything else
    if (mqs->mpool != NULL) {
        apr_thread_mutex_lock(mqs->lock);
    }

    //** Let everyone know we are shuting down
    mqs->shutdown = 1;

    //** This will flush the buffer
    if (mqs->mpool != NULL) {
        apr_thread_mutex_unlock(mqs->lock);
    }
    gop_mq_stream_write(mqs, NULL, 0);
    if (mqs->mpool != NULL) {
        apr_thread_mutex_lock(mqs->lock);
    }

    if (mqs->flusher_thread != NULL) { //** Shut down the flusher
        log_printf(1, "Waiting for flusher to complete msid=%d\n", mqs->msid);

        apr_thread_cond_broadcast(mqs->cond);
        apr_thread_mutex_unlock(mqs->lock);
        apr_thread_join(&status, mqs->flusher_thread);
        log_printf(1, "flusher shut down msid=%d\n", mqs->msid);

        apr_thread_mutex_lock(mqs->lock); //** Re-acuire the lock
    }


    log_printf(2, "msid=%d sent_data=%d abort_error=%d oo=%p mpool=%p transfer_packets=%d\n", mqs->msid, mqs->sent_data, abort_error, mqs->oo, mqs->mpool, mqs->transfer_packets);

    if (mqs->mpool != NULL) {
        apr_thread_mutex_unlock(mqs->lock);    //** Release the lock in case the ongoing cleanup thread is trying to clean up as well
    }

    if (mqs->oo != NULL)  { //** In the ongoing table so remove us
        gop_mq_ongoing_remove(mqs->ongoing, mqs->host_id, mqs->hid_len, mqs->oo->key);
    }

    //** Clean up
    if (mqs->mpool != NULL) {
        apr_thread_mutex_destroy(mqs->lock);
        apr_thread_cond_destroy(mqs->cond);
        apr_pool_destroy(mqs->mpool);
    }

    if (mqs->hid != NULL) gop_mq_frame_destroy(mqs->hid);

    tbx_pack_destroy(mqs->pack);
    if (mqs->unsent_data == 1) free(mqs->data);

    log_printf(5, "END msid=%d\n", mqs->msid);
    free(mqs);

    return;
}

//***********************************************************************
// gop_mq_stream_write_create - Creates an MQ stream for writing
//***********************************************************************

gop_mq_stream_t *gop_mq_stream_write_create(gop_mq_context_t *mqc, gop_mq_portal_t *server_portal, gop_mq_ongoing_t *ongoing, char tbx_pack_type, int max_size, int timeout, mq_msg_t *address, gop_mq_frame_t *fid, gop_mq_frame_t *hid, bool launch_flusher)
{
    gop_mq_stream_t *mqs;
    intptr_t key;
    int ptype;

    tbx_type_malloc_clear(mqs, gop_mq_stream_t, 1);

    mqs->mqc = mqc;
    mqs->type = MQS_WRITE;
    mqs->server_portal = server_portal;
    mqs->ongoing = ongoing;
    mqs->address = address;
    mqs->fid = fid;
    mqs->hid = hid;
    mqs->timeout = timeout;
    mqs->max_size = max_size;
    mqs->want_more = MQS_MORE;
    mqs->expire = apr_time_from_sec(timeout) + apr_time_now();
    mqs->msid = tbx_atomic_global_counter();

    gop_mq_get_frame(hid, (void **)&(mqs->host_id), &(mqs->hid_len));

    //** Make the initial data block which has the header
    mqs->len = 4 * 1024;
    tbx_type_malloc_clear(mqs->data, unsigned char, mqs->len);
    mqs->data[MQS_STATE_INDEX] = MQS_MORE;
    mqs->data[MQS_PACK_INDEX] = tbx_pack_type;
    mqs->data[MQS_HANDLE_SIZE_INDEX] = sizeof(intptr_t);
    key = (intptr_t)mqs;
    memcpy(&(mqs->data[MQS_HANDLE_INDEX]), &key, sizeof(key));
    ptype = (mqs->data[MQS_PACK_INDEX] == MQS_PACK_COMPRESS) ? PACK_COMPRESS : PACK_NONE;
    log_printf(1, "msid=%d ptype=%d tbx_pack_type=%c\n", mqs->msid, ptype, mqs->data[MQS_PACK_INDEX]);
    mqs->pack = tbx_pack_create(ptype, PACK_WRITE, &(mqs->data[MQS_HEADER]), mqs->len-MQS_HEADER);

    log_printf(5, "initial used bpos=%d\n", tbx_pack_used(mqs->pack));

    //** Launch the flusher now if requested
    if (launch_flusher == 1) {
        apr_pool_create(&mqs->mpool, NULL);
        apr_thread_mutex_create(&(mqs->lock), APR_THREAD_MUTEX_DEFAULT, mqs->mpool);
        apr_thread_cond_create(&(mqs->cond), mqs->mpool);
        mqs->oo = gop_mq_ongoing_add(mqs->ongoing, 0, mqs->host_id, mqs->hid_len, mqs, mqs_write_on_fail, NULL);
        mqs->sent_data = 1;
        tbx_thread_create_assert(&(mqs->flusher_thread), NULL, mqs_flusher_thread,  (void *)mqs, mqs->mpool);
    }

    return(mqs);
}



//***********************************************************************
// gop_mq_stream_destroy - Destroys an MQ stream
//***********************************************************************

void gop_mq_stream_destroy(gop_mq_stream_t *mqs)
{
    if (mqs->type == MQS_READ) {
        gop_mq_stream_read_destroy(mqs);
    } else {
        gop_mq_stream_write_destroy(mqs);
    }
}

