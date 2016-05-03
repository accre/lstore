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
// MQ streaming task management header
//***********************************************************************

#include "gop/gop_visibility.h"
#include "opque.h"
#include "mq_portal.h"
#include "mq_ongoing.h"
#include "packer.h"

#ifndef _MQ_STREAM_H_
#define _MQ_STREAM_H_

#ifdef __cplusplus
extern "C" {
#endif

#define MQS_MORE_DATA_KEY  "mqs_more"
#define MQS_MORE_DATA_SIZE 8

//** States
#define MQS_MORE  '0'
#define MQS_ABORT '1'
#define MQS_FINISHED '2'

//** Pack type
#define MQS_PACK_RAW 'R'
#define MQS_PACK_COMPRESS 'Z'

#define MQS_READ  0
#define MQS_WRITE 1

//*** Header format [state][tbx_pack_type][handle_len][..handle..]
#define MQS_HEADER (1+1+1+sizeof(intptr_t))

//** Header indices
#define MQS_STATE_INDEX       0
#define MQS_PACK_INDEX        1
#define MQS_HANDLE_SIZE_INDEX 2
#define MQS_HANDLE_INDEX      3

typedef struct {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    mq_context_t *mqc;
    mq_portal_t *server_portal;
    mq_frame_t *fid;
    mq_frame_t *hid;
    op_generic_t *gop_waiting;
    op_generic_t *gop_processed;
    mq_ongoing_t *ongoing;
    mq_ongoing_object_t *oo;
    char want_more;
    mq_msg_t *remote_host;
    char *host_id;
    char *stream_id;
    int sid_len;
    int hid_len;
    mq_msg_t *address;
    unsigned char *data;
    apr_thread_t *flusher_thread;
    apr_time_t expire;
    tbx_pack_t *pack;
    int len;
    int bpos;
    int waiting;
    int processed;
    int ready;
    int type;
    int timeout;
    int max_size;
    int sent_data;
    int unsent_data;
    int shutdown;
    int transfer_packets;  //** Number of packets exchanged
    int msid;              //** Stream ID
    int dead_connection;   //** Connections is hosed so don;t even try sending anything
} mq_stream_t;

GOP_API void mqs_server_more_cb(void *arg, mq_task_t *task);

GOP_API int64_t mq_stream_read_varint(mq_stream_t *mqs, int *error);
int mq_stream_read_string(mq_stream_t *mqs, char *str, int bufsize);
GOP_API int mq_stream_read(mq_stream_t *mqs, void *buffer, int nbytes);

GOP_API int mq_stream_write_varint(mq_stream_t *mqs, int64_t value);
int mq_stream_write_string(mq_stream_t *mqs, char *str);
GOP_API int mq_stream_write(mq_stream_t *mqs, void *buffer, int nbytes);

void mq_stream_release_frame(mq_stream_t *mqs);
GOP_API mq_stream_t *mq_stream_read_create(mq_context_t *mqc,  mq_ongoing_t *ongoing, char *host_id, int hid_len, mq_frame_t *fdata, mq_msg_t *remote_host, int to);
GOP_API mq_stream_t *mq_stream_write_create(mq_context_t *mqc, mq_portal_t *server_portal, mq_ongoing_t *ongoing, char tbx_pack_type, int max_size, int timeout, mq_msg_t *address, mq_frame_t *fid, mq_frame_t *hid, int launch_flusher);

GOP_API void mq_stream_destroy(mq_stream_t *mqs);

#ifdef __cplusplus
}
#endif

#endif

