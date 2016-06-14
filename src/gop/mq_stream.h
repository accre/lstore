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
#include "gop/mq_stream.h"
//***********************************************************************
// MQ streaming task management header
//***********************************************************************

#include "gop/gop_visibility.h"
#include "gop.h"
#include "mq_portal.h"
#include "mq_ongoing.h"
#include <tbx/packer.h>

#ifndef _MQ_STREAM_H_
#define _MQ_STREAM_H_

#ifdef __cplusplus
extern "C" {
#endif

//** States
#define MQS_MORE  '0'
#define MQS_ABORT '1'
#define MQS_FINISHED '2'

#define MQS_READ  0
#define MQS_WRITE 1

//*** Header format [state][tbx_pack_type][handle_len][..handle..]
#define MQS_HEADER (1+1+1+sizeof(intptr_t))

//** Header indices
#define MQS_STATE_INDEX       0
#define MQS_PACK_INDEX        1
#define MQS_HANDLE_SIZE_INDEX 2
#define MQS_HANDLE_INDEX      3

struct mq_stream_t {
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
};


int gop_mq_stream_read_string(mq_stream_t *mqs, char *str, int bufsize);

int gop_mq_stream_write_string(mq_stream_t *mqs, char *str);

void mq_stream_release_frame(mq_stream_t *mqs);


#ifdef __cplusplus
}
#endif

#endif

