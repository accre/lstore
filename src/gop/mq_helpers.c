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
// Provides MQ helper routines
//***********************************************************************

#define _log_module_index 213
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#include "gop/gop.h"
#include "gop/types.h"
#include "mq_helpers.h"
#include "mq_portal.h"

static tbx_atomic_unit32_t _id_counter = 0;

//***********************************************************************
// mq_make_id_frame - Makes and generates and ID frame
//***********************************************************************

gop_mq_frame_t *mq_make_id_frame()
{
    tbx_atomic_unit32_t *id;

    tbx_type_malloc(id, tbx_atomic_unit32_t, 1);

    *id = tbx_atomic_inc(_id_counter);

    if (*id > 1000000000) {
        tbx_atomic_set(_id_counter, 0);
    }

    return(gop_mq_frame_new(id, sizeof(tbx_atomic_unit32_t), MQF_MSG_AUTO_FREE));
}


//***********************************************************************
// gop_mq_read_status_frame - Processes a status frame
//***********************************************************************

gop_op_status_t gop_mq_read_status_frame(gop_mq_frame_t *f, int destroy)
{
    char *data;
    int nbytes, n;
    int64_t value;
    gop_op_status_t status;

    gop_mq_get_frame(f, (void **)&data, &nbytes);

    n = tbx_zigzag_decode((unsigned char *)data, nbytes, &value);
    status.op_status = value;
    tbx_zigzag_decode((unsigned char *)&(data[n]), nbytes-n, &value);
    status.error_code = value;

    if (destroy == 1) gop_mq_frame_destroy(f);

    return(status);
}

//***********************************************************************
// gop_mq_make_status_frame -Creates a status frame
//***********************************************************************

gop_mq_frame_t *gop_mq_make_status_frame(gop_op_status_t status)
{
    unsigned char buffer[128];
    unsigned char *bytes;
    int n;

    n = tbx_zigzag_encode(status.op_status, buffer);
    n = n + tbx_zigzag_encode(status.error_code, &(buffer[n]));
    tbx_type_malloc(bytes, unsigned char, n);
    memcpy(bytes, buffer, n);
    return(gop_mq_frame_new(bytes, n, MQF_MSG_AUTO_FREE));
}

//***********************************************************************
// gop_mq_remove_header - Removes the header from the message.
//***********************************************************************

void gop_mq_remove_header(mq_msg_t *msg, int drop_extra)
{
    int i;

    gop_mq_msg_first(msg);                  //** Move to the 1st frame
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the NULL frame
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the version frame
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the MQ command frame

    for (i=0; i<drop_extra; i++) {
        gop_mq_frame_destroy(mq_msg_pop(msg));
    }
}

//***********************************************************************
// gop_mq_make_exec_core_msg - Makes the EXEC/TRACKEXEC message core
//***********************************************************************

mq_msg_t *gop_mq_make_exec_core_msg(mq_msg_t *address, int do_track)
{
    mq_msg_t *msg;

    msg = gop_mq_msg_new();

    gop_mq_msg_append_msg(msg, address, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    if (do_track) {
        gop_mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_frame(msg, mq_make_id_frame());

    return(msg);
}


//***********************************************************************
// gop_mq_make_response_core_msg - Makes the RESPONSE message core
//***********************************************************************

mq_msg_t *gop_mq_make_response_core_msg(mq_msg_t *address, gop_mq_frame_t *fid)
{
    mq_msg_t *response;

    response = gop_mq_msg_new();
    gop_mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(response, fid);

    //** Now address it
    gop_mq_msg_apply_return_address(response, address, 0);

    return(response);
}

//***********************************************************************
// mq_num_frames - Returns the number of frames in the message
//***********************************************************************

int mq_num_frames(mq_msg_t *msg)
{
    gop_mq_frame_t *f;
    int n;

    for(f = gop_mq_msg_first(msg), n = 0; f != NULL; f = gop_mq_msg_next(msg), n++);

    return n;
}

//***********************************************************************
// mq_address_to_string - Converts a message to a comma-separated string
//***********************************************************************

char *mq_address_to_string(mq_msg_t *address)
{
    gop_mq_frame_t *f;
    int msg_size, frames, n, size;
    char *string, *data;

    if (address == NULL) return(NULL);
    msg_size = gop_mq_msg_total_size(address); // sum of frame data lengths
    frames = mq_num_frames(address);
    n = 0;
    size = 0;

    string = malloc(msg_size + frames);

    for (f = gop_mq_msg_first(address); f != NULL; f = gop_mq_msg_next(address)) {
        gop_mq_get_frame(f, (void **)&data, &size);
        memcpy(string + n, data, size);
        n += size;
        if(size == 0) break;
        *(string + (n++)) = ',';
    }
    *(string + (--n)) = 0; // remove the trailing comma and make this the end of the string

    // For testing:
    log_printf(0, "DEBUG: string created = %s, malloc size = %d, actual size = %lu\n", string, (msg_size+frames+10), strlen(string));

    return(string);
}

//***********************************************************************
// gop_mq_string_to_address - Converts a comma-separated string to a message
//  ***NOTE: The input string is MODIFIED!!!!!!*****
//***********************************************************************

mq_msg_t *gop_mq_string_to_address(char *string)
{
    int fin;
    char *token;
    mq_msg_t *address;
    char *bstate;

    if (string == NULL) return(NULL);

    address = gop_mq_msg_new();
    token = tbx_stk_string_token(string, ",", &bstate, &fin);
    while(fin == 0) {
        log_printf(5, "host frame=%s\n", token);
        gop_mq_msg_append_mem(address, token, strlen(token), MQF_MSG_KEEP_DATA);
        token = tbx_stk_string_token(NULL, ",", &bstate, &fin);
    }

    return address;
}
