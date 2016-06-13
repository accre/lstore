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

#include "gop/gop_visibility.h"
#include "mq_portal.h"

#ifndef _MQ_HELPERS_H_
#define _MQ_HELPERS_H_

#ifdef __cplusplus
extern "C" {
#endif

GOP_API void gop_mq_remove_header(mq_msg_t *msg, int drop_extra);
GOP_API op_status_t gop_mq_read_status_frame(mq_frame_t *f, int destroy);
GOP_API mq_frame_t *gop_mq_make_status_frame(op_status_t status);
mq_frame_t *mq_make_id_frame();
GOP_API mq_msg_t *gop_mq_make_exec_core_msg(mq_msg_t *address, int do_track);
GOP_API mq_msg_t *gop_mq_make_response_core_msg(mq_msg_t *address, mq_frame_t *fid);
int mq_num_frames(mq_msg_t *msg);
char *mq_address_to_string(mq_msg_t *address);
GOP_API mq_msg_t *gop_mq_string_to_address(char *string);


#ifdef __cplusplus
}
#endif

#endif

