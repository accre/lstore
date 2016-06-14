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
#include "gop/mq_helpers.h"
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

mq_frame_t *mq_make_id_frame();
int mq_num_frames(mq_msg_t *msg);
char *mq_address_to_string(mq_msg_t *address);


#ifdef __cplusplus
}
#endif

#endif

