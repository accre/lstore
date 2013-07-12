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

//***********************************************************************
// Provides MQ helper routines
//***********************************************************************

#include "mq_portal.h"

#ifndef _MQ_HELPERS_H_
#define _MQ_HELPERS_H_

#ifdef __cplusplus
extern "C" {
#endif

void mq_remove_header(mq_msg_t *msg, int drop_extra);
op_status_t mq_read_status_frame(mq_frame_t *f, int destroy);
mq_frame_t *mq_make_status_frame(op_status_t status);
mq_frame_t *mq_make_id_frame();
mq_msg_t *mq_make_exec_core_msg(char *address, int do_track);
mq_msg_t *mq_make_response_core_msg(mq_msg_t *address, mq_frame_t *fid);


#ifdef __cplusplus
}
#endif

#endif

