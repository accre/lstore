/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012)

Copyright (c) 2012, Advanced Computing Center for Research and Education,
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

//*************************************************************************
//*************************************************************************

#ifndef _RR_BASE_H_
#define _RR_BASE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <zmq.h>
#include <czmq.h>
#include <uuid/uuid.h>

#include "iniparse.h"
#include "log.h"
#include "type_malloc.h"

#define RETRIES_DFT 3
#define TIMEOUT_DFT -1 

//** Communication mode 
#define SYNC_MODE 0
#define ASYNC_MODE 1

#define HEARTBEAT_LIVENESS 3
#define HEARTBEAT_DFT 2500
#define RECONNECT_DFT 2500
#define HEARTBEAT_EXPIRY HEARTBEAT_LIVENESS * HEARTBEAT_DFT

//** RRWRK commands, as strings
#define RRWRK_READY          "\001"
#define RRWRK_REQUEST        "\002"
#define RRWRK_REPLY          "\003"
#define RRWRK_HEARTBEAT      "\004"
#define RRWRK_DISCONNECT     "\005"
#define RRWRK_OUTPUT	     "\006"

//** RRCLI commands, as strings
#define RRCLI_FINISHED 		"\001"
#define RRCLI_ACK		"\002"
#define RRCLI_REQUEST		"\003"

//**  This is the version of RR/Client we implement
#define RR_CLIENT         "RRC01"

//**  This is the version of RR/Worker we implement
#define RR_WORKER         "RRW01"

//** Task Data 
typedef struct {
    size_t len;
    void *data;
}rrtask_data_t;

//** Task related functions
rrtask_data_t *rrtask_data_new();
void rrtask_data_destroy(rrtask_data_t **self_p);
void rrtask_set_data(rrtask_data_t *self, void *buf, size_t len);
void *rrtask_get_data(rrtask_data_t *self);
size_t rrtask_get_len(rrtask_data_t *self);


void rr_set_mode_tm(inip_file_t *keyfile, char *section, int *mode, int *timeout);
void *rr_create_socket(zctx_t *ctx, int mode, int sync_st, int asyn_st);
void rr_dump(void *buf, int len);
char *rr_uuid_str(uuid_t uuid);


#ifdef __cplusplus
}
#endif

#endif
