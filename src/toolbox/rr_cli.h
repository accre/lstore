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

#ifndef _RR_CLI_H_
#define _RR_CLI_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "rr_base.h"

//** Request and Response client class
typedef struct {
    zctx_t *ctx;     //** CZMQ context
    zmsg_t *single;
    void *socket;    //** Client socket
    char *pattern;   //** ZMQ pattern
    char *server;    //** Used by pirate pattern
    char *broker;    //** Used by majordomo pattern
    char *identity;  //** Client's identity
    int timeout;     //** Request timeout
    int mode;        //** Either SYNC_MODE or ASYNC_MODE
    int retries;     //** Used by SYNC_MODE
} rrcli_t;

rrcli_t *rrcli_new();
void rrcli_destroy(rrcli_t **self_p);

void rrcli_load_config(rrcli_t *self, char *fname);
int rrcli_send(rrcli_t *self, void *buf, size_t len);
int rrcli_recv(rrcli_t *self, void *buf, size_t len);

#ifdef __cplusplus
}
#endif

#endif
