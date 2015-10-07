/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2012)

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

//***********************************************************************
// ZMQ resource managment implementation
//***********************************************************************
#ifndef _RS_ZMQ_PRIV_H_
#define _RS_ZMQ_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "data_service_abstract.h"
#include "thread_pool.h"

typedef struct {
    void *zmq_context;
    void *zmq_socket;
    char *zmq_svr; //** Format: protocol://host:port
    data_service_fn_t *ds; //** Used for getting the data attribute
    thread_pool_context_t *tpc; //** Contains a new thread pool context for rs zmq
} rs_zmq_priv_t;

#ifdef __cplusplus
}
#endif

#endif

