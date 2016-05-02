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

