/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (July 2012)

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

#include "list.h"
#include "resource_service_abstract.h"

#ifndef _RS_ZMQ_H_
#define _RS_ZMQ_H_

#ifdef __cplusplus 
extern "C" {
#endif

#define RS_TYPE_ZMQ "rs_zmq"
#define RS_ZMQ_DFT_PROTO "tcp"
#define RS_ZMQ_DFT_PORT "5555"

resource_service_fn_t *rs_zmq_create(void *arg, char *fname, char *section);
resource_service_fn_t *rs_zmq_create_driver(void *arg); //** Don't know how to use this function

#ifdef __cplusplus
}
#endif

#endif
