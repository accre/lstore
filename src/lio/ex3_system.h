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
// Linear exnode3 support
//***********************************************************************


#include "list.h"
#include "ex3_abstract.h"
#include "cache.h"

#ifndef _EX3_SYSTEM_H_
#define _EX3_SYSTEM_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  resource_service_fn_t *rs;
  data_service_fn_t *ds;
  object_service_fn_t *os;
  thread_pool_context_t *tpc_unlimited;
  thread_pool_context_t *tpc_cpu;
  cache_t *cache;
} exnode_abstract_set_t;

extern exnode_abstract_set_t *exnode_service_set;

//** ex3_global functions
int ex3_set_default_ds(data_service_fn_t *ds);
data_service_fn_t *ex3_get_default_ds();
int ex3_set_default_rs(resource_service_fn_t *rs);
resource_service_fn_t *ex3_get_default_rs();
int ex3_set_default_os(object_service_fn_t *os);
object_service_fn_t *ex3_get_default_os();

int exnode_system_init(data_service_fn_t *ds, resource_service_fn_t *rs, object_service_fn_t *os, thread_pool_context_t *tpc_unlimited, thread_pool_context_t *tpc_cpu, cache_t *c);
void exnode_system_destroy();

#ifdef __cplusplus
}
#endif

#endif

