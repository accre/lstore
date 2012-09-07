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
// OS file header file
//***********************************************************************

#include "object_service_abstract.h"

#ifndef _OS_FILE_H_
#define _OS_FILE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_FILE "file"

typedef struct {
  object_service_fn_t *os;
  os_object_iter_t  *oit;
} local_object_iter_t;

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len);
local_object_iter_t *create_local_object_iter(os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth);
void destroy_local_object_iter(local_object_iter_t *it);

object_service_fn_t *object_service_file_create(service_manager_t *authn_sm, service_manager_t *osaz_sm, thread_pool_context_t *tpc, thread_pool_context_t *tpc_unlimited, char *fname, char *section);

#ifdef __cplusplus
}
#endif

#endif

