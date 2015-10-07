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
// Blacklist structure definition
//***********************************************************************

#ifndef _BLACKLIST_H_
#define _BLACKLIST_H_

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_hash.h>
#include <apr_time.h>
#include "ex3_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char *rid;
    apr_time_t recheck_time;
} blacklist_rid_t;

typedef struct {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_hash_t *table;
    ex_off_t  min_bandwidth;
    apr_time_t min_io_time;
    apr_time_t timeout;
} blacklist_t;

#ifdef __cplusplus
}
#endif

#endif

