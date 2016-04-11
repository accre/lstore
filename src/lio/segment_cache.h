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
// Generic cache segment support
//***********************************************************************

#include "cache.h"

#ifndef _SEGMENT_CACHE_H_
#define _SEGMENT_CACHE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_CACHE "cache"

int cache_page_drop(segment_t *seg, ex_off_t lo, ex_off_t hi);
int cache_stats_print(cache_stats_t *cs, char *buffer, int *used, int nmax);
int cache_stats(cache_t *c, cache_stats_t *cs);
cache_stats_t segment_cache_stats(segment_t *seg);
segment_t *segment_cache_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_cache_create(void *arg);

#ifdef __cplusplus
}
#endif

#endif

