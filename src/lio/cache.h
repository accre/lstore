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

//*************************************************************************
//*************************************************************************

#ifndef __CACHE_H_
#define __CACHE_H_

#include "cache_priv.h"
#include "cache_lru.h"
#include "cache_amp.h"
#include "cache_round_robin.h"

#define CACHE_LOAD_AVAILABLE "cache_load_available"
#define CACHE_CREATE_AVAILABLE "cache_create_available"

void print_cache_table(int dolock);
typedef cache_t *(cache_load_t)(void *arg, inip_file_t *ifd, char *section, data_attr_t *da, int timeout);
typedef cache_t *(cache_create_t)(void *arg, data_attr_t *da, int timeout);

//#define CACHE_PRINT_LOCK  log_printf(0, "CACHE_PRINT START\n"); print_cache_table(1); log_printf(0, "CACHE_PRINT END\n")
//#define CACHE_PRINT       log_printf(0, "CACHE_PRINT START\n"); print_cache_table(0); log_printf(0, "CACHE_PRINT END\n")

#define CACHE_PRINT
#define CACHE_PRINT_LOCK


#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif


