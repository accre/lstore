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

#ifndef __ATOMIC_COUNTER_H_
#define __ATOMIC_COUNTER_H_

#include "apr_atomic.h"

typedef apr_uint32_t atomic_int_t;

#define atomic_inc(v) apr_atomic_inc32(&(v))
#define atomic_dec(v) apr_atomic_dec32(&(v))
#define atomic_set(v, n) apr_atomic_set32(&(v), n)
#define atomic_get(v) apr_atomic_read32(&(v))
#define atomic_exchange(a, v) apr_atomic_xchg32(&a, v)

int atomic_global_counter();
int atomic_counter(atomic_int_t *counter);

extern int *_a_thread_id_ptr();
#define atomic_thread_id (*_a_thread_id_ptr())

void atomic_init();
void atomic_destroy();

#endif


