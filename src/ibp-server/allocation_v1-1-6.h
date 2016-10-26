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

//******************************************************************
//******************************************************************

#ifndef _IBP_ALLOCATION_V116_H_
#define _IBP_ALLOCATION_V116_H_

#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <allocation.h>

//#define CAP_SIZE 32
//#define CAP_BITS CAP_SIZE*6

//#define READ_CAP   0
//#define WRITE_CAP  1
//#define MANAGE_CAP 2

//#define ALLOC_HEADER 4096

//#define ALLOC_HARD 0
//#define ALLOC_SOFT 1

//typedef struct {
//    char v[CAP_SIZE+1];
//} Cap_t;

typedef struct {    // IBP Allocation
  uint32_t   expiration;
  osd_id_t id;
  uint64_t size;
  uint64_t max_size;
  uint64_t r_pos;
  uint64_t w_pos;
  int32_t  type; /* BYTE_ARRAY, QUEUE, etc */
  int32_t  reliability; /* SOFT/HARD */
  int32_t  read_refcount;
  int32_t  write_refcount;
  Cap_t    caps[3];
} Allocation_v116_t;

#endif

