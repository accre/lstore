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

