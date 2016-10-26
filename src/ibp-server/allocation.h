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

#ifndef _IBP_ALLOCATION_H_
#define _IBP_ALLOCATION_H_

#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include "ibp_protocol.h"
#include "osd_abstract.h"
#include "ibp_protocol.h"

#define CAP_SIZE 32
#define CAP_BITS CAP_SIZE*6

#define READ_CAP   0
#define WRITE_CAP  1
#define MANAGE_CAP 2

#define ALLOC_HEADER 4096

#define ALLOC_HARD 0
#define ALLOC_SOFT 1

#define ALLOC_HISTORY 16

typedef int64_t ibp_off_t;

typedef struct {
    char v[CAP_SIZE+1];
} Cap_t;

typedef struct {  //** Internal address
  uint32_t atype;
  char     ip[16];
} Allocation_address_t;

typedef struct {
   union {
     uint64_t id[4];
     char     cid[32];
   };
   Allocation_address_t ca_host;
} Allocation_cert_t;

typedef struct {
   uint32_t time;
   Allocation_address_t host;
} Allocation_timestamp_t;

typedef struct {  //** R/W ops
  uint64_t offset;
  uint64_t size;
  osd_id_t id;   
  Allocation_timestamp_t ts;
} Allocation_rw_ts_t;

typedef struct {  //** Manage ops
  short int cmd;
  short int subcmd;
  uint32_t expiration;    
  uint64_t reliability;   //** This duals as the offset for IBP_ALIAS_ALLOCATE/MANAGE calls
  uint64_t size;          //** This duals as the alias size for IBP_ALIAS_ALLOCATE/MANAGE calls
  osd_id_t id;    //** This is the alias ID for a IBP_ALIAS_ALLOCATE call
  Allocation_timestamp_t ts;
} Allocation_manage_ts_t;

typedef struct {  //** Allocation Timestamp information
  osd_id_t id;    //** This is the alias ID for a IBP_ALIAS_ALLOCATE call
  Allocation_manage_ts_t manage_ts[ALLOC_HISTORY];
  Allocation_rw_ts_t read_ts[ALLOC_HISTORY];
  Allocation_rw_ts_t write_ts[ALLOC_HISTORY];
  int32_t manage_slot;
  int32_t read_slot;
  int32_t write_slot;
} Allocation_history_t;


typedef struct {   //** Timekey for expiration indices
  osd_id_t id;
  uint32_t time;
} DB_timekey_t;

typedef struct {    // IBP Allocation
  osd_id_t id;
  osd_id_t split_parent_id;   //** Non-zero if this allocation was split off an existing alloc
  uint64_t size;
  uint64_t max_size;
  uint64_t r_pos;
  uint64_t w_pos;
  uint32_t is_alias;
  uint32_t expiration;
  int32_t  type; /* BYTE_ARRAY, QUEUE, etc */
  int32_t  reliability; /* SOFT/HARD */
  int32_t  read_refcount;
  int32_t  write_refcount;
  uint64_t alias_offset;
  uint64_t alias_size;
  osd_id_t alias_id;
  Cap_t    caps[3];
  DB_timekey_t expirekey;
  DB_timekey_t softkey;
  Allocation_cert_t creation_cert;
  Allocation_timestamp_t creation_ts;
} Allocation_t;

#endif

