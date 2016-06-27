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

//***********************************************************************
// IBP based data service
//***********************************************************************

#ifndef _DS_IBP_H_
#define _DS_IBP_H_

#include <lio/ds_ibp.h>
#include <ibp/ibp.h>
#include <tbx/iniparse.h>
#include <tbx/network.h>

#include "ds.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ds_ibp_attr_t;

#define DS_TYPE_IBP "ibp"

//** Additional IBP attributes
#define DS_IBP_ATTR_RELIABILITY 2
#define DS_IBP_ATTR_TYPE        3
#define DS_IBP_ATTR_DEPOT       4
#define DS_IBP_ATTR_CC          5
#define DS_IBP_ATTR_NET_CKSUM    6
#define DS_IBP_ATTR_DISK_CHKSUM_TYPE 7
#define DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE 8

data_service_fn_t *ds_ibp_create(void *arg, tbx_inip_file_t *ifd, char *section);

struct ds_ibp_alloc_op_t {
    ibp_depot_t depot;
};

struct ds_ibp_truncate_op_t {
    int state;
    int timeout;
    ibp_off_t new_size;
    ibp_capstatus_t probe;
    callback_t *cb;
    data_service_fn_t *dsf;
    ds_ibp_attr_t *attr;
    opque_t *q;
    ibp_cap_t *mcap;
};

struct ds_ibp_op_t {
    void *sf_ptr;
    ds_ibp_attr_t *attr;
    op_generic_t *gop;
    void (*free)(op_generic_t *d, int mode);
    void *free_ptr;
    union {
        ds_ibp_alloc_op_t alloc;
        ds_ibp_truncate_op_t truncate;
    } ops;
};


#ifdef __cplusplus
}
#endif

#endif
