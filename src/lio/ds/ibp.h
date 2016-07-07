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

#include <ibp/ibp.h>
#include <tbx/iniparse.h>
#include <tbx/network.h>

#include "ds.h"

#ifdef __cplusplus
extern "C" {
#endif

struct lio_ds_ibp_attr_t;

#define DS_TYPE_IBP "ibp"

//** Additional IBP attributes
#define DS_IBP_ATTR_RELIABILITY 2
#define DS_IBP_ATTR_TYPE        3
#define DS_IBP_ATTR_DEPOT       4
#define DS_IBP_ATTR_CC          5
#define DS_IBP_ATTR_NET_CKSUM    6
#define DS_IBP_ATTR_DISK_CHKSUM_TYPE 7
#define DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE 8

lio_data_service_fn_t *ds_ibp_create(void *arg, tbx_inip_file_t *ifd, char *section);

struct lio_ds_ibp_alloc_op_t {
    ibp_depot_t depot;
};

struct lio_ds_ibp_truncate_op_t {
    int state;
    int timeout;
    ibp_off_t new_size;
    ibp_capstatus_t probe;
    gop_callback_t *cb;
    lio_data_service_fn_t *dsf;
    lio_ds_ibp_attr_t *attr;
    gop_opque_t *q;
    ibp_cap_t *mcap;
};

struct lio_ds_ibp_op_t {
    void *sf_ptr;
    lio_ds_ibp_attr_t *attr;
    gop_op_generic_t *gop;
    void (*free)(gop_op_generic_t *d, int mode);
    void *free_ptr;
    union {
        lio_ds_ibp_alloc_op_t alloc;
        lio_ds_ibp_truncate_op_t truncate;
    } ops;
};


#ifdef __cplusplus
}
#endif

#endif
