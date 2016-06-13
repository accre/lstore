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

#include "data_service_abstract.h"
#include "ibp.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DS_TYPE_IBP "ibp"

//** Additional IBP attributes
#define DS_IBP_ATTR_RELIABILITY 2
#define DS_IBP_ATTR_TYPE        3
#define DS_IBP_ATTR_DEPOT       4
#define DS_IBP_ATTR_CC          5
#define DS_IBP_ATTR_NET_CKSUM    6
#define DS_IBP_ATTR_DISK_CHKSUM_TYPE 7
#define DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE 8

typedef struct ds_ibp_attr_t ds_ibp_attr_t;
struct ds_ibp_attr_t {
    ibp_attributes_t attr;
    ibp_depot_t depot;
    ibp_connect_context_t cc;
    tbx_ns_chksum_t ncs;
    int disk_cs_type;
    int disk_cs_blocksize;
};

data_service_fn_t *ds_ibp_create(void *arg, tbx_inip_file_t *ifd, char *section);

#ifdef __cplusplus
}
#endif

#endif

