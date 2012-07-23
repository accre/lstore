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

typedef struct {
  ibp_attributes_t attr;
  ibp_depot_t depot;
  ibp_connect_context_t cc;
  ns_chksum_t ncs;
  int disk_cs_type;
  int disk_cs_blocksize;
} ds_ibp_attr_t;

data_service_fn_t *ds_ibp_create(char *fname_config);

#ifdef __cplusplus
}
#endif

#endif

