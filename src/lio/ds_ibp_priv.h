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

#include "ibp.h"
#include "ds_ibp.h"

#ifndef _DS_IBP_PRIV_H_
#define _DS_IBP_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  ibp_depot_t depot;
} ds_ibp_alloc_op_t;

typedef struct {
  int state;
  int timeout;
  ibp_off_t new_size;
  ibp_capstatus_t probe;
  callback_t *cb;
  data_service_fn_t *dsf;
  ds_ibp_attr_t *attr;
  opque_t *q;
  ibp_cap_t *mcap;
} ds_ibp_truncate_op_t;

typedef struct {
  void *sf_ptr;
  ds_ibp_attr_t *attr;
  op_generic_t *gop;
  void (*free)(op_generic_t *d, int mode);
  void *free_ptr;
  union {
    ds_ibp_alloc_op_t alloc;
    ds_ibp_truncate_op_t truncate;
  };
}  ds_ibp_op_t;

typedef struct {
  ds_ibp_attr_t attr_default;
  ibp_context_t *ic;
} ds_ibp_priv_t;


#ifdef __cplusplus
}
#endif

#endif

