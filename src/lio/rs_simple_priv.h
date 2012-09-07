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
// Simple resource managment implementation
//***********************************************************************

#include "list.h"
#include "data_service_abstract.h"
#include "opque.h"
#include "service_manager.h"

#ifndef _RS_SIMPLE_PRIV_H_
#define _RS_SIMPLE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char *rid_key;
  char *ds_key;
  list_t *attr;
  int  status;
  int  slot;
} rss_rid_entry_t;

typedef struct {
  list_t *rid_table;
  rss_rid_entry_t **random_array;
  data_service_fn_t *ds;
  int n_rids;
} rs_simple_priv_t;

#ifdef __cplusplus
}
#endif

#endif

