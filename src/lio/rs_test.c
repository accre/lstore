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

#define _log_module_index 182

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, start_option, err;
  int timeout, ll, mode, n_alloc = 0;
  ibp_context_t *ic;
  inip_file_t *ifd;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache = NULL;
  data_attr_t *da;
  char *cfg_name = NULL;
  char *ctype;
  char *qstr = NULL;
  rs_query_t *rsq;
  op_generic_t *gop;
  opque_t *q;
  op_status_t status;
  data_cap_set_t **cap_list;
  rs_request_t *req_list;


//printf("argc=%d\n", argc);
  if (argc < 3) {
     printf("\n");
     printf("rs_test [-d log_level] [-c system.cfg] -q query_string -n n_alloc\n");
     printf("    -c system.cfg - IBP and Cache configuration options\n");
     printf("    -q query_string - Resource service Query string\n");
     printf("    -n n_alloc      - Number of allocations to request\n");
     return(1);
  }

  tpc_unlimited = thread_pool_create_context("UNLIMITED", 0, 2000);
  tpc_cpu = thread_pool_create_context("CPU", 0, 0);
  rs = NULL;
  ic = ibp_create_context();  //** Initialize IBP
  ds = ds_ibp_create(ic);
  da = ds_attr_create(ds);
  cache_system_init();
  timeout = 120;
  ll = -1;
  mode = CLONE_STRUCTURE;

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        ll = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-c") == 0) { //** Load the config file
        i++;
        cfg_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-n") == 0) { //** Allocation count
        i++;
        n_alloc = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-q") == 0) { //** Query string
        i++;
        qstr = argv[i]; i++;
     }

  } while ((start_option < i) && (i<argc));

  if (cfg_name != NULL) {
     ibp_load_config(ic, cfg_name);

     ifd = inip_read(cfg_name);
     ctype = inip_get_string(ifd, "cache", "type", CACHE_LRU_TYPE);
     inip_destroy(ifd);
     cache = load_cache(ctype, da, timeout, cfg_name);
     free(ctype);
     mlog_load(cfg_name);
     if (rs == NULL) rs = rs_simple_create(cfg_name, ds);
  } else {
     cache = create_cache(CACHE_LRU_TYPE, da, timeout);
  }

  if (ll > -1) set_log_level(ll);

  exnode_system_init(ds, rs, NULL, tpc_unlimited, tpc_cpu, cache);


  //** Parse the query
  rsq = rs_query_parse(rs, qstr);

  //** Generate the data request
  type_malloc_clear(req_list, rs_request_t, n_alloc);
  type_malloc_clear(cap_list, data_cap_set_t *, n_alloc);

  for (i=0; i<n_alloc; i++) {
    cap_list[i] = ds_cap_set_create(ds);
    req_list[i].rid_index = i;
    req_list[i].size = 1000;  //** Don't really care how big it is for testing
    req_list[i].rid_key = NULL;  //** This will let me know if I got success as well as checking the cap
  }

  gop = rs_data_request(rs, da, rsq, cap_list, req_list, n_alloc, NULL, 0, n_alloc, timeout);


  //** Wait for it to complete
  gop_waitall(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);

  if (status.op_status != OP_STATE_SUCCESS) {
     printf("Error with data request! err_code=%d\n", status.error_code);
     abort();
  }


  //** Print the caps
  printf("Query: %s  n_alloc: %d\n", qstr, n_alloc);
  printf("\n");
  for (i=0; i<n_alloc; i++) {
     printf("%d.\tRID key: %s\n", i, req_list[i].rid_key);
     printf("\tRead  : %s\n", (char *)ds_get_cap(ds, cap_list[i], DS_CAP_READ));
     printf("\tWrite : %s\n", (char *)ds_get_cap(ds, cap_list[i], DS_CAP_WRITE));
     printf("\tManage: %s\n", (char *)ds_get_cap(ds, cap_list[i], DS_CAP_MANAGE));
  }
  printf("\n");

  //** Now destroy the allocation I just created
  q = new_opque();
  for (i=0; i<n_alloc; i++) {
    gop = ds_remove(ds, da, ds_get_cap(ds, cap_list[i], DS_CAP_MANAGE), timeout);
    opque_add(q, gop);
  }

  //** Wait for it to complete
  err = opque_waitall(q);
  opque_free(q, OP_DESTROY);

  if (err != OP_STATE_SUCCESS) {
     printf("Error removing allocations!\n");
  }

  //** Destroy the caps
  for (i=0; i<n_alloc; i++) {
     ds_cap_set_destroy(ds, cap_list[i], 1);
  }
  free(cap_list);
  free(req_list);

  //** Clean up
  rs_query_destroy(rs, rsq);

  exnode_system_destroy();
  cache_destroy(cache);
  cache_system_destroy();

  ds_attr_destroy(ds, da);
  ds_destroy_service(ds);
  ibp_destroy_context(ic);
  thread_pool_destroy_context(tpc_unlimited);
  thread_pool_destroy_context(tpc_cpu);

  return(0);
}


