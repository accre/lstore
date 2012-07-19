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

#define _log_module_index 169

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
  int i, start_option;
  int timeout, ll, mode;
  ibp_context_t *ic;
  inip_file_t *ifd;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache = NULL;
  data_attr_t *da;
  char *clone_arg = NULL;
  char *sfname = NULL;
  char *cfname = NULL;
  char *cfg_name = NULL;
  char *ctype;
  exnode_t *ex, *cex;
  exnode_exchange_t *exp, *exp_out;
  op_generic_t *gop;
  op_status_t status;
  FILE *fd;

//printf("argc=%d\n", argc);
  if (argc < 3) {
     printf("\n");
     printf("ex_clone [-d log_level] [-c system.cfg] [-structure|-data] [-a clone_attr] source_file.ex3 clone_file.ex3\n");
     printf("    -c system.cfg - IBP and Cache configuration options\n");
     printf("    -structure      - Clone the structure only [default mode]\n");
     printf("    -data           - Clone the structure and copy the data\n");
     printf("    -a clone_attr   - Segment specific attribute passed to the clone routine. Not used for all Segment types.\n");
     printf("    source_file.ex3 - File to clone\n");
     printf("    clone_file.ex3  - DEstination cloned file\n");
     return(1);
  }

//set_log_level(20);
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
     } else if (strcmp(argv[i], "-rs") == 0) { //** Load the resource file
        i++;
        rs = rs_simple_create(argv[i], ds); i++;
     } else if (strcmp(argv[i], "-c") == 0) { //** Load the config file
        i++;
        cfg_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-structure") == 0) { //** Clone the structure only
        i++;
        mode = CLONE_STRUCTURE;
     } else if (strcmp(argv[i], "-data") == 0) { //** Clone the structure and the data
        i++;
        mode = CLONE_STRUCT_AND_DATA;
     } else if (strcmp(argv[i], "-a") == 0) { //** Alternate query attribute
        i++;
        clone_arg = argv[i]; i++;
     }

  } while (start_option < i);

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

  //** This is the source file
  sfname = argv[i]; i++;
  if (sfname == NULL) {
    printf("Missing source file!\n");
    return(2);
  }

  //** This is the cloned file
  cfname = argv[i]; i++;
  if (cfname == NULL) {
    printf("Missing cloned file!\n");
    return(2);
  }

  //** Load the source
  exp = exnode_exchange_load_file(sfname);
  ex = exnode_create();
  exnode_deserialize(ex, exp);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


  //** Execute the clone operation
  gop = exnode_clone(ex, da, &cex, (void *)clone_arg, mode, timeout);

  gop_waitany(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);

  if (status.op_status != OP_STATE_SUCCESS) {
     printf("Error with clone! source=%s mode=%d\n", sfname, mode);
     abort();
  }

  //** Store the updated exnode back to disk
  exp_out = exnode_exchange_create(EX_TEXT);
  exnode_serialize(cex, exp_out);
//  printf("Updated remote: %s\n", fname);
//  printf("-----------------------------------------------------\n");
//  printf("%s", exp_out->text);
//  printf("-----------------------------------------------------\n");

  fd = fopen(cfname, "w");
  fprintf(fd, "%s", exp_out->text);
  fclose(fd);
  exnode_exchange_destroy(exp_out);

  //** Clean up
  exnode_exchange_destroy(exp);

  exnode_destroy(ex);
  exnode_destroy(cex);

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


