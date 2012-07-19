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

#define n_inspect 10
char *inspect_opts[] = { "DUMMY", "inspect_quick_check",  "inspect_scan_check",  "inspect_full_check",
                                  "inspect_quick_repair", "inspect_scan_repair", "inspect_full_repair",
                                  "inspect_soft_errors",  "inspect_hard_errors", "inspect_migrate" };


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j, start_option, whattodo;
  int timeout, ll, force_repair;
  ibp_context_t *ic;
  inip_file_t *ifd;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache = NULL;
  data_attr_t *da;
  char *fname = NULL;
  char *cfg_name = NULL;
  char *ctype;
  exnode_t *ex;
  exnode_exchange_t *exp, *exp_out;
  segment_t *seg;
  op_generic_t *gop;
  ex_off_t bufsize = 100*1024*1024;
  ex_off_t n;
  op_status_t status;
  FILE *fd;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("ex_inspect [-d log_level] [-c system.cfg] [-bufsize n] [-force] inspect_opt file.ex3\n");
     printf("    file.ex3 - File to inspect\n");
     printf("    -c system.cfg - IBP and Cache configuration options\n");
     n = bufsize / 1024 / 1024;
     printf("    -bufsize n    - Buffer size to use.  Defaults to " XOT "MB\n", n);
     printf("    -force        - Forces data replacement even if it would result in data loss\n");
     printf("    inspect_opt   - Inspection option.  One of the following:\n");
     for (i=1; i<n_inspect; i++) { printf("                 %s\n", inspect_opts[i]); }
     printf("\n");
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
  force_repair = 0;

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
     } else if (strcmp(argv[i], "-force") == 0) { //** Force repair
        i++;
        force_repair = INSPECT_FORCE_REPAIR;
     } else if (strcmp(argv[i], "-bufsize") == 0) { //** Change the buffer size
        i++;
        bufsize = atoi(argv[i])*1024*1024; i++;
        if (bufsize <= 0) {
           n = bufsize / 1024 / 1024;
           printf("Invalid buffer size=" XOT "\n", n);
           abort();
        }
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

  //** Get the inspect_opt;
  whattodo = -1;
  for(j=1; j<n_inspect; j++) {
     if (strcasecmp(inspect_opts[j], argv[i]) == 0) { whattodo = j; break; }
  }
  if (whattodo == -1) {
     printf("Invalid inspect option:  %s\n", argv[i]);
     abort();
  }
  if ((whattodo == INSPECT_QUICK_REPAIR) || (whattodo == INSPECT_SCAN_REPAIR) || (whattodo == INSPECT_FULL_REPAIR)) whattodo |= force_repair;
  i++;

  //** This is the file to inspect
  fname = argv[i]; i++;
  if (fname == NULL) {
    printf("Missing remote file!\n");
    return(2);
  }
 
  //** Load it
  exp = exnode_exchange_load_file(fname);
  ex = exnode_create();
  exnode_deserialize(ex, exp);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     printf("No default segment!  Aborting!\n");
     abort();
  }

printf("whattodo=%d\n", whattodo);
  //** Execute the inspection operation
  gop = segment_inspect(seg, da, stdout, whattodo, bufsize, timeout);
flush_log();
  gop_waitany(gop);
flush_log();
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);

  //** Print out the results
  whattodo = whattodo & INSPECT_COMMAND_BITS;
  switch(whattodo) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
    case (INSPECT_MIGRATE):
        if (status.op_status == OP_STATE_SUCCESS) {
           printf("Success!\n");
        } else {
           printf("Error!  status=%d error_code=%d\n", status.op_status, status.error_code);
        }
        break;
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        if (status.op_status == OP_STATE_SUCCESS) {
           printf("Success! error_cont=%d\n", status.error_code);
        } else {
           printf("Error!  status=%d error_code=%d\n", status.op_status, status.error_code);
        }
        break;
  }


  //** Store the updated exnode back to disk
  exp_out = exnode_exchange_create(EX_TEXT);
  exnode_serialize(ex, exp_out);
//  printf("Updated remote: %s\n", fname);
//  printf("-----------------------------------------------------\n");
//  printf("%s", exp_out->text);
//  printf("-----------------------------------------------------\n");

  fd = fopen(fname, "w");
  fprintf(fd, "%s", exp_out->text);
  fclose(fd);
  exnode_exchange_destroy(exp_out);


  //** Clean up
  exnode_exchange_destroy(exp);

  exnode_destroy(ex);

  exnode_system_destroy();
  cache_destroy(cache);
  cache_system_destroy();

  rs_destroy_service(rs);
  ds_attr_destroy(ds, da);
  ds_destroy_service(ds);
  ibp_destroy_context(ic);
  thread_pool_destroy_context(tpc_unlimited);
  thread_pool_destroy_context(tpc_cpu);

  return(0);
}


