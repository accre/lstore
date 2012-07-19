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

#define _log_module_index 172

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
  int timeout, ll;
  ibp_context_t *ic;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited, *tpc_cpu;
  inip_file_t *ifd;
  data_attr_t *da;
  char *fname = NULL;
  char *cfg_name = NULL;
  char *ctype = NULL;
  cache_t *cache;
  exnode_t *ex;
  exnode_exchange_t *exp;
  exnode_exchange_t *exp_in;
  FILE *fd;

  if (argc < 4) {
     printf("\n");
     printf("ex_load [-d log_level] [-c system.cfg] -rs rs.ini file.ex3\n");
     printf("\n");
     return(1);
  }

  tpc_unlimited = thread_pool_create_context("UNLIMITED", 0, 2000);
  tpc_cpu = thread_pool_create_context("CPU", 0, 0);
  ic = ibp_create_context();  //** Initialize IBP
  ds = ds_ibp_create(ic);
  da = ds_attr_create(ds);
  cache_system_init();
  timeout = 120;
  ll = -1;

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
     } else if (strcmp(argv[i], "-rs") == 0) { //** Load the resource file
        i++;
        rs = rs_simple_create(argv[i], ds); i++;
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

  //** Load the fixed options
  fname = argv[i]; i++;

  //** Do some simple sanity checks
  //** Make sure we loaded a simple res service
  if (rs == NULL) {
    printf("Missing resource service!\n");
    return(1);
  }
  if (fname == NULL) {
    printf("Missing input filename!\n");
    return(2);
  }


  //** Create a blank exnode
  ex = exnode_create();

  //** Load it
  exp_in = exnode_exchange_create(EX_TEXT);
  fd = fopen(fname, "r");
  assert(fd != NULL);
  fseek(fd, 0, SEEK_END);
  i = ftell(fd);
  printf("exnode size=%d\n", i);
  type_malloc_clear(exp_in->text, char, i + 2);
  fseek(fd, 0, SEEK_SET);
  fread(exp_in->text, i, 1, fd);
  exp_in->text[i] = '\n';
  exp_in->text[i+1] = '\0';
  fclose(fd);

  printf("Initial exnode=====================================\n");
  printf("%s", exp_in->text);
  printf("===================================================\n");

  exnode_deserialize(ex, exp_in);

  //** Print it
  exp = exnode_exchange_create(EX_TEXT);
  exnode_serialize(ex, exp);

  printf("Loaded exnode=====================================\n");
  printf("%s", exp->text);
  printf("===================================================\n");

  exnode_exchange_destroy(exp_in);
  exnode_exchange_destroy(exp);

  exnode_destroy(ex);

  cache_destroy(cache);
  cache_system_destroy();

  exnode_system_destroy();

  rs_destroy_service(rs);
  ds_attr_destroy(ds, da);
  ds_destroy_service(ds);
  ibp_destroy_context(ic);
  thread_pool_destroy_context(tpc_unlimited);
  thread_pool_destroy_context(tpc_cpu);

  return(0);
}


