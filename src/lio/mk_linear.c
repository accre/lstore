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

#define _log_module_index 173

#include "exnode.h"
#include "log.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j, start_option;
  int n_rid, timeout;
  char *query_text;
  ibp_context_t *ic;
  rs_query_t *rq;
  ex_off_t block_size, total_size;
  exnode_t *ex;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache;
  inip_file_t *ifd;
  data_attr_t *da;
  char *fname_out = NULL;
  char *cfg_name = NULL;
  char *ctype = NULL;
  exnode_exchange_t *exp;
  segment_t *seg = NULL;
  op_generic_t *gop;
   
  if (argc < 5) {
     printf("\n");
     printf("mk_linear [-d log_level] [-c system.cfg] -q rs_query_string \n");
     printf("           -rs rs.ini n_rid block_size total_size file.ex3\n"); 
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

  //*** Parse the args
  i=1;
  do {
     start_option = i;
     
     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        j = atoi(argv[i])*1024; i++;
        set_log_level(j);
    } else if (strcmp(argv[i], "-c") == 0) { //** Load the config file
        i++;
        cfg_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-rs") == 0) { //** Load the resource file
        i++;
        rs = rs_simple_create(argv[i], ds); i++;
     } else if (strcmp(argv[i], "-q") == 0) { //** Load the query
        i++;
        query_text = argv[i]; i++;
     } 

  } while (start_option < i);

  if (cfg_name != NULL) {
     ibp_load_config(ic, cfg_name);

     ifd = inip_read(cfg_name);  
     ctype = inip_get_string(ifd, "cache", "type", CACHE_LRU_TYPE);
     inip_destroy(ifd);
     cache = load_cache(ctype, da, timeout, cfg_name);
     free(ctype);
  } else {
     cache = create_cache(CACHE_LRU_TYPE, da, timeout);
  }

  exnode_system_init(ds, rs, NULL, tpc_unlimited, tpc_cpu, cache);

  //** Load the fixed options
  n_rid = atoi(argv[i]); i++;
  block_size = atoi(argv[i]); i++;
  total_size = atoi(argv[i]); i++;
  fname_out = argv[i]; i++;

  //** Do some simple sanity checks 
  //** Make sure we loaded a simple res service
  if (rs == NULL) {
    printf("Missing resource service!\n");
    return(1);
  }
  if (fname_out == NULL) {
    printf("Missing output filename!\n");
    return(2);
  }
 
  //** Create an empty linear segment  
  seg = create_segment(SEGMENT_TYPE_LINEAR);

  //** Parse the query
  rq = rs_query_parse(rs, query_text);
//  rs_query_add(rs, &rq, RSQ_BASE_OP_AND, "lun", RSQ_BASE_KV_EXACT, "", RSQ_BASE_KV_ANY);
  if (rq == NULL) {
     printf("Error parsing RS query: %s\n", query_text);
     printf("Exiting!\n");
     exit(1);
  }

  //** Make the actual segment
  gop = segment_linear_make(seg, NULL, rq, n_rid, block_size, total_size, timeout);
  i = gop_waitall(gop);
  if (i != 0) {
     printf("ERROR making segment! nerr=%d\n", i);
     return(-1);
  }
  gop_free(gop, OP_DESTROY);

  //** Make an empty exnode
  ex = exnode_create();

  //** and insert it  
  view_insert(ex, seg);


  //** Print it
  exp = exnode_exchange_create(EX_TEXT);
  exnode_serialize(ex, exp);
  printf("%s", exp->text);

  //** and Save if back to disk
  FILE *fd = fopen(fname_out, "w");
  fprintf(fd, "%s", exp->text);
  fclose(fd);
  exnode_exchange_destroy(exp);


  //** Clean up
  exnode_destroy(ex);

  rs_query_destroy(rs, rq);
  
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


