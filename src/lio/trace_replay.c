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

#define _log_module_index 168

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "trace.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize+1];
  tbuffer_t tbuf;
  int i, j, start_option, np, update_interval;
  int timeout;
  ibp_context_t *ic;
  object_service_fn_t *os = NULL;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache = NULL;
  data_attr_t *da;
  char *cfg_name = NULL;
  char *rs_name = NULL;
  char *ctype = NULL;
  char *trace_header;
  char *base_path;
  char *template_name = NULL;
  inip_file_t *ifd;
  exnode_t *tex;
  exnode_exchange_t *template_exchange;
  ex_iovec_t *iov;
  op_generic_t *gop;
  trace_t *trace;
  trace_op_t *top;
  opque_t *q;
  apr_time_t start_time, end_time;

  double dt;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("trace_replay [-d log_level] [-rs rs.ini] [-np n_at_once] [-i update_interval] -c system.cfg [-path base_path] -template template.ex3 -t header.trh \n");

     printf("    -path base_path - Base LIO directory path\n");
     printf("    -np n_at_once   - Number of commands to execute in parallel (default is 1)\n");
     printf("    -t header.trh   - Trace header file\n");
     printf("    -rs rs.ini      - Resource Service config. \n");
     printf("    -c system.cfg   - IBP and Cache configuration options\n");
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
  np = 1;
  update_interval = 1000;

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        j = atoi(argv[i])*1024; i++;
        set_log_level(j);
    } else if (strcmp(argv[i], "-rs") == 0) { //** Load the resource file
        i++;
        rs_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-c") == 0) { //** Load a config file
        i++;
        cfg_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-template") == 0) { //** The template exnode
        i++;
        template_name = argv[i]; i++;
     } else if (strcmp(argv[i], "-path") == 0) { //** Base LIO path
        i++;
        base_path = argv[i]; i++;
     } else if (strcmp(argv[i], "-t") == 0) { //** TRace config file
        i++;
        trace_header = argv[i]; i++;
     } else if (strcmp(argv[i], "-np") == 0) { //** TRace config file
        i++;
        np = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-i") == 0) { //** TRace config file
        i++;
        update_interval = atoi(argv[i]); i++;
     } 
  } while (start_option < i);


  if (cfg_name != NULL) {
     ibp_load_config(ic, cfg_name);

     ifd = inip_read(cfg_name);  
     ctype = inip_get_string(ifd, "cache", "type", CACHE_LRU_TYPE);
     cache = load_cache(ctype, da, timeout, cfg_name);
     free(ctype);

     ctype = inip_get_string(ifd, "os", "type", CACHE_LRU_TYPE);
     os = create_object_service(ctype, cfg_name);
     free(ctype);

     if (rs_name == NULL) rs = rs_simple_create(cfg_name, ds);

     inip_destroy(ifd);
  } else {
     cache = create_cache(CACHE_LRU_TYPE, da, timeout);
  }

  if (rs_name != NULL) rs = rs_simple_create(rs_name, ds);

  assert(rs != NULL);
  assert(template_name == NULL);

  exnode_system_init(ds, rs, os, tpc_unlimited, tpc_cpu, cache);

  //** Load the template
  template_exchange = exnode_exchange_load_file(template_name);
  tex = exnode_create();
  exnode_deserialize(tex, template_exchange);

  //** Load the trace
  trace = trace_load(exnode_service_set, tex, da, timeout, trace_header);
  type_malloc_clear(iov, ex_iovec_t, trace->n_files);

  q = new_opque();
  tbuffer_single(&tbuf, bufsize, buffer);
  start_time = apr_time_now();
  for (i=0; i<trace->n_ops; i++) {
     if ((i%update_interval) == 0) {
        log_printf(0, "trace_replay: Submitting task %d\n", i);
     }

     top = &(trace->ops[i]);
     ex_iovec_single(&(iov[top->fd]), top->offset, top->len);
     if (top->cmd == CMD_READ) {
        gop = segment_write(trace->files[top->fd].seg, da, 1, &(iov[top->fd]), &tbuf, 0, timeout);
     } else {
        gop = segment_write(trace->files[top->fd].seg, da, 1, &(iov[top->fd]), &tbuf, 0, timeout);
     }

     gop_set_id(gop, i);
     opque_add(q, gop);
     if (opque_tasks_left(q) >= np) {
        gop = opque_waitany(q);
        if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
           log_printf(0, "trace_replay: Errow with command index=%d\n", gop_id(gop));
        }
        gop_free(gop, OP_DESTROY);
     }
  }

  log_printf(0, "trace_replay:: Completed task submission (n_ops=%d).  Waiting for taks to complete\n", trace->n_ops);
  opque_waitall(q);

  end_time = apr_time_now();

  dt = end_time - start_time;
  dt = dt / APR_USEC_PER_SEC;
  log_printf(0, "trace_replay:  Total processing time: %lf\n", dt);

  trace_destroy(trace);

  //** Shut everything down;
  exnode_destroy(tex);
  cache_destroy(cache);
  cache_system_destroy();

  exnode_system_destroy();

  os_destroy(os);
  ds_attr_destroy(ds, da);
  ds_destroy_service(ds);
  ibp_destroy_context(ic);  
  thread_pool_destroy_context(tpc_unlimited);
  thread_pool_destroy_context(tpc_cpu);
 
  return(0);
}


