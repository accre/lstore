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

#include "lio.h"

//** Define the global LIO config
lio_config_t *lio_gc = NULL;

//***************************************************************
// lio_print_options - Prints the standard supported lio options
//   Use "LIO_COMMON_OPTIONS" in the arg list
//***************************************************************

void lio_print_options(FILE *fd)
{
 fprintf(fd, "LIO_COMMON_OPTIONS\n");
 fprintf(fd, "   -d level    -Set the debug level (0-20).  Defaults to 0\n");
 fprintf(fd, "   -c config   -Configuration file\n");
}

//***************************************************************
// lio_init - Initializes LIO for use.  argc and argv are
//    modified by removing LIO common options.
//***************************************************************

int lio_init(int *argc, int **argv)
{
  int i, start_option, ll, nargs;
  int nargs;
  char *myargv[*nargc];
  char *ctype;

  type_malloc_clear(lio_gc, lio_config_t, 1);

  //** Parse any arguments
  memcpy(myargv, argv, sizeof(char *)*(*argc));

  nargs = 1;  //** argv[0] is preserved as the calling name
  i=1;
  do {
     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        ll = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-c") == 0) { //** Load a config file
        i++;
        lio_gc->cfg_name = argv[i]; i++;
     } else {
       myargv[nargs] = argv[i];
       nargs++;
       i++;
     }
  } while (i<*argc);

  //** Adjust argv to reflect the parsed arguments
  memcpy(argv, myargv, sizeof(char *)*nargs);
  *argc = nargs;

  lio_gc->tpc_unlimited = thread_pool_create_context("UNLIMITED", 0, 2000);
  lio_gc->tpc_cpu = thread_pool_create_context("CPU", 0, 0);
  lio_gc->rs = NULL;
  lio_gc->ic = ibp_create_context();  //** Initialize IBP
  lio_gc->ds = ds_ibp_create(ic);
  lio_gc->da = ds_attr_create(ds);
  lio_gc->timeout = 120;

  if (lio_gc->cfg_name != NULL) {
     ibp_load_config(ic, lio_gc->cfg_name);

     lio_gc->inifd = inip_read(lio_gc->cfg_name);
     lio_gc->timeout = inip_get_integer(lio_gc->ifd, "lio", "timeout", lio_gc->timeout);
     ctype = inip_get_string(lio_gc->ifd, "cache", "type", CACHE_AMP_TYPE);
     cache = load_cache(ctype, lio_gc->da, lio_gc->timeout, lio_gc->cfg_name);
     free(ctype);
     mlog_load(lio_gc->cfg_name);
     lio_gc->rs = rs_simple_create(lio_gc->cfg_name, lio_gc->ds);
  } else {
     cache = create_cache(CACHE_AMP_TYPE, lio_gc->da, lio_gc->timeout);
  }

  if (ll > -1) set_log_level(ll);

  exnode_system_init(lio_gc->ds, lio_gc->rs, NULL, lio_gc->tpc_unlimited, lio_gc->tpc_cpu, lio_gc->cache);

  return(0);
}

//***************************************************************
//  lio_shutdown - Shuts down the LIO system
//***************************************************************

int lio_shutdown()
{
  cache_destroy(lio_gc->cache);

  exnode_system_destroy();

  ds_attr_destroy(lio_gc->ds, lio_gc->da);
  ds_destroy_service(lio_gc->ds);
  ibp_destroy_context(lio_gc->ic);
  thread_pool_destroy_context(lio_gc->tpc_unlimited);
  thread_pool_destroy_context(lio_gc->tpc_cpu);
  inip_destroy(lio_gc->inifd);

  return(0);
}

