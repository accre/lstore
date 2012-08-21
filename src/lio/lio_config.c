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
#include "type_malloc.h"
#include "log.h"
#include "hwinfo.h"
#include "apr_wrapper.h"

//** Define the global LIO config
lio_config_t *lio_gc = NULL;

//***************************************************************
// lio_print_options - Prints the standard supported lio options
//   Use "LIO_COMMON_OPTIONS" in the arg list
//***************************************************************

void lio_print_options(FILE *fd)
{
 fprintf(fd, "    LIO_COMMON_OPTIONS\n");
 fprintf(fd, "       -d level    -Set the debug level (0-20).  Defaults to 0\n");
 fprintf(fd, "       -c config   -Configuration file\n");
}

//***************************************************************
// lio_init - Initializes LIO for use.  argc and argv are
//    modified by removing LIO common options.
//***************************************************************

int lio_init(int *argc, char **argv)
{
  int i, ll, nargs, err;
  int sockets, cores, vcores;
  char *myargv[*argc];
  char *ctype;

  type_malloc_clear(lio_gc, lio_config_t, 1);

  apr_wrapper_start();

  exnode_system_init();

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
     } else if (strcmp(argv[i], "-u") == 0) { //** Load a config file
        i++;
        lio_gc->userid = argv[i]; i++;
     } else {
       myargv[nargs] = argv[i];
       nargs++;
       i++;
     }
  } while (i<*argc);

  //** Adjust argv to reflect the parsed arguments
  memcpy(argv, myargv, sizeof(char *)*nargs);
  *argc = nargs;

  if (lio_gc->cfg_name != NULL) {
     mlog_load(lio_gc->cfg_name);

     if (ll > -1) ll = log_level();

     set_log_level(ll);


     lio_gc->ifd = inip_read(lio_gc->cfg_name);
     lio_gc->timeout = inip_get_integer(lio_gc->ifd, "lio_config", "timeout", 120);

     lio_gc->userid = inip_get_string(lio_gc->ifd, "lio_config", "userid", NULL);

     ctype = inip_get_string(lio_gc->ifd, "lio_config", "ds", DS_TYPE_IBP);
     lio_gc->ds = load_data_service(ctype, lio_gc->cfg_name);
     if (lio_gc->ds == NULL) {
        err = 2;
        log_printf(1, "Error loading data service!  type=%s\n", ctype);
     }
     free(ctype);
     lio_gc->da = ds_attr_create(lio_gc->ds);


     ctype = inip_get_string(lio_gc->ifd, "lio_config", "rs", RS_TYPE_SIMPLE);
     lio_gc->rs = load_resource_service(ctype, lio_gc->cfg_name, lio_gc->ds);
     if (lio_gc->rs == NULL) {
        err = 3;
        log_printf(1, "Error loading resource service!  type=%s\n", ctype);
     }
     free(ctype);

     proc_info(&sockets, &cores, &vcores);
     cores = inip_get_integer(lio_gc->ifd, "lio_config", "tpc_cpu", cores);
     lio_gc->tpc_cpu = thread_pool_create_context("CPU", 1, cores);
     if (lio_gc->tpc_cpu == NULL) {
        err = 5;
        log_printf(0, "Error loading tpc_cpu threadpool!  n=%d\n", cores);
     }

     cores = inip_get_integer(lio_gc->ifd, "lio_config", "tpc_unlimited", 10000);
     lio_gc->tpc_unlimited = thread_pool_create_context("UNLIMITED", 1, cores);
     if (lio_gc->tpc_unlimited == NULL) {
        err = 6;
        log_printf(0, "Error loading tpc_unlimited threadpool!  n=%d\n", cores);
     }

     ctype = inip_get_string(lio_gc->ifd, "lio_config", "os", OS_TYPE_FILE);
     lio_gc->os = create_object_service(ctype, lio_gc->tpc_cpu, lio_gc->cfg_name);
     if (lio_gc->os == NULL) {
        err = 4;
        log_printf(1, "Error loading object service!  type=%s\n", ctype);
     }
     free(ctype);

     ctype = inip_get_string(lio_gc->ifd, "lio_config", "cache", CACHE_TYPE_AMP);
     lio_gc->cache = load_cache(ctype, lio_gc->da, lio_gc->timeout, lio_gc->cfg_name);
     if (lio_gc->os == NULL) {
        err = 4;
        log_printf(0, "Error loading cache service!  type=%s\n", ctype);
     }
     free(ctype);
  }

  lio_gc->creds = os_login(lio_gc->os, lio_gc->userid, OS_CREDS_INI_TYPE, lio_gc->ifd);

  exnode_system_config(lio_gc->ds, lio_gc->rs, lio_gc->os, lio_gc->tpc_unlimited, lio_gc->tpc_cpu, lio_gc->cache);

  return(err);
}

//***************************************************************
//  lio_shutdown - Shuts down the LIO system
//***************************************************************

int lio_shutdown()
{
  cache_destroy(lio_gc->cache);
  cache_system_destroy();

  exnode_system_destroy();

  rs_destroy_service(lio_gc->rs);
  ds_attr_destroy(lio_gc->ds, lio_gc->da);
  ds_destroy_service(lio_gc->ds);
  thread_pool_destroy_context(lio_gc->tpc_unlimited);
  thread_pool_destroy_context(lio_gc->tpc_cpu);
  inip_destroy(lio_gc->ifd);

  os_logout(lio_gc->os, lio_gc->creds);
  os_destroy_service(lio_gc->os);

  apr_wrapper_stop();

  return(0);
}

