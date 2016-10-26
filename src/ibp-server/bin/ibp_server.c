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

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <apr_time.h>
#include <apr_signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <ibp-server/ibp_server.h>
#include "debug.h"
#include <tbx/log.h>
#include <tbx/dns_cache.h>
#include <tbx/type_malloc.h>
#include "lock_alloc.h"
#include "activity_log.h"

//***** This is just used in the parallel mounting of the resources ****
typedef struct {
  apr_thread_t *thread_id;
  DB_env_t *dbenv;
  tbx_inip_file_t *keyfile;
  char *group;
  int force_resource_rebuild;
} pMount_t;

//*****************************************************************************
// parallel_mount_resource - Mounts a resource in a separate thread
//*****************************************************************************

void *parallel_mount_resource(apr_thread_t *th, void *data) {
   pMount_t *pm = (pMount_t *)data;
   Resource_t *r;

   tbx_type_malloc_clear(r, Resource_t, 1);

   int err = mount_resource(r, pm->keyfile, pm->group, pm->dbenv,
        pm->force_resource_rebuild, global_config->server.lazy_allocate,
        global_config->truncate_expiration);

   if (err != 0) {
     log_printf(0, "parallel_mount_resource:  Error mounting resource!!!!!\n");
     exit(-10);
   }

   free(pm->group);

   r->rl_index = resource_list_insert(global_config->rl, r);

   //** Launch the garbage collection threads
//   launch_resource_cleanup_thread(r);  *** Not safe to do this here due to fork() becing called after mount


   apr_thread_exit(th, 0);
   return(0);   //** Never gets here but suppresses compiler warnings
}


//*****************************************************************************
// resource_health_check - Does periodic health checks on the RIDs
//*****************************************************************************

void *resource_health_check(apr_thread_t *th, void *data) {
  Resource_t *r;
  resource_list_iterator_t it;
  apr_time_t next_check;
  ibp_task_t task;
  apr_time_t dt, dt_wait;
  Cmd_internal_mount_t *cmd;
  resource_usage_file_t usage;
  tbx_stack_t *eject;
  FILE *fd;
  char *rname, *rid_name, *data_dir, *data_device;
  int i, j;
  pid_t pid;
  int err;
//int junk = 0;

  eject = tbx_stack_new();

  memset(&task, 0, sizeof(task));
  cmd = &(task.cmd.cargs.mount);
  dt_wait = apr_time_from_sec(global_config->server.rid_check_interval);
  next_check = apr_time_now() + dt_wait;
  dt = apr_time_from_sec(global_config->server.eject_timeout);

  apr_thread_mutex_lock(shutdown_lock);
  while (shutdown_now == 0) {
    if (apr_time_now() > next_check) {
       log_printf(5, "Running RID check\n");
       it = resource_list_iterator(global_config->rl);
//junk++;
       j = 0;
       while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
          err = read_usage_file(r, &usage);
//if ((junk > 1) && (it == 1)) { err= 1; log_printf(0,"Forcing a failure\n"); }
          if (err == 0) {
             r->last_good_check = apr_time_now();  //** this should be done in resource.c But I'm the only one that ever touches the routine
          } else if (apr_time_now() > (r->last_good_check + dt)) {
             strncpy(cmd->crid, global_config->rl->res[r->rl_index].crid, sizeof(cmd->crid));
             strncpy(cmd->msg, "Health check failed. Ejecting drive.", sizeof(cmd->msg));

             //** Push the failed drive on the ejected stack
             j++;
             tbx_stack_push(eject, strdup(r->data_pdev));
             tbx_stack_push(eject, strdup(r->device));
             tbx_stack_push(eject, strdup(cmd->crid));

             apr_thread_mutex_unlock(shutdown_lock);
             cmd->delay = 10;
             handle_internal_umount(&task);
             apr_thread_mutex_lock(shutdown_lock);
          }
       }
       resource_list_iterator_destroy(global_config->rl, &it);

       log_printf(5, "Finished RID check tbx_stack_count(eject)=%d\n", tbx_stack_count(eject));

       if ((j > 0) && (global_config->server.rid_eject_script != NULL)) {  //** Ejected something so run the eject program
          //** Make the RID list file and name
          i = strlen(global_config->server.rid_eject_tmp_path) + 1 + 6 + 30;
          tbx_type_malloc(rname, char, i);
          snprintf(rname, i, "%s/eject." TT, global_config->server.rid_eject_tmp_path, apr_time_now());
          fd = fopen(rname, "w");
          if (fd == NULL) {
             log_printf(0, "ERROR: failed to create RID eject temp file: %s\n", rname);
             goto bail;
          }

          //** Line format: total # of RIDs | Good RIDs | Ejected/Bad RIDs
          i = j+resource_list_n_used(global_config->rl);
          fprintf(fd, "%d|%d|%d\n", i, resource_list_n_used(global_config->rl), j);

          //** Cycle though the good RIDs printing RID info
          //** Line format: RID|data_directory|data_device|status(0=good|1=bad)
          it = resource_list_iterator(global_config->rl);
          while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
              fprintf(fd, "%s|%s|%s|%d\n", global_config->rl->res[r->rl_index].crid, r->device, r->data_pdev, 0);
          }
          resource_list_iterator_destroy(global_config->rl, &it);

          //** Now do the same for the ejected drives
          for (i=0; i<j; i++) {
             rid_name = tbx_stack_pop(eject);  data_dir = tbx_stack_pop(eject);  data_device = tbx_stack_pop(eject);
             fprintf(fd, "%s|%s|%s|%d\n", rid_name, data_dir, data_device, 1);
             free(rid_name); free(data_dir); free(data_device);
          }
          fclose(fd);

          //** Now spawn the child process to do it's magic
          pid = fork();
          if (pid == 0) { //** Child process
             execl(global_config->server.rid_eject_script, global_config->server.rid_eject_script, rname, NULL);
             exit(0);  //** Should never get here
          } else if (pid == -1) { //** Fork error
            log_printf(0, "FORK error!!!! rname=%s\n", rname);
          }
       bail:
          free(rname);
       }

       next_check = apr_time_now() + apr_time_from_sec(global_config->server.rid_check_interval);
    }

    apr_thread_cond_timedwait(shutdown_cond, shutdown_lock, dt_wait);
  }
  apr_thread_mutex_unlock(shutdown_lock);

  tbx_stack_free(eject, 1);

  apr_thread_exit(th, 0);
  return(0);   //** Never gets here but suppresses compiler warnings
}

//*****************************************************************************
// log_preamble - Print the initial log file output
//*****************************************************************************

void log_preamble(Config_t *cfg)
{
  char buffer[100*1024];
  int used = 0;
  apr_time_t t = get_starttime();
  apr_ctime(buffer, t);

  log_printf(0, "\n");
  log_printf(0, "*****************************************************************\n");
  log_printf(0, "Starting ibp_server on %s\n", buffer);
  log_printf(0, "*****************************************************************\n");
  log_printf(0, "\n");

  log_printf(0, "*********************Printing configuration file **********************\n\n");


  print_config(buffer, &used, sizeof(buffer), cfg);
  log_printf(0, "%s", buffer);

  log_printf(0, "*****************************************************************\n\n");
}


//*****************************************************************************
//  parse_config - Parses the config file(fname) and initializes the config
//                 data structure (cfg).
//*****************************************************************************

int parse_config(tbx_inip_file_t *keyfile, Config_t *cfg, int force_rebuild)
{
  Server_t *server;
  char *str, *bstate;
  int val, k, i, timeout_ms;
  char iface_default[1024];
  apr_time_t t;
  pMount_t *pm, *pmarray;

  // *** Initialize the data structure to default values ***
  server = &(cfg->server);
  server->max_threads = 64;
  server->max_pending = 16;
  server->min_idle = apr_time_make(60, 0);
  server->stats_size = 5000;
  timeout_ms = 1 * 1000;   //** Wait 1 sec
//  tbx_ns_timeout_set(&(server->timeout), 1, 0);  //**Wait 1sec
  server->timeout_secs = timeout_ms / 1000;
  server->logfile = "ibp.log";
  server->log_overwrite = 0;
  server->tbx_log_level = 0;
  server->log_maxsize = 100;
  server->debug_level = 0;
  server->timestamp_interval = 60;
  server->password = DEFAULT_PASSWORD;
  server->lazy_allocate = 1;
  server->backoff_scale = 1.0/10;
  server->backoff_max = 30;
  server->big_alloc_enable = (sizeof(off_t) > 4) ? 1 : 0;
  server->splice_enable = 0;
  server->alog_name = "ibp_activity.log";
  server->alog_max_size = 50;
  server->alog_max_history = 1;
  server->alog_host = NULL;
  server->alog_port = 0;
  server->port = IBP_PORT;
  server->return_cap_id = 1;
  server->rid_check_interval = 15;
  server->eject_timeout = 35;
  server->rid_log = "/log/rid.log";
  server->rid_eject_script = NULL;
  server->rid_eject_tmp_path = "/tmp";

  cfg->dbenv_loc = "/tmp/ibp_dbenv";
  cfg->db_mem = 256;
  cfg->force_resource_rebuild = force_rebuild;
  cfg->truncate_expiration = 0;
  cfg->soft_fail = -1;

  // *** Parse the Server settings ***
  server->port = tbx_inip_get_integer(keyfile, "server", "port", server->port);

  //** Make the default interface
  gethostname(iface_default, sizeof(iface_default));
  i = strlen(iface_default);
  tbx_append_printf(iface_default, &i, sizeof(iface_default), ":%d", server->port);

  char *iface_str = tbx_inip_get_string(keyfile, "server", "interfaces", iface_default);

  //** Determine the number of interfaces
  char *list[100];
  i = 0;
  list[i] = tbx_stk_string_token(iface_str, ";", &bstate, &k);
  while (strcmp(list[i], "") != 0) {
     i++;
     list[i] = tbx_stk_string_token(NULL, ";", &bstate, &k);
  }

  server->n_iface = i;

  //** Now parse and store them
  server->iface = (interface_t *)malloc(sizeof(interface_t)*server->n_iface);
  interface_t *iface;
  for (i=0; i<server->n_iface; i++) {
      iface = &(server->iface[i]);
      iface->hostname = tbx_stk_string_token(list[i], ":", &bstate, &k);
      if (sscanf(tbx_stk_string_token(NULL, " ", &bstate, &k), "%d", &(iface->port)) != 1) {
         iface->port = server->port;
      }
  }

  server->max_threads = tbx_inip_get_integer(keyfile, "server", "threads", server->max_threads);
  server->max_pending = tbx_inip_get_integer(keyfile, "server", "max_pending", server->max_pending);
  t = 0; t = tbx_inip_get_integer(keyfile, "server", "min_idle", t);
  if (t != 0) server->min_idle = apr_time_make(t, 0);
  val = tbx_inip_get_integer(keyfile, "server", "max_network_wait_ms", timeout_ms);

  int sec = val/1000;
  int us = val - 1000*sec;
  us = us * 1000;  //** Convert from ms->us
  server->timeout_secs = sec;
//log_printf(0, "parse_config: val=%d sec=%d us=%d\n", val, sec, us);
  tbx_ns_timeout_set(&(server->timeout), sec, us);  //**Convert it from ms->us

  server->stats_size =  tbx_inip_get_integer(keyfile, "server", "stats_size", server->stats_size);
  server->password = tbx_inip_get_string(keyfile, "server", "password", server->password);
  server->logfile = tbx_inip_get_string(keyfile, "server", "log_file", server->logfile);
  server->tbx_log_level = tbx_inip_get_integer(keyfile, "server", "tbx_log_level", server->tbx_log_level);
  server->log_maxsize = tbx_inip_get_integer(keyfile, "server", "log_maxsize", server->log_maxsize) * 1024 * 1024;
  server->debug_level = tbx_inip_get_integer(keyfile, "server", "debug_level", server->debug_level);
  server->lazy_allocate = tbx_inip_get_integer(keyfile, "server", "lazy_allocate", server->lazy_allocate);
  server->big_alloc_enable = tbx_inip_get_integer(keyfile, "server", "big_alloc_enable", server->big_alloc_enable);
  server->splice_enable = tbx_inip_get_integer(keyfile, "server", "splice_enable", server->splice_enable);
  server->backoff_scale = tbx_inip_get_double(keyfile, "server", "backoff_scale", server->backoff_scale);
  server->backoff_max = tbx_inip_get_double(keyfile, "server", "backoff_max", server->backoff_max);

  server->return_cap_id = tbx_inip_get_integer(keyfile, "server", "return_cap_id", server->return_cap_id);

  cfg->dbenv_loc = tbx_inip_get_string(keyfile, "server", "db_env_loc", cfg->dbenv_loc);
  cfg->db_mem = tbx_inip_get_integer(keyfile, "server", "db_mem", cfg->db_mem);

  server->alog_name = tbx_inip_get_string(keyfile, "server", "activity_file", server->alog_name);
  server->alog_max_size = tbx_inip_get_integer(keyfile, "server", "activity_maxsize", server->alog_max_size) * 1024 * 1024;
  server->alog_max_history = tbx_inip_get_integer(keyfile, "server", "activity_max_history", server->alog_max_history);
  server->alog_host = tbx_inip_get_string(keyfile, "server", "activity_host", server->alog_host);
  server->alog_port = tbx_inip_get_integer(keyfile, "server", "activity_port", server->alog_port);

  server->rid_check_interval = tbx_inip_get_integer(keyfile, "server", "rid_check_interval", server->rid_check_interval);
  server->eject_timeout = tbx_inip_get_integer(keyfile, "server", "eject_timeout", server->eject_timeout);
  server->rid_log = tbx_inip_get_string(keyfile, "server", "rid_log", server->rid_log);
  server->rid_eject_script = tbx_inip_get_string(keyfile, "server", "rid_eject_script", server->rid_eject_script);
  server->rid_eject_tmp_path = tbx_inip_get_string(keyfile, "server", "rid_eject_tmp_path", server->rid_eject_tmp_path);

  if (force_rebuild == 0) {  //** The command line option overrides the file
     cfg->force_resource_rebuild = tbx_inip_get_integer(keyfile, "server", "force_resource_rebuild", cfg->force_resource_rebuild);
  }
  cfg->truncate_expiration = tbx_inip_get_integer(keyfile, "server", "truncate_duration", cfg->truncate_expiration);

  i = tbx_inip_get_integer(keyfile, "server", "soft_fail", 0);
  cfg->soft_fail = (i==0) ? -1 : 0;

  //*** Do some initial config of the log and debugging info ***
  tbx_log_open(cfg->server.logfile, 0);
  tbx_set_log_level(cfg->server.tbx_log_level);
  set_debug_level(cfg->server.debug_level);
  tbx_set_log_maxsize(cfg->server.log_maxsize);

  // *** Now iterate through each resource which is assumed to be all groups beginning with "resource" ***
  apr_pool_t *mount_pool;
  apr_pool_create(&mount_pool, NULL);
  cfg->dbenv = create_db_env(cfg->dbenv_loc, cfg->db_mem, cfg->force_resource_rebuild);
  k= tbx_inip_group_count(keyfile);
  tbx_type_malloc_clear(pmarray, pMount_t, k-1);
  tbx_inip_group_t *igrp = tbx_inip_group_first(keyfile);
  val = 0;
  for (i=0; i<k; i++) {
      str = tbx_inip_group_get(igrp);
      if (strncmp("resource", str, 8) == 0) {
         pm = &(pmarray[val]);
         pm->keyfile = keyfile;
         pm->group = strdup(str);
         pm->dbenv = cfg->dbenv;
         pm->force_resource_rebuild = cfg->force_resource_rebuild;

         apr_thread_create(&(pm->thread_id), NULL, parallel_mount_resource, (void *)pm, mount_pool);

         val++;
      }

      igrp = tbx_inip_group_next(igrp);
  }

  //** Wait for all the threads to join **
  apr_status_t dummy;
  for (i=0; i<val; i++) {
     apr_thread_join(&dummy, pmarray[i].thread_id);
  }

  free(pmarray);

  if (val < 0) {
     printf("parse_config:  No resources defined!!!!\n");
     abort();
  }

  return(0);
}

//*****************************************************************************
//*****************************************************************************

void cleanup_config(Config_t *cfg)
{
  Server_t *server;
  int i;

  server = &(cfg->server);

  if (server->rid_eject_script) free(server->rid_eject_script);
  if (server->rid_eject_tmp_path) free(server->rid_eject_tmp_path);
  free(server->password);
  free(server->logfile);
  free(server->default_acl);
  free(cfg->dbenv_loc);
  free(server->rid_log);

  for (i=0; i<server->n_iface; i++) {
    free(server->iface[i].hostname);
  }
  free(server->iface);
}

//*****************************************************************************
//*****************************************************************************

void signal_shutdown(int sig)
{
  char date[128];
  apr_ctime(date, apr_time_now());

  log_printf(0, "Shutdown requested on %s\n", date);

  apr_thread_mutex_lock(shutdown_lock);
  shutdown_now = 1;
  apr_thread_cond_broadcast(shutdown_cond);
  apr_thread_mutex_unlock(shutdown_lock);

  signal_taskmgr();
  tbx_network_wakeup(global_network);

  return;
}

//*****************************************************************************
// ibp_shutdown - Shuts down everything
//*****************************************************************************

int ibp_shutdown(Config_t *cfg)
{
  int err;
  Resource_t *r;
  resource_list_iterator_t it;

  //** Close all the resources **
  it = resource_list_iterator(cfg->rl);
  while ((r = resource_list_iterator_next(cfg->rl, &it)) != NULL) {
    if ((err = umount_resource(r)) != 0) {
       char tmp[RID_LEN];
       log_printf(0, "ibp_server: Error closing Resource %s!  Err=%d\n",ibp_rid2str(r->rid, tmp), err);
    }
    free(r);
  }
  resource_list_iterator_destroy(cfg->rl, &it);

  //** Now clsoe the DB environment **
  if ((err = close_db_env(cfg->dbenv)) != 0) {
     log_printf(0, "ibp_server: Error closing DB envirnment!  Err=%d\n", err);
  }

  return(0);
}

//*****************************************************************************
// configure_signals - Configures the signals
//*****************************************************************************

void configure_signals()
{

  //***Attach the signal handler for shutdown
  apr_signal_unblock(SIGQUIT);
  apr_signal(SIGQUIT, signal_shutdown);

  //** Want everyone to ignore SIGPIPE messages
#ifdef SIGPIPE
  apr_signal_block(SIGPIPE);
#endif
}

//*****************************************************************************
//*****************************************************************************
//*****************************************************************************

int main(int argc, const char **argv)
{
  Config_t config;
  char *config_file;
  int i, j, k;
  apr_thread_t *rid_check_thread;
  apr_status_t dummy;

  assert(apr_initialize() == APR_SUCCESS);
  assert(apr_pool_create(&global_pool, NULL) == APR_SUCCESS);
  tbx_random_startup();

  shutdown_now = 0;

  global_config = &config;   //** Make the global point to what's loaded
  memset(global_config, 0, sizeof(Config_t));  //** init the data
  global_network = NULL;

  if (argc < 1) {
     printf("ibp_server [-d] [-r] config_file\n\n");
     printf("-r          - Rebuild RID databases. Same as force_rebuild=2 in config file\n");
     printf("-d          - Run as a daemon\n");
     printf("config_file - Configuration file\n");
     return(0);
  }

  int daemon = 0;
  int force_rebuild = 0;
  for (i=1; i<argc; i++) {
     if (strcmp(argv[i], "-d") == 0) {
        daemon = 1;
     } else if (strcmp(argv[i], "-r") == 0) {
        force_rebuild = 2;
     }
  }

  config_file = (char *)argv[argc-1];
  global_config->config_file = config_file;

  //*** Open the config file *****
  printf("Config file: %s\n\n", config_file);

  tbx_inip_file_t *keyfile;

  //* Load the config file
  keyfile = tbx_inip_file_read(config_file);
  if (keyfile == NULL) {
    log_printf(0, "ibp_load_config:  Error parsing config file! file=%s\n", config_file);
    return(-1);
  }


  set_starttime();

  config.rl = create_resource_list(1);

  //** Parse the global options first ***
  parse_config(keyfile, &config, force_rebuild);

  //** Make sure we have enough fd's
  i = sysconf(_SC_OPEN_MAX);
  j = 3*config.server.max_threads + 2*resource_list_n_used(config.rl) + 64;
  if (i < j) {
     k = (i - 2*resource_list_n_used(config.rl) - 64) / 3;
     log_printf(0, "ibp_server: ERROR Too many threads!  Current threads=%d, n_resources=%d, and max fd=%d.\n", config.server.max_threads, resource_list_n_used(config.rl), i);
     log_printf(0, "ibp_server: Either make threads < %d or increase the max fd > %d (ulimit -n %d)\n", k, j, j);
     shutdown_now = 1;
  }

  init_thread_slots(2*config.server.max_threads);  //** Make pigeon holes

  tbx_dnsc_startup_sized(1000);
  init_subnet_list(config.server.iface[0].hostname);

  //*** Install the commands: loads Vectable info and parses config options only ****
  install_commands(keyfile);

  tbx_inip_destroy(keyfile);   //Free the keyfile context

  log_preamble(&config);

  configure_signals();   //** Setup the signal handlers

  //*** Set up the shutdown variables
  apr_thread_mutex_create(&shutdown_lock, APR_THREAD_MUTEX_DEFAULT, global_pool);
  apr_thread_cond_create(&shutdown_cond, global_pool);

//  log_printf(0, "Looking up resource 2 and printing info.....\n")
//  print_resource(resource_lookup(config.rl, "2"), log_fd());

  init_stats(config.server.stats_size);
  lock_alloc_init();

  //***Launch as a daemon if needed***
  if (daemon == 1) {    //*** Launch as a daemon ***
     if ((strcmp(config.server.logfile, "stdout") == 0) ||
         (strcmp(config.server.logfile, "stderr") == 0)) {
        log_printf(0, "Can't launch as a daemom because log_file is either stdout or stderr\n");
        log_printf(0, "Running in normal mode\n");
     } else if (fork() == 0) {    //** This is the daemon
        log_printf(0, "Running as a daemon.\n");
        tbx_log_flush();
        fclose(stdin);     //** Need to close all the std* devices **
        fclose(stdout);
        fclose(stderr);

        char fname[1024];
        fname[1023] = '\0';
        snprintf(fname, 1023, "%s.stdout", config.server.logfile);
        assert((stdout = fopen(fname, "w")) != NULL);
        snprintf(fname, 1023, "%s.stderr", config.server.logfile);
        assert((stderr = fopen(fname, "w")) != NULL);
//        stdout = stderr = log_fd();  //** and reassign them to the log device
printf("ibp_server.c: STDOUT=STDERR=LOG_FD() dnoes not work!!!!!!!!!!!!!!!!!!!!!!!!\n");
     } else {           //** Parent exits
        exit(0);
     }
  }

//  test_alloc();   //** Used for testing allocation speed only

  //*** Initialize all command data structures.  This is mainly 3rd party commands ***
  initialize_commands();


  //** Launch the garbage collection threads ...AFTER fork!!!!!!
  resource_list_iterator_t it;
  Resource_t *r;
  it = resource_list_iterator(global_config->rl);
  while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
     launch_resource_cleanup_thread(r);
  }
  resource_list_iterator_destroy(global_config->rl, &it);

  //** Launch the RID health checker thread
  apr_thread_create(&rid_check_thread, NULL, resource_health_check, NULL, global_pool);

  //*** Start the activity log ***
  alog_open();

  server_loop(&config);     //***** Main processing loop ******

  //** Wait forthe healther checker thread to complete
  apr_thread_join(&dummy, rid_check_thread);

  //*** Shutdown the activity log ***
  alog_close();

  //*** Destroy all the 3rd party structures ***
  destroy_commands();

  lock_alloc_destroy();

  destroy_thread_slots();

  ibp_shutdown(&config);

  free_resource_list(config.rl);

  free_stats();

  cleanup_config(&config);
  log_printf(0, "main: Completed shutdown. Exiting\n");
//  close_log();
//  close_debug();

  apr_terminate();

  return(0);
}
