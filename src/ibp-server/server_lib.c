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

//*****************************************************************
//*****************************************************************

#include <string.h>
#include <signal.h>
#include <sys/socket.h>
#include <apr_time.h>
#include "stack.h"
#include <ibp-server/ibp_server.h>
#include <tbx/log.h>
#include "debug.h"
#include "allocation.h"
#include "resource.h"
#include <tbx/network.h>
#include "ibp_task.h"
#include "ibp_protocol.h"
#include "activity_log.h"
#include <tbx/net_sock.h>
#include <tbx/append_printf.h>

tbx_network_t *global_network;

apr_time_t depot_start_time;   //**Depot start time

uint64_t task_count;
apr_thread_mutex_t *task_count_lock = NULL;

typedef struct {
  int max_threads;       //** Max allowed threads
  int curr_threads;      //** Current running threads
  int request_thread;    //** Request for a new thread
  int reject_count;      //** How many sockets we've closed due to load
  uint64_t reject_total; //** Total nubmer of sockets rejected ofver  the life of the depot
  apr_thread_mutex_t *lock;  //** Lock for accessing the taskmgr data
  apr_thread_cond_t  *cond;
  tbx_stack_t *completed;
} Taskmgr_t;

Taskmgr_t taskmgr;  //** Global used by the task rountines

//*****************************************************************
//  server_ns_readline - Helper for reading text from the network
//*****************************************************************

int server_ns_readline(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_readline(ns, &tbuf, 0, bsize, dt));
}

//*****************************************************************
//  server_ns_readline_raw - Helper for reading text from the network
//*****************************************************************

int server_ns_readline_raw(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt, int *status)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_readline_raw(ns, &tbuf, 0, bsize, dt, status));
}

//*****************************************************************
//  server_ns_write - Helper for dumping data to the network
//*****************************************************************

int server_ns_write(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_write(ns, &tbuf, 0, bsize, dt));
}

//*****************************************************************
//  server_ns_read - Helper for getting data from the network
//*****************************************************************

int server_ns_read(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_read(ns, &tbuf, 0, bsize, dt));
}

//*****************************************************************
//  server_ns_write_block - Helper for dumping data to the network
//*****************************************************************

int server_ns_write_block(tbx_ns_t *ns, apr_time_t end_time, char *buffer, int bsize)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_write_block(ns, end_time, &tbuf, 0, bsize));
}

//*****************************************************************
//  server_ns_read_block - Helper for getting data from the network
//*****************************************************************

int server_ns_read_block(tbx_ns_t *ns, apr_time_t end_time, char *buffer, int bsize)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, bsize, buffer);
    return(tbx_ns_read_block(ns, end_time, &tbuf, 0, bsize));
}

//*****************************************************************
//  convert_epoch_time2net - Des what is says:)
//*****************************************************************

tbx_ns_timeout_t *convert_epoch_time2net(tbx_ns_timeout_t *tm, apr_time_t epoch_time)
{
   apr_time_t dt = epoch_time - apr_time_now();
   if (apr_time_sec(dt) < 0) dt = 5;  //** Even if it's timed out give it a little time
   return(tbx_ns_timeout_set(tm, apr_time_sec(dt), 0));
}

//*****************************************************************
// send_cmd_result - Sends the command result back
//*****************************************************************

int send_cmd_result(ibp_task_t *task, int status)
{
   tbx_ns_t *ns = task->ns;
   char result[100];
   tbx_ns_timeout_t dt;
   int nbytes, nstr;

   if (ns == NULL) return(0);

   snprintf(result, sizeof(result), "%d \n", status);
   log_printf(10, "send_cmd_result(tid=" LU " ns=%d): %s", task->tid, tbx_ns_getid(ns), result);
   convert_epoch_time2net(&dt, task->cmd_timeout);
   nstr = strlen(result);
   nbytes = server_ns_write(ns, result, nstr, dt);
   if (nbytes != nstr) {
      log_printf(10, "send_cmd_result: Sent partial command!  sent %d bytes but should be %d\n", nbytes, nstr);
   }

   alog_append_cmd_result(task->myid, status);

   return(nbytes);
}

//*****************************************************************
//  lock_task - Lock the task mutex
//*****************************************************************

void lock_task(ibp_task_t *task)
{
  log_printf(15, "lock_task: ns=%d\n", tbx_ns_getid(task->ns));
  apr_thread_mutex_lock(task->lock);
}

//*****************************************************************
//  unlock_task - Lock the task mutex
//*****************************************************************

void unlock_task(ibp_task_t *task)
{
  log_printf(15, "unlock_task: ns=%d\n", tbx_ns_getid(task->ns));
  apr_thread_mutex_unlock(task->lock);
}


//*****************************************************************
// shutdown_request - Returns 1 if a shutdown request has been made
//*****************************************************************

int shutdown_request()
{
  int state;

   apr_thread_mutex_lock(shutdown_lock);
   state = shutdown_now;
   apr_thread_mutex_unlock(shutdown_lock);

   return(state);
}

//*****************************************************************
// init_starttime - Stores the depot start time
//*****************************************************************

void set_starttime()
{
  depot_start_time = apr_time_now();
}

//*****************************************************************
//  get_starttime - Returns the depot start time
//*****************************************************************

apr_time_t get_starttime()
{
  return(depot_start_time);
}

//*****************************************************************
//  print_uptime - Stores in the provided string the depot uptime
//*****************************************************************

void print_uptime(char *str, int n)
{
   char up_str[256], time_str[256];
   int days, hours, min, sec, d;

   apr_time_t dt = get_starttime();
   apr_ctime(time_str, dt);
   time_str[strlen(time_str)-1] = '\0';  //**Strip the CR

   dt = apr_time_now() - get_starttime();
   dt = apr_time_sec(dt);
   hours = dt / 3600;
   days = hours / 24;
   hours = hours - days * 24;

   d = dt % 3600; min = d / 60;
   sec = d - min * 60;

   snprintf(up_str, sizeof(up_str), "%d:%02d:%02d:%02d", days, hours, min, sec);

   snprintf(str, n, "Depot start time: %s\nUptime(d:h:m:s): %s\n", time_str, up_str);

   return;
}

//*****************************************************************************
//  print_config - Stores the config in the buffer.  Returned is the number of
//    bytes used or -1 if the buffer is too small.
//*****************************************************************************

int print_config(char *buffer, int *used, int nbytes, Config_t *cfg) 
{
  Server_t *server;
  int i, d, k;
  resource_list_iterator_t it;
  Resource_t *r;
  interface_t *iface;
  server = &(cfg->server);

  //Print the server settings first
  tbx_append_printf(buffer, used, nbytes, "[server]\n");
  tbx_append_printf(buffer, used, nbytes, "interfaces=");
  for (d=0; d<server->n_iface; d++) {
      iface = &(server->iface[d]);
      tbx_append_printf(buffer, used, nbytes, "%s:%d;", iface->hostname, iface->port);
  }
  tbx_append_printf(buffer, used, nbytes, "\n");

  tbx_append_printf(buffer, used, nbytes, "threads = %d\n", server->max_threads);
  tbx_append_printf(buffer, used, nbytes, "max_pending = %d\n", server->max_pending);
//  tbx_append_printf(buffer, used, nbytes, "max_connections = %d\n", server->max_connections);
  tbx_append_printf(buffer, used, nbytes, "min_idle = " TT "\n", apr_time_sec(server->min_idle));
  tbx_ns_timeout_get(server->timeout, &d, &k);
//log_printf(0, "print_timeout: s=%d ms=%d\n",d, k); 
  d = d * 1000 + k/1000;
  tbx_append_printf(buffer, used, nbytes, "max_network_wait_ms = %d\n", d);
  tbx_append_printf(buffer, used, nbytes, "password = %s\n", server->password);  
  tbx_append_printf(buffer, used, nbytes, "stats_size = %d\n", server->stats_size);
  tbx_append_printf(buffer, used, nbytes, "lazy_allocate = %d\n", server->lazy_allocate);
  tbx_append_printf(buffer, used, nbytes, "big_alloc_enable = %d\n", server->big_alloc_enable);
  tbx_append_printf(buffer, used, nbytes, "splice_enable = %d\n", server->splice_enable);
  tbx_append_printf(buffer, used, nbytes, "db_env_loc = %s\n", cfg->dbenv_loc);
  tbx_append_printf(buffer, used, nbytes, "db_mem = %d\n", cfg->db_mem);
  tbx_append_printf(buffer, used, nbytes, "log_file = %s\n", server->logfile);
  tbx_append_printf(buffer, used, nbytes, "tbx_log_level = %d\n", server->tbx_log_level);
  tbx_append_printf(buffer, used, nbytes, "return_cap_id = %d\n", server->return_cap_id);
  tbx_append_printf(buffer, used, nbytes, "backoff_scale = %lf\n", server->backoff_scale);
  tbx_append_printf(buffer, used, nbytes, "backoff_max = %d\n", server->backoff_max);
  d = server->log_maxsize / 1024 / 1024;
  tbx_append_printf(buffer, used, nbytes, "log_maxsize = %d\n", d);
  tbx_append_printf(buffer, used, nbytes, "debug_level = %d\n", server->debug_level);
  tbx_append_printf(buffer, used, nbytes, "timestamp_interval = %d\n", server->timestamp_interval);
  tbx_append_printf(buffer, used, nbytes, "activity_file = %s\n", server->alog_name);
  d = server->alog_max_size / 1024 / 1024;
  tbx_append_printf(buffer, used, nbytes, "activity_maxsize = %d\n", d);
  tbx_append_printf(buffer, used, nbytes, "activity_max_history = %d\n", server->alog_max_history);
  tbx_append_printf(buffer, used, nbytes, "activity_host = %s\n", server->alog_host);
  tbx_append_printf(buffer, used, nbytes, "activity_port = %d\n", server->alog_port);
  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "force_resource_rebuild = %d\n", cfg->force_resource_rebuild);
  tbx_append_printf(buffer, used, nbytes, "truncate_duration = %d\n", cfg->truncate_expiration);
  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "rid_check_interval = %d\n", server->rid_check_interval);
  tbx_append_printf(buffer, used, nbytes, "eject_timoeut = %d\n", server->eject_timeout);
  tbx_append_printf(buffer, used, nbytes, "rid_log = %s\n", server->rid_log);
  tbx_append_printf(buffer, used, nbytes, "rid_eject_script = %s\n", server->rid_eject_script);
  tbx_append_printf(buffer, used, nbytes, "rid_eject_tmp_path = %s\n", server->rid_eject_tmp_path);
  tbx_append_printf(buffer, used, nbytes, "\n");
  d = (cfg->soft_fail == -1) ? 0 : 1;
  tbx_append_printf(buffer, used, nbytes, "soft_fail = %d\n", d);
  tbx_append_printf(buffer, used, nbytes, "\n");

  //Cycle through each resource
  i = resource_list_n_used(cfg->rl);
  tbx_append_printf(buffer, used, nbytes, "# Total Resources : %d\n\n", i);
  it = resource_list_iterator(cfg->rl);
  while ((r = resource_list_iterator_next(cfg->rl, &it)) != NULL) {
      print_resource(buffer, used, nbytes, r);
  }
  resource_list_iterator_destroy(cfg->rl, &it);

  i = print_command_config(buffer, used, nbytes);  //** Print the rest of the command info

  return(i);
}

//*****************************************************************
// read_command - Reads a command from the stream
//*****************************************************************

int read_command(ibp_task_t *task)
{
   tbx_ns_t *ns = task->ns;
   Cmd_state_t *cmd = &(task->cmd);

   int bufsize = 200*1024;
   char buffer[bufsize];
   char *bstate;
   int  nbytes, status, offset, count, close_request;
   int err, fin;
   apr_time_t endtime;
   command_t *mycmd;

//   apr_thread_mutex_unlock(task->lock);

   log_printf(10, "read_command: ns=%d initial tid=" LU " START--------------------------\n", tbx_ns_getid(task->ns), task->tid);

   cmd->state = CMD_STATE_CMD;
   task->child = NULL;
   task->parent = NULL;

   clear_stat(&(task->stat));
   memset(task->stat.address, 0, sizeof(task->stat.address));
   if (task->stat.address != 0) {
     strncpy(task->stat.address, tbx_ns_peer_address_get(task->ns), sizeof(task->stat.address));
   }

   tbx_ns_timeout_t dt;
   tbx_ns_timeout_set(&dt, 1, 0);
   offset = 0;
   count = sizeof(buffer);
   endtime = apr_time_now() + global_config->server.min_idle; //** Wait for the min_idle time
   close_request = 0;
   do {
     nbytes = server_ns_readline_raw(ns, &(buffer[offset]), count - offset, dt, &status);
     offset = offset + nbytes;
     if ((request_task_close() == 1) && (offset <= 0)) close_request = 1;
log_printf(15, "read_command: ns=%d nbytes=%d offset=%d status=%d sizeof(buffer)=" LU " close_req=%d\n", tbx_ns_getid(ns), nbytes, offset, status, sizeof(buffer), close_request);
   } while ((offset < (count-1)) && (status == 0) && (apr_time_now() <= endtime) && (close_request == 0));
   nbytes = offset;

   log_printf(10, "read_command: ns=%d tid=" LU " Command: %s\n", tbx_ns_getid(task->ns), task->tid, buffer);
   log_printf(10, "read_command: ns=%d total_bytes=%d status=%d\n", tbx_ns_getid(ns), nbytes, status);
   tbx_log_flush();

   task->cmd_timeout = apr_time_now() + apr_time_make(1, 0); //** Set a default timeout for any errors

   if (status == 0) {  //** Not enough buffer space or nothing to do
     if (nbytes != 0) {  //** Out of buffer space
        send_cmd_result(task, IBP_E_BAD_FORMAT);
     }
     cmd->command = IBP_NOP;
     cmd->state = CMD_STATE_FINISHED;
     log_printf(10, "read_command: end of routine ns=%d tid=" LU " status=%d nbytes=%d\n", tbx_ns_getid(task->ns), task->tid, status, nbytes);
     return(-1);
   } else if (status == -1) {
     log_printf(10, "read_command: end of routine ns=%d tid=" LU " status=%d nbytes=%d\n", tbx_ns_getid(task->ns), task->tid, status, nbytes);
     return(-1);
   }

   //** Looks like we have actual data so inc the tid **
   apr_thread_mutex_lock(task_count_lock);
   task->tid = task_count;
   task_count++;
   if (task_count > 1000000000) task_count = 0;
   apr_thread_mutex_unlock(task_count_lock);

   cmd->version = -1;  cmd->command = -1;
   sscanf(tbx_stk_string_token(buffer, " ", &bstate, &fin), "%d", &(cmd->version));
   log_printf(10, "read_command: version=%d\n", cmd->version); tbx_log_flush();
   sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), "%d", &(cmd->command));

   log_printf(10, "read_command: ns=%d version = %d tid=" LU "* Command = %d\n", tbx_ns_getid(task->ns), cmd->version, task->tid, cmd->command);
tbx_log_flush();

   if (cmd->version == -1) {
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      cmd->state = CMD_STATE_FINISHED;
      return(-1);
  }

   //** Rest of arguments depends on the command **
   err = 0;
   if ((cmd->command<0) || (cmd->command > COMMAND_TABLE_MAX)) {
      log_printf(10, "read_command:  Unknown command! ns=%d\n", tbx_ns_getid(task->ns));
      send_cmd_result(task, IBP_E_UNKNOWN_FUNCTION);
      cmd->state = CMD_STATE_FINISHED;
      err = -1;
   } else {
      mycmd = &(global_config->command[cmd->command]);
      if (mycmd->read != NULL) err = mycmd->read(task, &bstate);

      if (task->command_acl[cmd->command] == 0) {  //** Not allowed so err out
         log_printf(10, "read_command:  Can't execute command due to ACL restriction! ns=%d cmd=%d\n", tbx_ns_getid(task->ns), cmd->command);
         send_cmd_result(task, IBP_E_UNKNOWN_FUNCTION);
         cmd->state = CMD_STATE_FINISHED;
         err = -1;
      }
   }

  log_printf(10, "read_command: end of routine ns=%d tid=" LU " err = %d\n", tbx_ns_getid(task->ns), task->tid, err);

  return(err);

}

//*****************************************************************
// handle_command - Main worker thread to handle an IBP commands
//*****************************************************************

int handle_task(ibp_task_t *task)
{
   tbx_ns_t *ns = task->ns;
   Cmd_state_t *cmd = &(task->cmd);
   uint64_t mytid = task->tid;
   command_t *mycmd;
   int err = 0;

   cmd->state = CMD_STATE_CMD;

   apr_time_t tt = apr_time_now();
   log_printf(10, "handle_task: ns=%d ***START*** tid=" LU " Got a connection at " TT "\n", tbx_ns_getid(ns), task->tid, apr_time_now());

   //**** Start processing the command *****
   err = 0;
   if ((cmd->command<0) || (cmd->command > COMMAND_TABLE_MAX)) {
      log_printf(10, "handle_command:  Unknown command! ns=%d\n", tbx_ns_getid(task->ns));
      send_cmd_result(task, IBP_E_BAD_FORMAT);
      cmd->state = CMD_STATE_FINISHED;
      err = -1;
   } else {
      mycmd = &(global_config->command[cmd->command]);
      if (mycmd->execute != NULL) err = mycmd->execute(task);
   }

   log_printf(10, "handle_task: tid=" LU " After handle_command " TT " ns=%d\n", task->tid, apr_time_now(),tbx_ns_getid(task->ns));


   tt = apr_time_now();
   log_printf(10, "handle_task: ns=%d err=%d ***END*** tid=" LU " Completed processing at " TT "\n", tbx_ns_getid(ns), err, mytid, tt);

   return(err);
}

//*****************************************************************
// worker_task - Handles the task thread
//*****************************************************************

void *worker_task(apr_thread_t *ath, void *arg)
{
   Thread_task_t *th = (Thread_task_t *)arg;
   ibp_task_t task;
   int closed;
   int status;
   int myid;
   int ncommands;
   apr_status_t retval;
   tbx_ns_timeout_t start_read, start_handle, end_time, dt_read, dt_handle, dt_total;

   log_printf(10, "worker_task: ns=%d ***START*** Got a connection at " TT "\n", tbx_ns_getid(th->ns), apr_time_now());


   task.tid = 0;
   task.ns = th->ns;
   task.net = global_network;
   closed = 0;
   ncommands = 0;

   if (th->reject_connection > 0) {  //** Rejecting the connection
//char buffer[1024];
//int status;
//tbx_ns_timeout_t dt;
//tbx_ns_timeout_set(&dt,1,0);
//server_ns_readline_raw(task.ns, buffer, sizeof(buffer), dt, &status);

      reject_task(task.ns, 0);     //** Reject the connections
      tbx_ns_close(task.ns);  //** And close it

      //** Push myself on the completed task
      apr_thread_mutex_lock(taskmgr.lock);
      tbx_stack_push(taskmgr.completed, th);

      log_printf(10, "worker_task: ns=%d  ***END*** REJECTING at " TT "\n", tbx_ns_getid(th->ns), apr_time_now());

      apr_thread_mutex_unlock(taskmgr.lock);

      //** Lastly exit
      retval = 0;
      apr_thread_exit(th->thread, retval);
   }

   //** Store the address for use in the time stamps
   task.ipadd.atype = AF_INET;
   ipdecstr2address(tbx_ns_peer_address_get(task.ns), task.ipadd.ip);

   generate_command_acl(tbx_ns_peer_address_get(task.ns), task.command_acl);

   myid = reserve_thread_slot();
   task.myid = myid;
   log_printf(0, "worker_task: open_thread: myid=%d ns=%d\n", myid, tbx_ns_getid(task.ns));

   alog_append_thread_open(myid, tbx_ns_getid(task.ns), task.ipadd.atype, task.ipadd.ip);


   start_read = apr_time_now();
   while ((shutdown_request() == 0) && (closed == 0)) {
      tbx_ns_chksum_read_clear(task.ns);
      tbx_ns_chksum_write_clear(task.ns);

      status = read_command(&task);
      if (status == 0) {
          start_handle = apr_time_now();
          ncommands++;
          closed = handle_task(&task);
          end_time = apr_time_now();

          dt_read = start_handle - start_read;
          dt_handle = end_time - start_handle;
          dt_total = end_time - start_read;

          log_printf(10, "worker_task: ns=%d myid=%d command=%d start_time=" TT " end_time=" TT " dt_read=" TT " dt_handle=" TT " dt_total=" TT "\n", 
              tbx_ns_getid(th->ns), myid, task.cmd.command, start_read, end_time, dt_read, dt_handle, dt_total);
          start_read = apr_time_now();
      } else if (status == -1) {
          closed = 1;
      }

      if (request_task_close() == 1) closed = 1;
//      if ((apr_time_now() - start_read) > global_config->server.min_idle) closed = 1;
      if ((apr_time_now() - start_read) > global_config->server.min_idle) {
         closed = 1;
         dt_read = (apr_time_now() - start_read) / APR_USEC_PER_SEC;
         log_printf(10, "worker_task: ns=%d myid=%d MIN_IDLE=" TT " exiting at " TT " start_read=" TT " dt=" TT "sec\n", tbx_ns_getid(th->ns), myid, global_config->server.min_idle, apr_time_now(), start_read, dt_read);
      }
   }

   alog_append_thread_close(myid, ncommands);

   release_thread_slot(myid);

   //** Notify the client why I'm closing.  IF already closed this just returns
   reject_close(task.ns);

   tbx_ns_close(task.ns);
   release_task(th);

   log_printf(10, "worker_task: ns=%d myid=%d ***END*** exiting at " TT "\n", tbx_ns_getid(th->ns), myid, apr_time_now());

   retval = 0;
   apr_thread_exit(th->thread, retval);

   return(NULL);
}


//*****************************************************************
//  currently_running_tasks - Returns the number of tasks currently 
//         running
//*****************************************************************

int currently_running_tasks()
{
  int n;

  apr_thread_mutex_lock(taskmgr.lock);
  n = taskmgr.curr_threads;
  apr_thread_mutex_unlock(taskmgr.lock);

  return(n);  
}

//*****************************************************************
// reject_count - Returns the rejected socket counts
//*****************************************************************

void reject_count(int *curr, uint64_t *total)
{
  apr_thread_mutex_lock(taskmgr.lock);
  *curr = taskmgr.reject_count;
  *total = taskmgr.reject_total;
  apr_thread_mutex_unlock(taskmgr.lock);
}

//*****************************************************************
//  to_many_connections - Returns 1 if to many connections are open
//*****************************************************************

int to_many_connections()
{
  int n;

  apr_thread_mutex_lock(taskmgr.lock);
  if (taskmgr.curr_threads >= taskmgr.max_threads) {
     n = 1;
  } else {
     n = 0;
     taskmgr.reject_count = 0;
  }
  apr_thread_mutex_unlock(taskmgr.lock);

  return(n);  
}

//*****************************************************************
// spawn_new_task - Spawns a new task
//*****************************************************************

void spawn_new_task(tbx_ns_t *ns, int reject_connection)
{
  Thread_task_t *t = (Thread_task_t *)malloc(sizeof(Thread_task_t));

  t->ns = ns;

  apr_pool_create(&(t->pool), NULL); 

  t->attr = NULL;

//** if needed set the default stack size **
  apr_size_t stacksize = 4*1024*1024;
  apr_threadattr_create(&(t->attr), t->pool);
  apr_threadattr_stacksize_set(t->attr, stacksize);
  t->reject_connection = reject_connection;
  log_printf(15, "spawn_new_task: default stacksize=" ST "\n", stacksize);

  //** Increment the task count 1st
  if (reject_connection == 0) {
     apr_thread_mutex_lock(taskmgr.lock);  
     taskmgr.curr_threads++;
     apr_thread_mutex_unlock(taskmgr.lock);  
  }

  //** then launch the thread
  apr_thread_create(&(t->thread), t->attr, worker_task, (void *)t, t->pool);
}

//*****************************************************************
// release_task - Releases the current task back for respawning
//*****************************************************************

void release_task(Thread_task_t *t)
{
  apr_thread_mutex_lock(taskmgr.lock);

  taskmgr.curr_threads--;
  tbx_stack_push(taskmgr.completed, t);
  
  apr_thread_mutex_unlock(taskmgr.lock);
  
}

//*****************************************************************
// request_task_close - checks to see if the task should be closed
//*****************************************************************

int request_task_close()
{
  int result;

  result = 0;

  apr_thread_mutex_lock(taskmgr.lock);

  if (taskmgr.request_thread > 0) {
     if (taskmgr.curr_threads >= taskmgr.max_threads) {
        taskmgr.request_thread--;
        result = 1;
     } else {
        taskmgr.request_thread = 0;
     }         
  }

  if (result == 1) apr_thread_cond_signal(taskmgr.cond);

  log_printf(15, "request_task_close: result=%d ncurr=%d\n", result, taskmgr.curr_threads);

  apr_thread_mutex_unlock(taskmgr.lock);

  return(result);
}

//*****************************************************************
// join_completed - Does a join on any completed task/threads
//*****************************************************************

void join_completed()
{
  Thread_task_t *t;
  apr_status_t err;

  apr_thread_mutex_lock(taskmgr.lock);

  while ((t=(Thread_task_t *)tbx_stack_pop(taskmgr.completed)) != NULL) {
     apr_thread_join(&err, t->thread);
     tbx_ns_destroy(t->ns);
//handled by the pool free     if (t->attr != NULL) apr_threadattr_destroy(t->attr);
     apr_pool_destroy(t->pool);
     free(t);
  }
  
  apr_thread_mutex_unlock(taskmgr.lock);
}

//*****************************************************************
// signal_taskmgr - Signals the taskmgr to wakeup if needed
//*****************************************************************

void signal_taskmgr()
{
  apr_thread_mutex_lock(taskmgr.lock);
  apr_thread_cond_signal(taskmgr.cond);
  apr_thread_mutex_unlock(taskmgr.lock);
}

//*****************************************************************
// wait_for_free_task - Waits until a free task slot is available
//*****************************************************************

void wait_for_free_task()
{
  apr_thread_mutex_lock(taskmgr.lock);
 
  if (taskmgr.curr_threads >= taskmgr.max_threads) {
     taskmgr.request_thread = 1;
     log_printf(15, "wait_for_free_task: Before cond_wait time=" TT "\n", apr_time_now());
     apr_thread_cond_wait(taskmgr.cond, taskmgr.lock);
     log_printf(15, "wait_for_free_task: After cond_wait time=" TT "\n", apr_time_now());
     taskmgr.request_thread = 0;
  }


  apr_thread_mutex_unlock(taskmgr.lock);
}

//*****************************************************************
// reject_close - Closes the netstream but sends a reject error
//*****************************************************************

void reject_close(tbx_ns_t *ns)
{
  double dt;
  apr_time_t end_time;
  unsigned char r;
  char buffer[1024];

  //** Add some randomness to the time
  r = 0;
  tbx_random_get_bytes(&r, 1);
  dt = r;
  dt = dt / 1024.0;  //** Up to 0.25 secs

  apr_thread_mutex_lock(taskmgr.lock); 
  
  dt = dt + global_config->server.backoff_scale * taskmgr.reject_count;
  if (dt > global_config->server.backoff_max) {
     dt = global_config->server.backoff_max;
  }
  log_printf(15, "reject_close: ns=%d  curr_threads=%d request_thread=%d reject_count=%d dt=%lf\n", tbx_ns_getid(ns), taskmgr.curr_threads, taskmgr.request_thread, taskmgr.reject_count, dt);
  apr_thread_mutex_unlock(taskmgr.lock);

  //** Send the result back
  snprintf(buffer, sizeof(buffer), "%d %lf\n", IBP_E_OUT_OF_SOCKETS, dt);
  end_time = apr_time_now() + apr_time_make(5, 0);
  server_ns_write_block(ns, end_time, buffer, strlen(buffer));
}

//*****************************************************************
// reject_task - Rejects a task due to load
//*****************************************************************

void reject_task(tbx_ns_t *ns, int destroy_ns)
{
  double dt;
  apr_time_t end_time;
  unsigned char r;
  char buffer[1024];

  //** Add some randomness to the time
  tbx_random_get_bytes(&r, 1);
  dt = r;
  dt = dt / 256.0;


  apr_thread_mutex_lock(taskmgr.lock); 
  if (taskmgr.curr_threads >= taskmgr.max_threads) {
     taskmgr.request_thread++;
  }
  taskmgr.reject_count++;
  taskmgr.reject_total++;
  dt = dt + global_config->server.backoff_scale * taskmgr.reject_count;
  if (dt > global_config->server.backoff_max) {
     dt = global_config->server.backoff_max;
  }
  log_printf(15, "reject_task: ns=%d curr_threads=%d request_thread=%d reject_count=%d dt=%lf\n", tbx_ns_getid(ns), taskmgr.curr_threads, taskmgr.request_thread, taskmgr.reject_count, dt);
  apr_thread_mutex_unlock(taskmgr.lock);

  //** Send the result back
  snprintf(buffer, sizeof(buffer), "%d %lf\n", IBP_E_OUT_OF_SOCKETS, dt);
  end_time = apr_time_now() + apr_time_make(dt, 0);
  server_ns_write_block(ns, end_time, buffer, strlen(buffer));

  //**Close and destroy the socket
  tbx_ns_close(ns);
  if (destroy_ns == 1) tbx_ns_destroy(ns);
}

//*****************************************************************
// wait_all_tasks - Waits until all tasks have completed
//*****************************************************************

void wait_all_tasks()
{
  apr_time_t t;
  apr_thread_mutex_lock(taskmgr.lock);
 
  while (taskmgr.curr_threads > 0) {
    t = apr_time_make(1, 0);    //wait for at least a second
    taskmgr.request_thread = 1;
    apr_thread_cond_timedwait(taskmgr.cond, taskmgr.lock, t);
  }   

  apr_thread_mutex_unlock(taskmgr.lock);
}

//*****************************************************************
// init_tasks  -Inititalizes the task management
//*****************************************************************

void init_tasks()
{
  taskmgr.max_threads = global_config->server.max_threads;
  taskmgr.curr_threads = 0;
  taskmgr.request_thread = 0;
  taskmgr.reject_count = 0;
  taskmgr.reject_total = 0;
  taskmgr.completed = tbx_stack_new();

  apr_thread_mutex_create(&task_count_lock, APR_THREAD_MUTEX_DEFAULT, global_pool);
  apr_thread_mutex_create(&(taskmgr.lock), APR_THREAD_MUTEX_DEFAULT, global_pool);
  apr_thread_cond_create(&(taskmgr.cond), global_pool);  
}

//*****************************************************************
// close_tasks - Closes the task management
//*****************************************************************

void close_tasks()
{
  wait_all_tasks();

  join_completed();

  tbx_stack_free(taskmgr.completed, 0);

  apr_thread_mutex_destroy(taskmgr.lock);
  apr_thread_cond_destroy(taskmgr.cond);
}

//*****************************************************************
// server_loop - Main processing loop
//*****************************************************************

void server_loop(Config_t *config)
{
  tbx_network_t *network;
  tbx_ns_t *ns, *bns;
  apr_time_t tt;
  apr_time_t print_time;
  int i;
  char current_time[128];

  print_time = apr_time_now();

  log_printf(10, "server_loop: Start.....\n");

  task_count = 0;

log_printf(0, "shutdown_now=%d\n", shutdown_now);

  //*** Init the networking ***
  network = tbx_network_new();

  if (network == NULL) return;
  global_network = network;

  for (i=0; i<config->server.n_iface; i++) {
     bns = tbx_ns_new();
     tbx_ns_sock_config(bns, 0);
     if (tbx_network_bind(network, bns, config->server.iface[i].hostname, config->server.iface[i].port, config->server.max_pending) != 0) {
        log_printf(0, "ERROR binding iface[%d]=%s:%d so shutting down!\n", i, config->server.iface[i].hostname, config->server.iface[i].port);
        shutdown_now =1;  //** Trigger a shutdown
     }
  }

  init_tasks();

  //*** Main processing loop ***
  while (shutdown_request() == 0) {
     tt = apr_time_now();
     log_printf(10, "server_loop: Waiting for a connection time= " TT "\n", tt);
     if (tbx_network_wait_for_connection(network, config->server.timeout_secs) > 0) { // ** got a new connection
        log_printf(10, "server_loop: Got a connection request or timed out!  time=" TT "\n", apr_time_now());
//        wait_for_free_task();
        ns = tbx_ns_new();
        if (tbx_network_accept_pending_connection(network, ns) == 0) {
           spawn_new_task(ns, to_many_connections());
        } else {
           tbx_ns_destroy(ns);
        }
     }

     if (apr_time_now() > print_time) {   //** Print the time stamp
        print_time = apr_time_now();
        apr_ctime(current_time, print_time);
        log_printf(0, "MARK: " TT " ------> %s", print_time, current_time);
        print_time += config->server.timestamp_interval;
     }

     join_completed();   //** reap any completed threads
  }

  log_printf(15, "Exited server_loop\n"); tbx_log_flush();

  tbx_network_close(network);  //** Stop accepting connections

  close_tasks();

  log_printf(15, "before tbx_network_close\n"); tbx_log_flush();

  //*** Shutdown the networking ***
  tbx_network_destroy(network);

  log_printf(10, "server_loop: Stop.....\n");

  return;
}

