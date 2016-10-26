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

#ifndef _IBP_SERVER_H_
#define _IBP_SERVER_H_

#include <ibp-server/visibility.h>
#include <sys/types.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <apr_time.h>
#include <tbx/append_printf.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/network.h>
#include "resource_list.h"
#include "ibp_task.h"
#include "resource.h"
#include "subnet.h"
#include <tbx/string_token.h>
#include "phoebus.h"
#include "ibp_time.h"

#define IBP_ST_STATS  4   //** Return stats
#define IBP_ST_VERSION 5  //** Return the version string

#define DEFAULT_PASSWORD "IBPch@ngem3"
#define IBP_PORT 6714

//** Private internal commands
#define INTERNAL_RID_SET_MODE 90
#define INTERNAL_RID_MOUNT    91
#define INTERNAL_RID_UMOUNT   92
#define INTERNAL_GET_CORRUPT  93
#define INTERNAL_GET_CONFIG   94
#define INTERNAL_RESCAN       95
#define INTERNAL_UNDELETE     96
#define INTERNAL_EXPIRE_LIST  97
#define INTERNAL_DATE_FREE    98
#define INTERNAL_GET_ALLOC    99
#define INTERNAL_SEND        100

//*** Internal constant to represent the key is the osd_id
#define INTERNAL_ID 4

typedef struct {  //** bind ports
  char *hostname;
  int port;
} interface_t;

typedef struct {    //*** forms the fn table for a depot command
   int command;                             //** Command value
   int used;                                //** Determines if the command is used
   char name[64];                           //** Command name
   subnet_list_t *subnet;                   //** Valid subnets for command execution
   char *acl;                               //** String version of the ACLs
   void (*load_config)(tbx_inip_file_t *keyfile);  //** Used to load the config file
   void (*init)(void);                      //** Perform final initialization. Called after depot init
   void (*destroy)(void);                   //** Cleanup.  Called before depot destroy
   int (*print)(char *buffer, int *used, int nbytes);  //** Print command cofnig to fd
   int (*read)(ibp_task_t *task, char **bstate);  //** Reads the command from the socket
   int (*execute)(ibp_task_t *task);       //** The actual command
}  command_t;

#define COMMAND_TABLE_MAX 100   //** Size of static command table

typedef struct {       // Structure containg the overall server config
   interface_t *iface;   //Interfaces listening on
   int n_iface;          //Number of bound interfaces
   int port;             //Default Port to listen on
   int max_threads;      //Max number of threads for pool
   int max_pending;      //Max pending connections
   int timestamp_interval;  //Log timestamp interval in sec
   int stats_size;       //Max size of statistics to keep
   tbx_ns_timeout_t timeout;  //Max waiting time on a network connection
   int timeout_secs;        //Same as above just in simple seconds and not a struct
   apr_time_t min_idle;   //Min time of inactivity for a connection before it's *possibly* considered dead
   double backoff_scale;  //Backoff multiplier
   int backoff_max;       //MAx backoff time in sec
   int big_alloc_enable;  //Enable 2G+ allocations
   int splice_enable;     //Enable use of splice if available
   int lazy_allocate;    //If 1 don't create the physical file just make the DB entry
   char *logfile;        //Log file
   int log_maxsize;      //Max size of logs to keep before rolling over
   int  tbx_log_level;       //Log level to control output
   int  log_overwrite;   //overwrite log file instead of append(default)
   int  debug_level;     //Debug level if compiled with debugging turned on
   char *debugfile;      //Debug output file (default stdout)
   int   alog_max_size;      //Max size for the activity log
   char *alog_name;      //Activity file (default ibp_activity.log)
   char *alog_host;      //** Host to send alog info
   int   alog_max_history;     //** How many alog files to keep before dropping them
   int   alog_port;            //** alog host's port to use
   int   return_cap_id;    //** Returns the cap id in the capability if set
   int   rid_check_interval;  //** DRive check interval
   int   eject_timeout;   //** How long to wait for RID check failures before ejecting a drive
   char *rid_log;         //Where to store the running RID changes
   char *password;        //Depot Password for status change commands
   char *default_acl;     //Default command ACLs
   char *rid_eject_script; //Script to run when ejecting drives that fail a health check
   char *rid_eject_tmp_path; //Location to create the temporary files for the eject script
} Server_t;

typedef struct {      //Main config structure
   char *config_file;    // Configuration file
   Server_t server;     // Server config
   char *dbenv_loc;     // Location of DB enviroment
   int  db_mem;      // DB envirment memory usage in MB
   DB_env_t  *dbenv;     // Container for DB environment
   int force_resource_rebuild; // Force rebuilding of all resources
   int truncate_expiration;    // Force existing allocs duration to be the RID max.  Only used in rebuild!
   int soft_fail;       // defaults to -1 for all errors. Only used for commands that mey be recoverable 
   Resource_list_t *rl; // Searchable list of resources
   command_t command[COMMAND_TABLE_MAX+1];  //** List of commands
} Config_t;


typedef struct {     //** Thread data strcuture
  apr_pool_t *pool;
  apr_thread_t *thread;
  apr_threadattr_t *attr;
  tbx_ns_t *ns;
  int reject_connection;
} Thread_task_t;


//**************Global control Variables*****************
extern IBPS_API apr_pool_t *global_pool;
extern IBPS_API apr_thread_mutex_t *shutdown_lock;
extern IBPS_API apr_thread_cond_t *shutdown_cond;
extern IBPS_API int shutdown_now;
extern IBPS_API Config_t *global_config;
extern IBPS_API ibp_task_t *global_task;
extern IBPS_API tbx_network_t *global_network;

//************** Function definitions *******************

//*** Function in install_commands.c ***
IBPS_API void install_commands(tbx_inip_file_t *kf);

//*** functions in commands.c ***
IBPS_API void generate_command_acl(char *peer_name, int *acl);
IBPS_API void add_command(int cmd, const char *cmd_keyword, tbx_inip_file_t *kf,
   void (*load_config)(tbx_inip_file_t *keyfile), 
   void (*init)(void),
   void (*destroy)(void),
   int (*print)(char *buffer, int *used, int nbytes),
   int (*read)(ibp_task_t *task, char **bstate),
   int (*execute)(ibp_task_t *task) );
IBPS_API int print_command_config(char *buffer, int *used, int nbytes);
IBPS_API void initialize_commands();
IBPS_API void destroy_commands();

//*** Functions in server_lib.c ****
IBPS_API int server_ns_readline(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt);
IBPS_API int server_ns_write(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt);
IBPS_API int server_ns_read(tbx_ns_t *ns, char *buffer, int bsize, tbx_ns_timeout_t dt);
IBPS_API int server_ns_write_block(tbx_ns_t *ns, apr_time_t end_time, char *buffer, int bsize);
IBPS_API int server_ns_read_block(tbx_ns_t *ns, apr_time_t end_time, char *buffer, int bsize);
IBPS_API int print_config(char *buffer, int *used, int nbytes, Config_t *cfg);
IBPS_API void set_starttime();
IBPS_API apr_time_t get_starttime();
IBPS_API void print_uptime(char *str, int n);
IBPS_API void lock_task(ibp_task_t *task);
IBPS_API void unlock_task(ibp_task_t *task);
IBPS_API void server_loop(Config_t *config);
IBPS_API tbx_ns_timeout_t *convert_epoch_time2net(tbx_ns_timeout_t *tm, apr_time_t epoch_time);
IBPS_API int send_cmd_result(ibp_task_t *task, int status);
IBPS_API int get_command_timeout(ibp_task_t *task, char **bstate);
IBPS_API int read_command(ibp_task_t *task);
IBPS_API Cmd_state_t *new_command();
IBPS_API void free_command(Cmd_state_t *cmd);
IBPS_API void *worker_task(apr_thread_t *ath, void *arg);
IBPS_API void server_loop(Config_t *config);
IBPS_API void signal_shutdown(int sig);
IBPS_API int to_many_connections();
IBPS_API void reject_close(tbx_ns_t *ns);
IBPS_API void reject_task(tbx_ns_t *ns, int destroy_ns);
IBPS_API int request_task_close();
IBPS_API int currently_running_tasks();
IBPS_API void reject_count(int *curr, uint64_t *total);
IBPS_API void release_task(Thread_task_t *t);
IBPS_API void wait_all_tasks();
IBPS_API void signal_taskmgr();

//*** Functions in parse_commands.c ***
IBPS_API int read_rename(ibp_task_t *task, char **bstate);
IBPS_API int read_allocate(ibp_task_t *task, char **bstate);
IBPS_API int read_validate_get_chksum(ibp_task_t *task, char **bstate);
IBPS_API int read_merge_allocate(ibp_task_t *task, char **bstate);
IBPS_API int read_alias_allocate(ibp_task_t *task, char **bstate);
IBPS_API int read_status(ibp_task_t *task, char **bstate);
IBPS_API int read_manage(ibp_task_t *task, char **bstate);
IBPS_API int read_write(ibp_task_t *task, char **bstate);
IBPS_API int read_read(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_get_alloc(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_get_corrupt(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_get_config(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_date_free(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_expire_list(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_undelete(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_rescan(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_mount(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_umount(ibp_task_t *task, char **bstate);
IBPS_API int read_internal_set_mode(ibp_task_t *task, char **bstate);

//*** Functions in handle_commands.c ***
IBPS_API int handle_allocate(ibp_task_t *task);
IBPS_API int handle_validate_chksum(ibp_task_t *task);
IBPS_API int handle_get_chksum(ibp_task_t *task);
IBPS_API int handle_merge(ibp_task_t *task);
IBPS_API int handle_alias_allocate(ibp_task_t *task);
IBPS_API int handle_rename(ibp_task_t *task);
IBPS_API int handle_status(ibp_task_t *task);
IBPS_API int handle_manage(ibp_task_t *task);
IBPS_API int handle_write(ibp_task_t *task);
IBPS_API int handle_read(ibp_task_t *task);
IBPS_API int handle_copy(ibp_task_t *task);
IBPS_API int handle_transfer(ibp_task_t *task, osd_id_t rpid, tbx_ns_t *ns, const char *key, const char *typekey);
IBPS_API int handle_internal_get_alloc(ibp_task_t *task);
IBPS_API int handle_internal_get_corrupt(ibp_task_t *task);
IBPS_API int handle_internal_get_config(ibp_task_t *task);
IBPS_API int handle_internal_date_free(ibp_task_t *task);
IBPS_API int handle_internal_expire_list(ibp_task_t *task);
IBPS_API int handle_internal_undelete(ibp_task_t *task);
IBPS_API int handle_internal_rescan(ibp_task_t *task);
IBPS_API int handle_internal_mount(ibp_task_t *task);
IBPS_API int handle_internal_umount(ibp_task_t *task);
IBPS_API int handle_internal_set_mode(ibp_task_t *task);

//*** Functions in buffer_transfer.c ***
IBPS_API void iovec_start(ibp_iovec_t *iovec, int *index, ibp_off_t *ioff, ibp_off_t *ileft);
IBPS_API void iovec_single(ibp_iovec_t *iovec, ibp_off_t off, ibp_off_t len);
IBPS_API int read_from_disk(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res);
IBPS_API int write_to_disk(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res);
IBPS_API int disk_to_disk_copy(Resource_t *src_res, osd_id_t src_id, ibp_off_t src_offset,
                      Resource_t *dest_res, osd_id_t dest_id, ibp_off_t dest_offset, ibp_off_t len, apr_time_t end_time);

//*** Functions in transfer_stats.c ***
IBPS_API void init_stats(int n);
IBPS_API void free_stats();
IBPS_API void clear_stat(Transfer_stat_t *s);
IBPS_API void get_transfer_stats(uint64_t *rbytes, uint64_t *wbytes, uint64_t *copybytes);
IBPS_API void add_stat(Transfer_stat_t *s);
IBPS_API int send_stats(tbx_ns_t *ns, ibp_time_t start_time, tbx_ns_timeout_t dt);

//*** Functions in test_alloc.c ***
IBPS_API void test_alloc();

//*** Functions in thread_slots.c ****
IBPS_API void release_thread_slot(int slot);
IBPS_API int reserve_thread_slot();
IBPS_API void destroy_thread_slots();
IBPS_API void init_thread_slots(int size);

#endif

