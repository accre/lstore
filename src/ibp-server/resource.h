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

//*******************************************************************
//*******************************************************************

#ifndef _RESOURCE_H_
#define _RESOURCE_H_

#include <ibp-server/visibility.h>
#include "rid.h"
#include "db_resource.h"
#include "osd.h"
#include <tbx/chksum.h>
#include "ibp_time.h"
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_pools.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>

#define _RESOURCE_VERSION 100000

//**** Mode states
#define RES_MODE_WRITE  1
#define RES_MODE_READ   2
#define RES_MODE_MANAGE 4

   //** Reserve modes.  Bit masks
#define RES_RESERVE_FALLOCATE 1
#define RES_RESERVE_BLANK     2

#define RES_EXPIRE_REMOVE OSD_EXPIRE_ID
#define RES_DELETE_REMOVE OSD_DELETE_ID
#define RES_PHYSICAL_REMOVE OSD_ID

#define RES_DELETE_INDEX 0
#define RES_EXPIRE_INDEX 1

//*** Types of resources ***
#define RES_TYPE_UNKNOWN 0
#define RES_TYPE_DIR     1
#define RES_TYPE_MAX     2

#define DEVICE_UNKNOWN "unknown"
#define DEVICE_DIR   "dir"

extern const char *_res_types[];

#define ALLOC_TOTAL 2         //Used for determing the total depot size

//typedef uint64_t ibp_off_t;    //Resource size

typedef struct {       //Resource structure
   char *keygroup;         //Keyfile group name
   char *name;             //Descriptive resource name
   char *data_pdev;        //Data physical /dev entry
   rid_t rid;              //Unique resource ID
   int   max_duration;     //MAx duration for an allocation in SECONDS
   int   lazy_allocate;    //If 1 then the actual file is created with the allocation.  Otherwise just the DB entry
   int   enable_chksum;    //If 1 then default chksum is enabled
   tbx_chksum_t chksum;    //Type of default chksum
   ibp_off_t chksum_blocksize; // Chksum block size
   int res_type;            //Resource type this is coupled with device_type below
   char *device_type;      //Type of resource dir
   char *device;           //Device name
   int  update_alloc;      //If 1 then the file allocation is also updated in addition to the DB
   int  enable_read_history;  //USed to enable tracking of the various history lists
   int  enable_write_history;
   int  enable_manage_history;
   int  enable_alias_history;
   int  cleanup_interval;        //Time to wait between cleanup iterations
   int  trash_grace_period[2];   //Trash grace period before cleanup
   int  preexpire_grace_period;  //Time to wait beofre moving an expired alloc to the trash bin
   int  rescan_interval;       //Wait time between trash scan
   int  n_cache;               //Number of cache entries
   int  rwm_mode;              //Read/Write/Manage mode
   ibp_time_t start_time;      //Time the resource ws added.  USed to keep expired allocations from bering removed at start.
   tbx_atomic_unit32_t counter;       //Activity counter
   apr_time_t cache_expire;    //Time before cache expires
   apr_time_t last_good_check; //Last good check
   ibp_time_t next_rescan;     //Time for next scan
   ibp_off_t max_size[3];     //Soft and Hard limits
   ibp_off_t minfree;         //Minimum amount of free space in KB
   ibp_off_t used_space[2];   //Soft/Hard data used
   ibp_off_t pending;         //Pending creates
   ibp_off_t n_allocs;        //Total number of allocations (physical and alias)
   ibp_off_t n_alias;         //Number of alias allocations
   ibp_off_t trash_size[2];   //Amount of space in trash
   ibp_off_t n_trash[2];      //Number of allocations in trash
   int    preallocate;      //PReallocate all new allocations
   DB_resource_t db;        //DB for maintaining the resource's caps
   osd_t *dev;              //Actual Device information
   int      rl_index;       //** Index in global resource array
   apr_thread_mutex_t *mutex;  //Lock for creates
   int             cleanup_shutdown;
   apr_thread_mutex_t *cleanup_lock;  //Used to shutdown cleanup thread
   apr_thread_cond_t  *cleanup_cond;  //Used to shutdown the cleanup thread
   apr_threadattr_t   *cleanup_attr;  //Used for setting the thread stack size
   apr_thread_t       *cleanup_thread;
   apr_pool_t         *pool;
} Resource_t;

typedef struct {    //*** Used to store usage info on a resource to keep from having to rescan the DB
 int version;
 int state;
 ibp_off_t used_space[2];
 ibp_off_t n_allocs;
 ibp_off_t n_alias;
} resource_usage_file_t;

typedef struct {
   int reset;
   Allocation_t hard_a;
   Allocation_t soft_a;
   DB_iterator_t *hard;
   DB_iterator_t *soft;
   Resource_t *r;
}  walk_expire_iterator_t;

#define resource_native_open_id(d, id, offset, mode) (d)->dev->native_open((d)->dev, id, offset, mode)
#define resource_native_enabled(d) (d)->dev->native_open
#define resource_native_close_id(d, fd) (d)->dev->native_close((d)->dev, fd)
#define resource_get_type(d) (d)->res_type
#define resource_get_counter(d) tbx_atomic_get((d)->counter)

IBPS_API int read_usage_file(Resource_t *r, resource_usage_file_t *u);
IBPS_API int mkfs_resource(rid_t rid, char *dev_type, char *device_name, char *db_location, ibp_off_t max_bytes);
IBPS_API int mount_resource(Resource_t *res, tbx_inip_file_t *keyfile, char *group, DB_env_t *env, int force_rebuild,
     int lazy_allocate, int truncate_expiration);
IBPS_API int umount_resource(Resource_t *res);
IBPS_API int print_resource(char *buffer, int *used, int nbytes, Resource_t *res);
IBPS_API int print_resource_usage(Resource_t *r, FILE *fd);
IBPS_API int resource_get_corrupt_count(Resource_t *r);
IBPS_API int get_allocation_state(Resource_t *r, osd_fd_t *fd);
IBPS_API osd_fd_t *open_allocation(Resource_t *r, osd_id_t id, int mode);
IBPS_API int close_allocation(Resource_t *r, osd_fd_t *fd);
IBPS_API int remove_allocation_resource(Resource_t *r, int rmode, Allocation_t *alloc);
IBPS_API void free_expired_allocations(Resource_t *r);
IBPS_API uint64_t resource_allocable(Resource_t *r, int free_space);
IBPS_API int create_allocation_resource(Resource_t *r, Allocation_t *a, ibp_off_t size, int type,
    int reliability, ibp_time_t length, int is_alias, int preallocate_space, int cs_type, ibp_off_t blocksize);
IBPS_API int resource_undelete(Resource_t *r, int trash_type, const char *trash_id, time_t expiration, Allocation_t *recovered_a);
IBPS_API int split_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a, ibp_off_t size, int type,
    int reliability, ibp_time_t length, int is_alias, int preallocate_space, int cs_type, ibp_off_t cs_blocksize);
IBPS_API int validate_allocation(Resource_t *r, osd_id_t id, int correct_errors);
IBPS_API int get_allocation_chksum_info(Resource_t *r, osd_id_t id, int *cs_type, ibp_off_t *header_blocksize, ibp_off_t *blocksize);
IBPS_API ibp_off_t get_allocation_chksum(Resource_t *r, osd_id_t id, char *disk_buffer, char *calc_buffer, ibp_off_t buffer_size,
     osd_off_t *block_len, char * good_block, ibp_off_t start_block, ibp_off_t end_block);
IBPS_API int rename_allocation_resource(Resource_t *r, Allocation_t *a);
IBPS_API int merge_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a);
IBPS_API int get_allocation_by_cap_resource(Resource_t *r, int cap_type, Cap_t *cap, Allocation_t *a);
IBPS_API int get_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a);
IBPS_API int modify_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a);
IBPS_API int get_manage_allocation_resource(Resource_t *r, Cap_t *mcap, Allocation_t *a);
IBPS_API int write_allocation_header(Resource_t *r, Allocation_t *a, int do_blank);
IBPS_API int read_allocation_header(Resource_t *r, osd_id_t id, Allocation_t *a);
IBPS_API ibp_off_t write_allocation(Resource_t *r, osd_fd_t *fd, ibp_off_t offset, ibp_off_t len, void *buffer);
IBPS_API ibp_off_t read_allocation(Resource_t *r, osd_fd_t *fd, ibp_off_t offset, ibp_off_t len, void *buffer);
IBPS_API int print_allocation_resource(Resource_t *r, FILE *fd, Allocation_t *a);
IBPS_API int calc_expired_space(Resource_t *r, time_t timestamp, ibp_off_t *nbytes);
IBPS_API walk_expire_iterator_t *walk_expire_iterator_begin(Resource_t *r);
IBPS_API void walk_expire_iterator_end(walk_expire_iterator_t *wei);
IBPS_API int set_walk_expire_iterator(walk_expire_iterator_t *wei, time_t t);
IBPS_API int get_next_walk_expire_iterator(walk_expire_iterator_t *wei, int direction, Allocation_t *a);
IBPS_API void resource_rescan(Resource_t *r);
IBPS_API void launch_resource_cleanup_thread(Resource_t *r);

IBPS_API int resource_get_mode(Resource_t *r);
IBPS_API int resource_set_mode(Resource_t *r, int mode);

IBPS_API int create_history_table(Resource_t *r);
IBPS_API int mount_history_table(Resource_t *r);
IBPS_API void umount_history_table(Resource_t *r);
IBPS_API int get_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h);
IBPS_API int put_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h);
IBPS_API int blank_history(Resource_t *r, osd_id_t id);
IBPS_API void update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid);
IBPS_API void update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid);
IBPS_API void update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, int cmd, int subcmd, int reliability, uint32_t expiration, uint64_t size, osd_id_t pid);

#endif

