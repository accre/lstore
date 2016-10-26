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

//*************************************************************
//*************************************************************

#ifndef _DB_RESOURCE_H_
#define _DB_RESOURCE_H_

#include <ibp-server/visibility.h>
#include <db.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include "allocation.h"
#include <tbx/iniparse.h>
#include "ibp_time.h"

#define DBR_TYPE_DB "db"
#define DBR_NEXT DB_NEXT
#define DBR_PREV DB_PREV

#define DBR_ITER_INIT   0
#define DBR_ITER_BUFFER 1
#define DBR_ITER_EMPTY  2

typedef struct {
  DB_ENV *dbenv;
  size_t max_size;
  int local;
} DB_env_t;


typedef struct {  //Resource DB interface
    char *kgroup;          //Ini file group
    char *loc;             //Directory with all the DB's in it
    DB *pdb;               //Primary DB (key=object id)
    DB *cap[3];            //Array of secondary DB holding caps
    DB *expire;            //DB with expiration as the key
    DB *soft;              //Expiration is used as the key but only soft allocs are stored in it  
    DB_env_t *env;
    DB_ENV *dbenv;         //Common DB enviroment to use
    apr_thread_mutex_t *mutex;  // Lock used for creates
    apr_pool_t *pool;      //** Memory pool
} DB_resource_t;

typedef struct {    //Container for cursor
    DBC *cursor;
    DB_TXN *transaction;
    DB_resource_t *dbr;
    int db_index;
    int id;
} DB_iterator_t;

void dbr_lock(DB_resource_t *dbr);
void dbr_unlock(DB_resource_t *dbr);
int print_db_resource(char *buffer, int *used, int nbytes, DB_resource_t *dbr);
int mkfs_db(DB_resource_t *dbr, char *loc, const char *kgroup, FILE *fd);
int mount_db(tbx_inip_file_t *kf, const char *kgroup, DB_env_t *dbenv, DB_resource_t *dbres);
int mount_db_generic(tbx_inip_file_t *kf, const char *kgroup, DB_env_t *dbenv, DB_resource_t *dbres, int wipe_clean);
int umount_db(DB_resource_t *dbres);
IBPS_API DB_env_t *create_db_env(const char *loc, int db_mem, int run_recover);
IBPS_API int close_db_env(DB_env_t *env);
int print_db(DB_resource_t *db, FILE *fd);
int get_num_allocations_db(DB_resource_t *db);
int get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc);
int _get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc);
int get_alloc_with_cap_db(DB_resource_t *dbr, int cap_type, Cap_t *cap, Allocation_t *alloc);
int _put_alloc_db(DB_resource_t *dbr, Allocation_t *a);
int put_alloc_db(DB_resource_t *dbr, Allocation_t *alloc);
int remove_id_only_db(DB_resource_t *dbr, osd_id_t id);
int remove_alloc_db(DB_resource_t *dbr, Allocation_t *alloc);
int remove_alloc_iter_db(DB_iterator_t *it);
int modify_alloc_iter_db(DB_iterator_t *it, Allocation_t *a);
int modify_alloc_db(DB_resource_t *dbr, Allocation_t *a);
int create_alloc_db(DB_resource_t *dbr, Allocation_t *alloc);

//DB_iterator_t *db_iterator_begin(DB *db);
int _id_iter_put_alloc_db(DB_iterator_t *it, Allocation_t *a);
int db_iterator_end(DB_iterator_t *it);
int db_iterator_next(DB_iterator_t *it, int direction, Allocation_t *a);
DB_iterator_t *expire_iterator(DB_resource_t *dbr);
DB_iterator_t *soft_iterator(DB_resource_t *dbr);
DB_iterator_t *id_iterator(DB_resource_t *dbr);
DB_iterator_t *cap_iterator(DB_resource_t *dbr, int cap_type);
int set_expire_iterator(DB_iterator_t *dbi, ibp_time_t t, Allocation_t *a);

#endif

