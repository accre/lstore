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

//***********************************************************************
// Simple file backed object storage service implementation
//***********************************************************************

#define _log_module_index 155

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include "opque.h"
#include "exnode.h"
#include "ex3_system.h"
#include "object_service_abstract.h"
#include "list.h"
#include "random.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_file.h"
#include "os_file_priv.h"
#include "append_printf.h"

//atomic_int_t _path_parse_count = 0;
//apr_thread_mutex_t *_path_parse_lock = NULL;
//apr_pool_t *_path_parse_pool = NULL;

typedef struct {
  DIR *d;
  struct dirent *entry;
  int slot;
  int type;
  char *frag;
} osf_dir_t;

typedef struct {
  object_service_fn_t *os;
  char *object_name;
  char *attr_dir;
  char *id;
  int ftype;
  int mode;
  uint64_t uuid;
} osfile_fd_t;


typedef struct {
  object_service_fn_t *os;
  char *path;
  int mode;
  char *id;
  creds_t *creds;
  osfile_fd_t **fd;
  osfile_fd_t *cfd;
  uint64_t uuid;
  int max_wait;
} osfile_open_op_t;

typedef struct {
  object_service_fn_t *os;
  osfile_fd_t *fd;
  creds_t *creds;
  char **key;
  void **val;
  char *key_tmp;
  void *val_tmp;
  int *v_size;
  int v_tmp;
  int n;
} osfile_attr_op_t;


typedef struct {
  object_service_fn_t *os;
  osfile_fd_t *fd;
  creds_t *creds;
  DIR *d;
  apr_hash_index_t *va_index;
  os_regex_table_t *regex;
  char *key;
  void *value;
  int v_max;
} osfile_attr_iter_t;

typedef struct {
  osf_dir_t *d;
  char *entry;
  char path[OS_PATH_MAX];
  regex_t *preg;
  long prev_pos;
  long curr_pos;
  int firstpass;
  char *fragment;
  int fixed_prefix;
} osf_obj_level_t;

typedef struct {
  object_service_fn_t *os;
  os_regex_table_t *table;
  os_regex_table_t *attr;
  os_regex_table_t *object_regex;
  regex_t *object_preg;
  osf_obj_level_t *level_info;
  creds_t *creds;
  os_attr_iter_t **it_attr;
  os_fd_t *fd;
  Stack_t *recurse_stack;
  char **key;
  void **val;
  int *v_size;
  int *v_size_user;
  int n_list;
  int v_fixed;
  int recurse_depth;
  int max_level;
  int v_max;
  int curr_level;
  int mode;
  int object_types;
  int finished;
} osf_object_iter_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  os_regex_table_t *rpath;
  os_regex_table_t *object_regex;
  int obj_types;
  int recurse_depth;
} osfile_remove_regex_op_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  os_regex_table_t *rpath;
  os_regex_table_t *object_regex;
  int recurse_depth;
  int object_types;
  char **key;
  void **val;
  char *id;
  int *v_size;
  int n_keys;
} osfile_regex_object_attr_op_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  char *src_path;
  char *dest_path;
  char *id;
  int type;
} osfile_mk_mv_rm_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  osfile_fd_t *fd;
  char **key_old;
  char **key_new;
  char *single_old;
  char *single_new;
  int n;
} osfile_move_attr_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  osfile_fd_t *fd_src;
  osfile_fd_t *fd_dest;
  char **key_src;
  char **key_dest;
  char *single_path;
  char *single_src;
  char *single_dest;
  char **src_path;
  int n;
} osfile_copy_attr_t;

typedef struct {
  Stack_t *stack;
  Stack_t *active_stack;
  int read_count;
  int write_count;
  pigeon_coop_hole_t pch;
} fobj_lock_t;

typedef struct {
  apr_thread_cond_t *cond;
  osfile_fd_t *fd;
  int abort;
} fobj_lock_task_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  char *path;
  os_regex_table_t *regex;
  DIR *ad;
  char *ad_path;
  os_object_iter_t *it;
  int mode;
} osfile_fsck_iter_t;

#define osf_obj_lock(lock)  apr_thread_mutex_lock(lock)
#define osf_obj_unlock(lock)  apr_thread_mutex_unlock(lock)

//void path_split(char *path, char **dir, char **file);
char *resolve_hardlink(object_service_fn_t *os, char *src_path, int add_prefix);
apr_thread_mutex_t *osf_retrieve_lock(object_service_fn_t *os, char *path, int *table_slot);
int osf_set_attr(object_service_fn_t *os, creds_t *creds, osfile_fd_t *ofd, char *attr, void *val, int v_size, int *atype);
int osf_get_attr(object_service_fn_t *os, creds_t *creds, osfile_fd_t *ofd, char *attr, void **val, int *v_size, int *atype);
op_generic_t *osfile_set_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size);
os_attr_iter_t *osfile_create_attr_iter(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, os_regex_table_t *attr, int v_max);
void osfile_destroy_attr_iter(os_attr_iter_t *oit);
op_status_t osfile_open_object_fn(void *arg, int id);
op_generic_t *osfile_open_object(object_service_fn_t *os, creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait);
op_status_t osfile_close_object_fn(void *arg, int id);
op_generic_t *osfile_close_object(object_service_fn_t *os, os_fd_t *fd);
os_object_iter_t *osfile_create_object_iter(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
                     os_regex_table_t *attr,  int recurse_depth, os_attr_iter_t **it_attr, int v_max);
int osfile_next_object(os_object_iter_t *oit, char **fname, int *prefix_len);
void osfile_destroy_object_iter(os_object_iter_t *it);
op_status_t osf_set_multiple_attr_fn(void *arg, int id);
int lowlevel_set_attr(object_service_fn_t *os, char *attr_dir, char *attr, void *val, int v_size);
char *object_attr_dir(object_service_fn_t *os, char *prefix, char *path, int ftype);


//*************************************************************
//  osf_store_val - Stores the return attribute value
//*************************************************************

int osf_store_val(void *src, int src_size, void **dest, int *v_size)
{
  char *buf;

  if (*v_size > 0) {
     if (*v_size < src_size) {
        *v_size = -src_size;
        return(1);
     } else if (*v_size > src_size) {
        buf = *dest; buf[src_size] = 0;  //** IF have the space NULL terminate
     }
  } else {
     *dest = malloc(src_size+1);
     buf = *dest; buf[src_size] = 0;  //** IF have the space NULL terminate
  }

  *v_size = src_size;
  memcpy(*dest, src, src_size);
  return(0);
}

//*************************************************************
//  osf_make_attr_symlink - Makes an attribute symlink
//*************************************************************

void osf_make_attr_symlink(object_service_fn_t *os, char *link_path, char *dest_path, char *dest_key)
{
//  osfile_priv_t *osf = (osfile_priv_t *)os->priv;

  snprintf(link_path, OS_PATH_MAX, "%s/%s", dest_path, dest_key);
}

//*************************************************************
//  osf_resolve_attr_symlink - Resolves an attribute symlink
//*************************************************************

int osf_resolve_attr_path(object_service_fn_t *os, char *real_path, char *path, char *key, int ftype, int *atype, int max_recurse)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  char *attr, *dkey, *dfile;
  char *pdir, *pfile;
  int n, dtype, err;
  char fullname[OS_PATH_MAX];

  //** Get the key path
  attr = object_attr_dir(os, osf->file_path, path, ftype);
  snprintf(real_path, OS_PATH_MAX, "%s/%s", attr, key);
  *atype = os_local_filetype(real_path);
log_printf(15, "fullname=%s atype=%d\n", real_path, *atype);
  if ((*atype & OS_OBJECT_SYMLINK) == 0) {  //** If a normal file then just return
     free(attr);
     return(0);
  }

  free(attr);

  //** It's a symlink so read it first
  n = readlink(real_path, fullname, OS_PATH_MAX-1);
  if (n < 0) {
    log_printf(0, "Bad link:  path=%s key=%s ftype=%d fullname=%s\n", path, key, ftype, fullname);
    return(1);
  }

  fullname[n] = 0;

log_printf(15, "fullname=%s real_path=%s\n", fullname, real_path);

  //** Now split it out into object and key
  os_path_split(fullname, &dfile, &dkey);

log_printf(15, "fullname=%s dfile=%s dkey=%s\n", fullname, dfile, dkey);

  //** Find out what ftype the target is
  if (dfile[0] == '/') {
     snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, dfile);
  } else {
     if (ftype & OS_OBJECT_DIR) {
        snprintf(fullname, OS_PATH_MAX, "%s%s/%s", osf->file_path, path, dfile);
     } else {
        os_path_split(path, &pdir, &pfile);
        snprintf(fullname, OS_PATH_MAX, "%s%s/%s", osf->file_path, pdir, dfile);
        free(pdir); free(pfile);
    }
  }
log_printf(15, "fullattrpath=%s ftype=%d\n", fullname, ftype);

  dtype = os_local_filetype(fullname);
  if (dtype == 0) {
    log_printf(0, "Missing object:  path=%s key=%s ftype=%d fullname=%s\n", path, key, ftype, fullname);
    free(dfile); free(dkey);
    return(1);
  }

//  attr = object_attr_dir(os, "", &(fullname[osf->file_path_len]), dtype);
  attr = object_attr_dir(os, "", fullname, dtype);
  snprintf(real_path, OS_PATH_MAX, "%s/%s", attr, dkey);

  log_printf(15, "path=%s key=%s ftype=%d real_path=%s\n", path, key, ftype, real_path);

  err = 0;
  dtype = os_local_filetype(real_path);
  if (dtype & OS_OBJECT_SYMLINK) {  //** Need to recurseively resolve the link
     if (max_recurse > 0) {
        err = osf_resolve_attr_path(os, real_path, &(fullname[osf->file_path_len]), dkey, dtype, &n, max_recurse-1);
     } else {
        log_printf(0, "Oops! Hit max recurse depth! last path=%s\n", real_path);
     }
  }

  free(attr); free(dfile); free(dkey);

  return(err);
}

//*************************************************************
// fobj_add_active - Adds the object to the active list
//*************************************************************

void fobj_add_active(fobj_lock_t *fol, osfile_fd_t *fd)
{
  move_to_bottom(fol->active_stack);
  insert_below(fol->active_stack, fd);
}

//*************************************************************
// fobj_remove_active - Removes the object to the active list
//*************************************************************

int fobj_remove_active(fobj_lock_t *fol, osfile_fd_t *myfd)
{
  osfile_fd_t *fd;
  int success = 1;

  move_to_top(fol->active_stack);
  while ((fd = (osfile_fd_t *)get_ele_data(fol->active_stack)) != NULL) {
     if (fd == myfd) {  //** Found a match
        delete_current(fol->active_stack, 0, 0);
        success = 0;
        break;
     }

     move_down(fol->active_stack);
  }

  return(success);
}

//*************************************************************
// fobj_lock_task_new - Creates a new shelf of for object locking
//*************************************************************

void *fobj_lock_task_new(void *arg, int size)
{
  apr_pool_t *mpool = (apr_pool_t *)arg;
  fobj_lock_task_t *shelf;
  int i;

  type_malloc_clear(shelf, fobj_lock_task_t, size);

  for (i=0; i<size; i++) {
    apr_thread_cond_create(&(shelf[i].cond), mpool);
  }

  return((void *)shelf);
}

//*************************************************************
// fobj_lock_task_free - Destroys a shelf of object locking variables
//*************************************************************

void fobj_lock_task_free(void *arg, int size, void *data)
{
  fobj_lock_task_t *shelf = (fobj_lock_task_t *)data;
  int i;

  for (i=0; i<size; i++) {
    apr_thread_cond_destroy(shelf[i].cond);
  }

  free(shelf);
  return;
}

//*************************************************************
// fobj_lock_new - Creates a new shelf of for object locking
//*************************************************************

void *fobj_lock_new(void *arg, int size)
{
//  apr_pool_t *mpool = (apr_pool_t *)arg;
  fobj_lock_t *shelf;
  int i;

  type_malloc_clear(shelf, fobj_lock_t, size);

  for (i=0; i<size; i++) {
    shelf[i].stack = new_stack();
    shelf[i].active_stack = new_stack();
    shelf[i].read_count = 0;
    shelf[i].write_count = 0;
  }

  return((void *)shelf);
}

//*************************************************************
// fobj_lock_free - Destroys a shelf of object locking variables
//*************************************************************

void fobj_lock_free(void *arg, int size, void *data)
{
  fobj_lock_t *shelf = (fobj_lock_t *)data;
  int i;

  for (i=0; i<size; i++) {
    free_stack(shelf[i].stack, 0);
    free_stack(shelf[i].active_stack, 0);
  }

  free(shelf);
  return;
}

//***********************************************************************
// fobj_wait - Waits for my turn to access the object
//    NOTE: On entry I should be holding osf->fobj_lock
//          The lock is cycled in the routine
//***********************************************************************

int fobj_wait(object_service_fn_t *os, fobj_lock_t *fol, osfile_fd_t *fd, int max_wait)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  pigeon_coop_hole_t task_pch;
  fobj_lock_task_t *handle;
  int aborted;
  apr_time_t timeout = apr_time_make(max_wait, 0);
apr_time_t dt;

  //** Get my slot
  task_pch = reserve_pigeon_coop_hole(osf->task_pc);
  handle = (fobj_lock_task_t *)pigeon_coop_hole_data(&task_pch);
  handle->fd = fd;
  handle->abort = 1;

  log_printf(15, "SLEEPING id=%s fname=%s mymode=%d read_count=%d write_count=%d handle=%p max_wait=%d\n", fd->id, fd->object_name, fd->mode, fol->read_count, fol->write_count, handle, max_wait);

  move_to_bottom(fol->stack);
  insert_below(fol->stack, handle);

dt = apr_time_now();
  //** Sleep until it's my turn.  Remember fobj_lock is already set upon entry
  apr_thread_cond_timedwait(handle->cond, osf->fobj_lock, timeout);
  aborted = handle->abort;
dt = apr_time_now() - dt;
int dummy = apr_time_sec(dt);
  log_printf(15, "AWAKE id=%s fname=%s mymode=%d read_count=%d write_count=%d handle=%p abort=%d uuid=" LU " dt=%d\n", fd->id, fd->object_name, fd->mode, fol->read_count, fol->write_count, handle, aborted, fd->uuid, dummy);

  //** I'm popped off the stack so just free my handle and update the counter
  release_pigeon_coop_hole(osf->task_pc, &task_pch);

  if (aborted == 1) { //** Open was aborted so remove myself from the pending and kick out
     move_to_top(fol->stack);
     while ((handle = (fobj_lock_task_t *)get_ele_data(fol->stack)) != NULL) {
        if (handle->fd->uuid == fd->uuid) {
           log_printf(15, "id=%s fname=%s uuid=" LU " ABORTED\n", fd->id, fd->object_name, fd->uuid);
           delete_current(fol->stack, 1, 0);
           break;
        }
        move_down(fol->stack);
     }

     return(1);
  }

  //** Check if the next person should be woke up as well
  if (stack_size(fol->stack) != 0) {
     move_to_top(fol->stack);
     handle = (fobj_lock_task_t *)get_ele_data(fol->stack);

     if ((fd->mode == OS_MODE_READ_BLOCKING) && (handle->fd->mode == OS_MODE_READ_BLOCKING)) {
         pop(fol->stack);  //** Clear it from the stack. It's already stored in handle above
         log_printf(15, "WAKEUP ALARM id=%s fname=%s mymode=%d read_count=%d write_count=%d handle=%p\n", fd->id, fd->object_name, fd->mode, fol->read_count, fol->write_count, handle);

         handle->abort = 0;
         apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released in the calling routine
     }
  }

  return(0);
}

//***********************************************************************
// full_object_lock -  Locks the object across all systems
//***********************************************************************

int full_object_lock(osfile_fd_t *fd, int max_wait)
{
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  pigeon_coop_hole_t obj_pch;
  fobj_lock_t *fol;
  fobj_lock_task_t *handle;
  int err;

  if (fd->mode == OS_MODE_READ_IMMEDIATE) return(0);

  apr_thread_mutex_lock(osf->fobj_lock);

  fol = list_search(osf->fobj_table, fd->object_name);

  if (fol == NULL) {  //** No one else is accessing the file
     obj_pch =  reserve_pigeon_coop_hole(osf->fobj_pc);
     fol = (fobj_lock_t *)pigeon_coop_hole_data(&obj_pch);
     fol->pch = obj_pch;  //** Reverse link my PCH for release later
     list_insert(osf->fobj_table, fd->object_name, fol);
     log_printf(15, "fname=%s new lock!\n", fd->object_name);
  }

  log_printf(15, "START id=%s fname=%s mymode=%d read_count=%d write_count=%d\n", fd->id, fd->object_name, fd->mode, fol->read_count, fol->write_count);

  err = 0;
  if (fd->mode == OS_MODE_READ_BLOCKING) { //** I'm reading
     if (fol->write_count == 0) { //** No one currently writing
        //** Check and make sure the person waiting isn't a writer
        if (stack_size(fol->stack) != 0) {
           move_to_top(fol->stack);
           handle = (fobj_lock_task_t *)get_ele_data(fol->stack);
           if (handle->fd->mode == OS_MODE_WRITE_BLOCKING) {  //** They want to write so sleep until my turn
              err = fobj_wait(fd->os, fol, fd, max_wait);  //** The fobj_lock is released/acquired inside
           }
        }
     } else {
        err = fobj_wait(fd->os, fol, fd, max_wait);  //** The fobj_lock is released/acquired inside
     }

     if (err == 0) fol->read_count++;
  } else {   //** I'm writing
     if ((fol->write_count != 0) || (fol->read_count != 0) || (stack_size(fol->stack) != 0)) {  //** Make sure no one else is doing anything
        err = fobj_wait(fd->os, fol, fd, max_wait);  //** The fobj_lock is released/acquired inside
     }
     if (err == 0) fol->write_count++;
  }

  if (err == 0) fobj_add_active(fol, fd);

  log_printf(15, "END id=%s fname=%s mymode=%d read_count=%d write_count=%d\n", fd->id, fd->object_name, fd->mode, fol->read_count, fol->write_count);

  apr_thread_mutex_unlock(osf->fobj_lock);

  return(err);
}

//***********************************************************************
// full_object_unlock -  Releases the global lock
//***********************************************************************

void full_object_unlock(osfile_fd_t *fd)
{
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  fobj_lock_t *fol;
  fobj_lock_task_t *handle;
  int err;

  if (fd->mode == OS_MODE_READ_IMMEDIATE) return;

  apr_thread_mutex_lock(osf->fobj_lock);

  fol = list_search(osf->fobj_table, fd->object_name);
  if (fol == NULL) return;

  err = fobj_remove_active(fol, fd);

  if (err != 0) {  //**Exit if it wasn't found
    apr_thread_mutex_unlock(osf->fobj_lock);
    return;
  }

  //** Update the counts
  if (fd->mode == OS_MODE_READ_BLOCKING) {
     fol->read_count--;
  } else {
     fol->write_count--;
  }

  log_printf(15, "fname=%s mymode=%d read_count=%d write_count=%d\n", fd->object_name, fd->mode, fol->read_count, fol->write_count);

  if ((stack_size(fol->stack) == 0) && (fol->read_count == 0) && (fol->write_count == 0)) {  //** No one else is waiting so remove the entry
     list_remove(osf->fobj_table, fd->object_name, NULL);
     release_pigeon_coop_hole(osf->fobj_pc, &(fol->pch));
  } else if (stack_size(fol->stack) > 0) { //** Wake up the next person
     move_to_top(fol->stack);
     handle = (fobj_lock_task_t *)get_ele_data(fol->stack);

     if (((handle->fd->mode == OS_MODE_READ_BLOCKING) && (fol->write_count == 0)) ||
         ((handle->fd->mode == OS_MODE_WRITE_BLOCKING) && (fol->write_count == 0) && (fol->read_count == 0))) {
         pop(fol->stack);  //** Clear it from the stack. It's already stored in handle above
         log_printf(15, "WAKEUP ALARM fname=%s mymode=%d read_count=%d write_count=%d handle=%p\n", fd->object_name, fd->mode, fol->read_count, fol->write_count, handle);
         handle->abort = 0;
         apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released in the calling routine
     }
  }

  apr_thread_mutex_unlock(osf->fobj_lock);
}

//***********************************************************************
// osf_multi_lock - Used to resolve/lock a collection of attrs that are
//     links
//***********************************************************************

void osf_multi_lock(object_service_fn_t *os, creds_t *creds, osfile_fd_t *fd, char **key, int n_keys, int first_link, apr_thread_mutex_t **lock_table, int *n_locks) 
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  int i, j, n, atype, small_slot, small_index, max_index;
  int v_size, va_prefix_len;
  int lock_slot[n_keys+1];
  char linkname[OS_PATH_MAX];
  char attr_name[OS_PATH_MAX];
  void *val = linkname;

  //** Always get the primary
  n = 0;
  lock_table[n] = osf_retrieve_lock(os, fd->object_name, &lock_slot[n]);
  max_index = lock_slot[0];
  n++;

  log_printf(15, "lock_slot[0]=%d fname=%s\n", lock_slot[0], fd->object_name);

  //** Set up the va attr_link key for use
  va_prefix_len = (long)osf->attr_link_pva.priv;
  strcpy(attr_name, osf->attr_link_pva.attribute);
  attr_name[va_prefix_len] = '.';
  va_prefix_len++;

  //** Now cycle through the attributes starting with the 1 that triggered the call
  linkname[sizeof(linkname)-1] = 0;
  for (i=first_link; i<n_keys; i++) {
     v_size = sizeof(linkname);
     linkname[0] = 0;
     strcpy(&(attr_name[va_prefix_len]), key[i]);
     osf->attr_link_pva.get(&osf->attr_link_pva, os, creds, fd, attr_name, &val, &v_size, &atype);
     log_printf(15, "key[%d]=%s v_size=%d attr_name=%s linkname=%s\n", i, key[i], v_size, attr_name, linkname);

     if (v_size > 0) {
        j=v_size-1;  //** Peel off the key name.  We only need the parent object path
        while (linkname[j] != '\n' && (j>0)) { j--; }
        linkname[j] = 0;

        lock_table[n] = osf_retrieve_lock(os, linkname, &lock_slot[n]);
        log_printf(15, "checking n=%d key=%s lname=%s v_size=%dj=%d\n", n, key[i], linkname, v_size, j);

        //** Make sure I don't already have it in the list
        for (j=0; j<n; j++) {
            if (lock_slot[n] == lock_slot[j]) { n--; break; }
        }
        n++;
     }
  }

  log_printf(15, "n_locks=%d\n", n);

  *n_locks = n;  //** Return the lock count

  //** Now acquire them in order from smallest->largest
  //** This is done naively cause normally there will be just a few locks
  max_index = osf->internal_lock_size;
  for (i=0; i<n; i++) {
     small_slot = -1;
     small_index = max_index;
     for (j=0; j<n; j++) {
        if (small_index > lock_slot[i]) {
           small_index = lock_slot[i];
           small_slot = i;
        }
     }

     apr_thread_mutex_lock(lock_table[small_slot]);
     lock_slot[small_slot] = max_index;
  }

  return;
}


//***********************************************************************
// osf_multi_unlock - Releases a collection of locks
//***********************************************************************

void osf_multi_unlock(apr_thread_mutex_t **lock, int n)
{
  int i;

  for (i=0; i<n; i++) {
     apr_thread_mutex_unlock(lock[i]);
  }

  return;
}

//***********************************************************************
// va_create_get_attr - Returns the object creation tim in secs since epoch
//***********************************************************************

int va_create_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  struct stat s;
  int  bufsize, err;
  uint64_t dt;
  char buffer[32];
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);
  err = stat(fullname, &s);
  if (err != 0) {
     *v_size = -1;
     return(1);
  }

  dt = s.st_ctime;  //** Linux doesn't really have a creation time but we don;t touch the proxy after creation
  snprintf(buffer, sizeof(buffer), LU , dt);
  bufsize = strlen(buffer);

  log_printf(15, "fname=%s sec=%s\n", fd->object_name, buffer);

  return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// va_link_get_attr - Returns the object link information
//***********************************************************************

int va_link_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  struct stat s;
  char buffer[32*1024];
  int err, n, offset;
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);

  err = lstat(fullname, &s);
  if (err == 0) {
     if (S_ISLNK(s.st_mode) == 0) {
        *v_size = 0;
        *val = NULL;
        return(0);
     }

     n = readlink(fullname, buffer, sizeof(buffer)-1);
     if (n > 0) {
        buffer[n] = 0;
        log_printf(15, "file_path=%s fullname=%s link=%s\n", osf->file_path, fullname, buffer);

        if (buffer[0] == '/') {
           offset = osf->file_path_len;
           n = n - offset;
           return(osf_store_val(&(buffer[offset]), n, val, v_size));
        } else {
          return(osf_store_val(buffer, n, val, v_size));
        }
     }
  }

  *v_size = 0;

  return(0);
}

//***********************************************************************
// va_link_count_get_attr - Returns the object link count information
//***********************************************************************

int va_link_count_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  struct stat s;
  char buffer[32];
  int err, n;
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);

  err = lstat(fullname, &s);
  if (err == 0) {
     n = s.st_nlink;
     if (S_ISDIR(s.st_mode)) {
        n = n - 1;  //** IF a dir don't count the attribute dir
     } else if ( n > 1) { //** Normal files should only have 1.  If more then it's a hardlink so tweak it
        n = n - 1;
     }
  } else {
    n = 1;   //** Dangling link probably
  }

  err = snprintf(buffer, 32, "%d", n);
  return(osf_store_val(buffer, err, val, v_size));
}

//***********************************************************************
// va_type_get_attr - Returns the object type information
//***********************************************************************

int va_type_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  int ftype, bufsize;
  char buffer[32];
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);
  ftype = os_local_filetype(fullname);

  snprintf(buffer, sizeof(buffer), "%d", ftype);
  bufsize = strlen(buffer);

  log_printf(15, "fname=%s type=%s v_size=%d\n", fd->object_name, buffer, *v_size);

  return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// va_lock_get_attr - Returns the file lock information
//***********************************************************************

int va_lock_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  osfile_fd_t *pfd;
  fobj_lock_t *fol;
  fobj_lock_task_t *handle;
  int used;
  int bufsize = 10*1024;
  char result[bufsize];
  char *buf;

  *atype = OS_OBJECT_VIRTUAL;

  log_printf(15, "fname=%s\n", fd->object_name);

  apr_thread_mutex_lock(osf->fobj_lock);

  fol = list_search(osf->fobj_table, fd->object_name);
  log_printf(15, "fol=%p\n", fol);

  if (fol == NULL) {
     *val = NULL;
     *v_size = 0;
     apr_thread_mutex_unlock(osf->fobj_lock);
     return(0);
  }

  //** Figure out the buffer
  buf = result;
  if (*v_size > 0) {
     buf = (char *)(*val);
     bufsize = *v_size;
  }

  used = 0;
  append_printf(buf, &used, bufsize, "[os.lock]\n");

  //** Print the active info
  if (fol->read_count > 0) {
     append_printf(buf, &used, bufsize, "active_mode=READ\n");
     append_printf(buf, &used, bufsize, "active_count=%d\n", fol->read_count);
  } else {
     append_printf(buf, &used, bufsize, "active_mode=WRITE\n");
     append_printf(buf, &used, bufsize, "active_count=%d\n", fol->write_count);
  }

  move_to_top(fol->active_stack);
  while ((pfd = (osfile_fd_t *)get_ele_data(fol->active_stack)) != NULL) {
     if (pfd->mode == OS_MODE_READ_BLOCKING) {
        append_printf(buf, &used, bufsize, "active_id=%s:" LU ":READ\n", pfd->id, pfd->uuid);
     } else {
        append_printf(buf, &used, bufsize, "active_id=%s:" LU ":WRITE\n", pfd->id, pfd->uuid);
     }
     move_down(fol->active_stack);
  }


  append_printf(buf, &used, bufsize, "\n");
  append_printf(buf, &used, bufsize, "pending_count=%d\n", stack_size(fol->stack));
  move_to_top(fol->stack);
  while ((handle = (fobj_lock_task_t *)get_ele_data(fol->stack)) != NULL) {
     if (handle->fd->mode == OS_MODE_READ_BLOCKING) {
        append_printf(buf, &used, bufsize, "pending_id=%s:" LU ":READ\n", handle->fd->id, handle->fd->uuid);
     } else {
        append_printf(buf, &used, bufsize, "pending_id=%s:" LU ":WRITE\n", handle->fd->id, handle->fd->uuid);
     }
     move_down(fol->stack);
  }

  apr_thread_mutex_unlock(osf->fobj_lock);

  if (*v_size < 0) *val = strdup(buf);
  *v_size = strlen(buf);

  return(0);
}


//***********************************************************************
// va_attr_type_get_attr - Returns the attribute type information
//***********************************************************************

int va_attr_type_get_attr(os_virtual_attr_t *myva, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  os_virtual_attr_t *va;
  int ftype, bufsize, n;
  char *key;
  char buffer[32];
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

  n = (int)(long)myva->priv;  //** HACKERY ** to get the attribute prefix length
  key = &(fullkey[n+1]);

  //** See if we have a VA first
  va = apr_hash_get(osf->vattr_hash, key, APR_HASH_KEY_STRING);
  if (va != NULL) {
    ftype = OS_OBJECT_VIRTUAL;
  } else {
//    n = osf_resolve_attr_path(os, fullname, fd->object_name, key, fd->ftype, &ftype, 10);
    snprintf(fullname, OS_PATH_MAX, "%s/%s", fd->attr_dir, key);
    ftype = os_local_filetype(fullname);
    if (ftype & OS_OBJECT_BROKEN_LINK) ftype = ftype ^ OS_OBJECT_BROKEN_LINK;
  }

  snprintf(buffer, sizeof(buffer), "%d", ftype);
  bufsize = strlen(buffer);

  log_printf(15, "fname=%s type=%s\n", fd->object_name, buffer);

  return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// va_attr_link_get_attr - Returns the attribute link information
//***********************************************************************

int va_attr_link_get_attr(os_virtual_attr_t *myva, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  os_virtual_attr_t *va;
  list_iter_t it;
  struct stat s;
  char buffer[OS_PATH_MAX];
  char *key;
  char *ca;
  int err, n;
  char fullname[OS_PATH_MAX];

  *atype = OS_OBJECT_VIRTUAL;

//log_printf(15, "val=%p, *val=%s\n", val, (char *)(*val));

  n = (int)(long)myva->priv;  //** HACKERY ** to get the attribute prefix length
  key = &(fullkey[n+1]);

  //** Do a Virtual Attr check
  //** Check the prefix VA's first
  it = list_iter_search(osf->vattr_prefix, key, -1);
  list_next(&it, (list_key_t **)&ca, (list_data_t **)&va);

  if (va != NULL) {
     n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
     log_printf(15, "va=%s attr=%s n=%d\n", va->attribute, key, n);
int d=strncmp(key, va->attribute, n);
     log_printf(15, "strncmp=%d\n", d);
     if (strncmp(key, va->attribute, n) == 0) {  //** Prefix matches
        return(va->get_link(va, os, creds, ofd, key, val, v_size, atype));
     }
  }

  //** Now check the normal VA's
  va = apr_hash_get(osf->vattr_hash, key, APR_HASH_KEY_STRING);
  if (va != NULL) { return(va->get_link(va, os, creds, ofd, key, val, v_size, atype)); }


  //** Now check the normal attributes
  snprintf(fullname, OS_PATH_MAX, "%s/%s", fd->attr_dir, key);

  err = lstat(fullname, &s);
  if (err == 0) {
     if (S_ISLNK(s.st_mode) == 0) {
        *v_size = 0;
        return(0);
     }

     buffer[0] = 0;
     n = readlink(fullname, buffer, OS_PATH_MAX-1);
     if (n > 0) {
        buffer[n] = 0;
log_printf(15, "readlink(%s)=%s  n=%d\n", fullname, buffer, n);
        log_printf(15, "munged path=%s\n", buffer);
//        os_path_split(buffer, &dname, &aname);
//        snprintf(buffer, OS_PATH_MAX, "%s\n%s", dname, aname);
//        free(dname); free(aname);
        return(osf_store_val(buffer, n, val, v_size));
     }
  }

  *v_size = 0;

  return(0);
}

//***********************************************************************
// va_timestamp_set_attr - Sets the requested timestamp
//***********************************************************************

int va_timestamp_set_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *fullkey, void *val, int v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
//  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  char buffer[512];
  char *key;
  int64_t curr_time;
  int n;

  n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length
  key = &(fullkey[n+1]);

  if (strlen(fullkey) < n) {  //** Nothing to do so return;
     *atype = OS_OBJECT_VIRTUAL;
     return(1);
  }

  curr_time = apr_time_sec(apr_time_now());
  if (v_size > 0) {
     n = snprintf(buffer, sizeof(buffer), I64T "|%s", curr_time, (char *)val);
  } else {
     n = snprintf(buffer, sizeof(buffer), I64T, curr_time);
  }

  n = osf_set_attr(os, creds, fd, key, (void *)buffer, n, atype);
  *atype |= OS_OBJECT_VIRTUAL;

  return(n);
}


//***********************************************************************
// va_timestamp_get_attr - Returns the requested timestamp or current time
//    if no timestamp is specified
//***********************************************************************

int va_timestamp_get_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
//  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  char buffer[32];
  char *key;
  int64_t curr_time;
  int n;

  n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length

  log_printf(15, "fullkey=%s va=%s\n", fullkey, va->attribute);

  if (strlen(fullkey) > n) {  //** Normal attribute timestamp
     key = &(fullkey[n+1]);
     n = osf_get_attr(os, creds, fd, key, val, v_size, atype);
     *atype |= OS_OBJECT_VIRTUAL;
  } else {  //** No attribute specified so just return my time
    curr_time = apr_time_sec(apr_time_now());
    n = snprintf(buffer, sizeof(buffer), I64T, curr_time);
    log_printf(15, "now=%s\n", buffer);
    *atype = OS_OBJECT_VIRTUAL;
    n = osf_store_val(buffer, n, val, v_size);
  }

  return(n);
}

//***********************************************************************
// va_timestamp_get_link - Returns the requested timestamp's link if available
//***********************************************************************

int va_timestamp_get_link_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  char buffer[OS_PATH_MAX];
  char *key;
  int n;

  n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length
  key = &(fullkey[n+1]);

  if (strlen(fullkey) > n) {  //** Normal attribute timestamp
     n = (long)osf->attr_link_pva.priv;
     strcpy(buffer, osf->attr_link_pva.attribute);
     buffer[n] = '.';
     n++;

     strcpy(&(buffer[n]), key);
     n = osf->attr_link_pva.get(&osf->attr_link_pva, os, creds, fd, buffer, val, v_size, atype);
  } else {  //** No attribute specified os return 0
     *atype = OS_OBJECT_VIRTUAL;
     *v_size = 0;
     n = 0;
  }

  return(n);
}



//***********************************************************************
// va_null_set_attr - Dummy routine since it can't be set
//***********************************************************************

int va_null_set_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size, int *atype)
{
  *atype = OS_OBJECT_VIRTUAL;
  return(-1);
}

//***********************************************************************
// va_null_get_link_attr - Routine for key's without links
//***********************************************************************

int va_null_get_link_attr(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype)
{
  *atype = OS_OBJECT_VIRTUAL;
  *v_size = 0;
  return(0);
}

//***********************************************************************
//  osf_retrieve_lock - Returns the internal lock for the object
//***********************************************************************

apr_thread_mutex_t *osf_retrieve_lock(object_service_fn_t *os, char *path, int *table_slot)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  chksum_t cs;
  char  digest[OSF_LOCK_CHKSUM_SIZE];
  unsigned int *n;
  int nbytes, slot;
  tbuffer_t tbuf;

  nbytes = strlen(path);
  tbuffer_single(&tbuf, nbytes, path);
  chksum_set(&cs, OSF_LOCK_CHKSUM);
  chksum_add(&cs, nbytes, &tbuf, 0);
  chksum_get(&cs, CHKSUM_DIGEST_BIN, digest);

  n = (unsigned int *)(&digest[OSF_LOCK_CHKSUM_SIZE-sizeof(unsigned int)]);
  slot = (*n) % osf->internal_lock_size;
  log_printf(15, "n=%u internal_lock_size=%d slot=%d path=!%s!\n", *n, osf->internal_lock_size, slot, path); flush_log();
  if (table_slot != NULL) *table_slot = slot;

  return(osf->internal_lock[slot]);
}


//***********************************************************************
// safe_remove - Does a simple check that the object to be removed
//     is not "/".
//***********************************************************************

int safe_remove(object_service_fn_t *os, char *path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;

  if ((strlen(path) > SAFE_MIN_LEN) && (strncmp(osf->base_path, path, osf->base_path_len) == 0)) {
     return(remove(path));
  }

  log_printf(15, " ERROR with remove!  base_path=%s path=%s safe_len=%d\n", osf->base_path, path, SAFE_MIN_LEN);
  return(-1234);
}

//***********************************************************************
// object_attr_dir - Returns the object attribute directory
//***********************************************************************

char *object_attr_dir(object_service_fn_t *os, char *prefix, char *path, int ftype)
{
//  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  char fname[OS_PATH_MAX];
  char *dir, *base;
  char *attr_dir = NULL;
  int n;

  if ((ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) != 0) {
     strncpy(fname, path, OS_PATH_MAX);
     os_path_split(fname, &dir, &base);
     //log_printf(15, "fname=%s dir=%s base=%s file_path=%s\n", fname, dir, base, osf->file_path);
     n = strlen(dir);
     if (dir[n-1] == '/') dir[n-1] = 0; //** Peel off a trialing /
     snprintf(fname, OS_PATH_MAX, "%s%s/%s/%s%s", prefix, dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     //log_printf(15, "attr_dir=%s\n", fname);
     attr_dir = strdup(fname);
     free(dir); free(base);
  } else if (ftype == OS_OBJECT_DIR) {
     snprintf(fname, OS_PATH_MAX, "%s%s/%s", prefix, path, FILE_ATTR_PREFIX);
     attr_dir = strdup(fname);
  }

  return(attr_dir);
}

//***********************************************************************
// osf_is_dir_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_dir_empty(char *path)
{
  DIR *d;
  struct dirent *entry;

  int empty = 1;

  d = opendir(path);
  if (d == NULL) return(1);

  while ((empty == 1) && ((entry = readdir(d)) != NULL)) {
     if ( ! ((strcmp(entry->d_name, FILE_ATTR_PREFIX) == 0) ||
             (strcmp(entry->d_name, ".") == 0) || (strcmp(entry->d_name, "..") == 0)) ) empty = 0;
  }

  if (empty == 0) log_printf(15, "path=%s found entry=%s\n", path, entry->d_name);
  closedir(d);

  return(empty);
}
//***********************************************************************
// osf_is_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_empty(char *path)
{
  int ftype;

  ftype = os_local_filetype(path);
  if (ftype == OS_OBJECT_FILE) {  //** Simple file
    return(1);
  } else if (ftype == OS_OBJECT_DIR) { //** Directory
    return(osf_is_dir_empty(path));
  }

  return(0);
}

//***********************************************************************

char *my_readdir(osf_dir_t *d)
{
  char *fname;

  if (d->type == 0) {
     d->entry = readdir(d->d);
     if (d->entry == NULL) return(NULL);
     fname = &(d->entry->d_name[0]);
//log_printf(0, "fname=%s\n", fname);
     return(fname);
  }

  if (d->slot < 1) {
     d->slot++;
//log_printf(0, "frag=%s slot=%d\n", d->frag, d->slot);
     return(d->frag);
  }

//log_printf(0, "fname=NULL slot=%d\n", d->slot);

  return(NULL);
}

//***********************************************************************

osf_dir_t *my_opendir(char *fullname, char *frag)
{
  osf_dir_t *d;

  type_malloc(d, osf_dir_t, 1);

//log_printf(0, "fullname=%s frag=%s\n", fullname, frag);
  if (frag == NULL) {
     d->type = 0;
     d->d = opendir(fullname);
  } else {
     d->type = 1;
     d->frag = frag;
     d->slot = 0;
  }

  return(d);
}

//***********************************************************************

void my_closedir(osf_dir_t *d)
{
  if (d->type == 0) {
    closedir(d->d);
  }

  free(d);
}

//***********************************************************************

long my_telldir(osf_dir_t *d)
{
  if (d->type == 0) {
     return(telldir(d->d));
  }

  return(d->slot);
}

//***********************************************************************

void my_seekdir(osf_dir_t *d, long offset)
{
  if (d->type == 0) {
     seekdir(d->d, offset);
  } else {
     d->slot = offset;
  }
}

//***********************************************************************
// osf_next_object - Returns the iterators next object
//***********************************************************************

int osf_next_object(osf_object_iter_t *it, char **myfname, int *prefix_len)
{
  osfile_priv_t *osf = (osfile_priv_t *)it->os->priv;
  int i, rmatch, tweak;
  osf_obj_level_t *itl, *it_top;
  char fname[OS_PATH_MAX];
  char fullname[OS_PATH_MAX];
  char *obj_fixed = NULL;

  *prefix_len = 0;
  if (it->finished == 1) { *myfname = NULL; return(0); }

  //** Check if we have a fixed object regex.  If so it's handled directly via strcmp()
  if (it->object_regex != NULL) {
     if (it->object_regex->regex_entry->fixed == 1) obj_fixed = it->object_regex->regex_entry->expression;
  }

  //** Check if we have a prefix path of '/'.  If so make a fake itl level
  tweak = 0;
  if (it->table->n == 0) {
     *prefix_len = 1;
     if (stack_size(it->recurse_stack) == 0) {  //**Make a fake level to get things going
        type_malloc_clear(itl, osf_obj_level_t, 1);
        strncpy(itl->path, "/", OS_PATH_MAX);
        itl->d = my_opendir(osf->file_path, NULL);
        itl->curr_pos = my_telldir(itl->d);
        itl->firstpass = 1;
        push(it->recurse_stack, itl);
     }
  } else {
     it_top = &(it->level_info[it->table->n-1]);
     if ((it->table->n == 1) && (it_top->fragment != NULL)) {
        tweak = it_top->fixed_prefix;
        if (tweak > 0) tweak += 2;
     }

log_printf(15, "top_level=%d it_top->fragment=%s it_top->path=%s tweak=%d\n", it->table->n-1, it_top->fragment, it_top->path, tweak);
  }

  do {
    if (it->curr_level >= it->table->n) {
      itl = (osf_obj_level_t *)pop(it->recurse_stack);
    } else {
      itl = &(it->level_info[it->curr_level]);
    }

log_printf(0, "curr_level=%d table->n=%d path=%s\n", it->curr_level, it->table->n, itl->path);

    while ((itl->entry = my_readdir(itl->d)) != NULL) {
       itl->prev_pos = itl->curr_pos;
       itl->curr_pos = my_telldir(itl->d);

       i = ((it->curr_level >= it->table->n) || (itl->fragment != NULL)) ? 0 : regexec(itl->preg, itl->entry, 0, NULL, 0);
       if (i == 0) {
          if ((strncmp(itl->entry, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) ||
              (strcmp(itl->entry, ".") == 0) || (strcmp(itl->entry, "..") == 0)) i = 1;
       }
       if (i == 0) { //** Regex match
          snprintf(fname, OS_PATH_MAX, "%s/%s", itl->path, itl->entry);
          snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fname);
          log_printf(15, "POSSIBLE MATCH level=%d table->n=%d fname=%s max_level=%d\n", it->curr_level, it->table->n, fname, it->max_level);

          if (osaz_object_access(osf->osaz, it->creds, fname, OS_MODE_READ_IMMEDIATE) == 1) { //** See if I can access it
             if (it->curr_level < it->max_level) {     //** Cap the recurse depth
                if (it->curr_level < it->table->n-1) { //** Still on the static table
                   i = os_local_filetype(fullname);
                   if (i & OS_OBJECT_DIR) {  //*  Skip normal files since we still have static levels left
                      if (osaz_object_access(osf->osaz, it->creds, fname, OS_MODE_READ_IMMEDIATE) == 1) {  //** Make sure I can access it
                         it->curr_level++;  //** Move to the next level
                         itl = &(it->level_info[it->curr_level]);

                         //** Initialize the level for use
                         strncpy(itl->path, fname, OS_PATH_MAX);
                         itl->d = my_opendir(fullname, itl->fragment);
                      }
                   }
                } else { //** Off the static table or on the last level.  From here on all hits are matches. Just have to check ftype
                   i = os_local_filetype(fullname);
                   log_printf(15, " ftype=%d object_types=%d firstpass=%d\n", i, it->object_types, itl->firstpass);
                   if (i & OS_OBJECT_FILE) {
                       if ((i & it->object_types) > 0) {
                          rmatch = (it->object_regex == NULL) ? 0 : ((obj_fixed != NULL) ? strcmp(itl->entry, obj_fixed) : regexec(it->object_preg, itl->entry, 0, NULL, 0));

                          if (rmatch == 0) { //** IF a match return
                             *myfname=strdup(fname);
                             if (*prefix_len == 0) {
                                *prefix_len = strlen(it_top->path);
                                if (*prefix_len == 0) *prefix_len = tweak;
                             }
                             log_printf(15, "MATCH=%s prefix=%d\n", fname, *prefix_len);
                             if (it->curr_level >= it->table->n) push(it->recurse_stack, itl);  //** Off the static table
                             return(i);
                          }
                       }
                   } else if (i & OS_OBJECT_DIR) {  //** If a dir recurse down
                       if (itl->firstpass == 1) { //** 1st pass so store the pos and recurse
                          itl->firstpass = 0;              //** Flag it as already processed
                          my_seekdir(itl->d, itl->prev_pos);  //** Move the dirp back one slot

                          if (it->curr_level >= it->table->n) push(it->recurse_stack, itl);  //** Off the static table

                          it->curr_level++;  //** Move to the next level which is *always* off the static table

                          //** Make a new level and initialize it for use
                          if (it->curr_level < it->max_level) {
                             type_malloc_clear(itl, osf_obj_level_t, 1);
                             strncpy(itl->path, fname, OS_PATH_MAX);
                             itl->d = my_opendir(fullname, itl->fragment);
                             itl->curr_pos = my_telldir(itl->d);
                             itl->firstpass = 1;
                          } else {                //** Hit max recursion
                             it->curr_level--;
                             if (it->curr_level >= it->table->n) pop(it->recurse_stack);
                          }
                       } else {  //** Already been here so just return the name
                          itl->firstpass = 1;        //** Set up for the next dir
                          if ((i & it->object_types) > 0) {
                             rmatch = (it->object_regex == NULL) ? 0 : ((obj_fixed != NULL) ? strcmp(itl->entry, obj_fixed) : regexec(it->object_preg, itl->entry, 0, NULL, 0));
                             if (rmatch == 0) { //** IF a match return
                                if (*prefix_len == 0) {
                                   *prefix_len = strlen(it_top->path);
                                   if (*prefix_len == 0) *prefix_len = tweak;
                                }
                                *myfname=strdup(fname);
                                log_printf(15, "MATCH=%s prefix=%d\n", fname, *prefix_len);
                                if (it->curr_level >= it->table->n) push(it->recurse_stack, itl);  //** Off the static table
                                return(i);
                             }
                          }
                       }
                   }
                }
             }
          }  //** end osaz
       }
    }


    log_printf(15, "DROPPING from level=%d table->n=%d fname=%s max_level=%d\n", it->curr_level, it->table->n, itl->path, it->max_level);

    my_closedir(itl->d);
    itl->d = NULL;
    if (it->curr_level >= it->table->n) free(itl);
    it->curr_level--;
  } while (it->curr_level >= 0);

  it->finished = 1;

  *myfname = NULL;
  return(0);
}




//***********************************************************************
// osf_purge_dir - Removes all files from the path and will recursively
//     purge subdirs based o nteh recursion depth
//***********************************************************************

int osf_purge_dir(object_service_fn_t *os, char *path, int depth)
{
  int ftype;
  char fname[OS_PATH_MAX];
  DIR *d;
  struct dirent *entry;

  d = opendir(path);
  if (d == NULL) return(1);

  while ((entry = readdir(d)) != NULL) {
     snprintf(fname, OS_PATH_MAX, "%s/%s", path, entry->d_name);
     ftype = os_local_filetype(fname);
     if (ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) {
        safe_remove(os, fname);
     } else if (ftype & OS_OBJECT_DIR) {
       if (depth > 0) {
          osf_purge_dir(os, fname, depth-1);
          safe_remove(os, fname);
       }
     }
  }

  closedir(d);

  return(0);
}

//***********************************************************************
// osfile_free_mk_mv_rm
//***********************************************************************

void osfile_free_mk_mv_rm(void *arg)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;

  if (op->src_path != NULL) free(op->src_path);
  if (op->dest_path != NULL) free(op->dest_path);
  if (op->id != NULL) free(op->id);

  free(op);
}

//***********************************************************************
// osf_object_remove - Removes the current dir or object (non-recursive)
//***********************************************************************

int osf_object_remove(object_service_fn_t *os, char *path)
{
  int ftype, err;
  char *dir, *base, *hard_inode;
  struct stat s;
  char fattr[OS_PATH_MAX];

  ftype = os_local_filetype(path);
  hard_inode = NULL;

log_printf(15, "ftype=%d path=%s\n", ftype, path);

  if (ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK|OS_OBJECT_HARDLINK)) { //** It's a file
log_printf(15, "file or link removal\n");
     if (ftype & OS_OBJECT_HARDLINK) {  //** If this is the last hardlink we need to remove the hardlink inode as well
        memset(&s, 0, sizeof(s));
        err = stat(path, &s);
        if (s.st_nlink <= 2) {  //** Yep we have to remove it
           hard_inode = resolve_hardlink(os, path, 0);
        }
     }

     remove(path);  //** Remove the file
     os_path_split(path, &dir, &base);
     snprintf(fattr, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     if ((ftype & OS_OBJECT_HARDLINK) == 0) {
        osf_purge_dir(os, fattr, 0);
     }
     remove(fattr);
     free(dir); free(base);

     if (hard_inode != NULL) {  //** Remove the hard inode as well
       err = osf_object_remove(os, hard_inode);
       free(hard_inode);
       return(err);
     }
  } else if (ftype & OS_OBJECT_DIR) {  //** A directory
log_printf(15, "dir removal\n");
     osf_purge_dir(os, path, 0);  //** Removes all the files
     snprintf(fattr, OS_PATH_MAX, "%s/%s", path,  FILE_ATTR_PREFIX);
     osf_purge_dir(os, fattr, 1);
     safe_remove(os, fattr);
     safe_remove(os, path);
  }

  return(0);

}

//***********************************************************************
// osfile_remove_object - Removes an object
//***********************************************************************

op_status_t osfile_remove_object_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  int ftype;
  char fname[OS_PATH_MAX];
  op_status_t status;
  apr_thread_mutex_t *lock;

  if (osaz_object_remove(osf->osaz, op->creds, op->src_path) == 0)  return(op_failure_status);
  snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);

  lock = osf_retrieve_lock(op->os, op->src_path, NULL);
  osf_obj_lock(lock);


  ftype = os_local_filetype(fname);
  if (ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) {  //** Regular file so rm the attributes dir and the object
      log_printf(15, "Simple file removal: fname=%s\n", op->src_path);
      status = (osf_object_remove(op->os, fname) == 0) ? op_success_status : op_failure_status;
  } else {  //** Directory so make sure it's empty
    if (osf_is_empty(fname) != 1) {
       osf_obj_unlock(lock);
       log_printf(15, "Oops! trying to remove a non-empty dir: fname=%s ftype=%d\n", op->src_path, ftype);
       return(op_failure_status);
    }

    log_printf(15, "Remove an empty dir: fname=%s\n", op->src_path);

    //** The directory is empty so can safely remove it
    status = (osf_object_remove(op->os, fname) == 0) ? op_success_status : op_failure_status;
  }

  osf_obj_unlock(lock);

  return(status);
}

//***********************************************************************
// osfile_remove_object - Makes a remove object operation
//***********************************************************************

op_generic_t *osfile_remove_object(object_service_fn_t *os, creds_t *creds, char *path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(path);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_remove_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_remove_regex_fn - Does the actual bulk object removal
//***********************************************************************

op_status_t osfile_remove_regex_fn(void *arg, int id)
{
  osfile_remove_regex_op_t *op = (osfile_remove_regex_op_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  osfile_mk_mv_rm_t rm_op;
  os_object_iter_t *it;
  int prefix_len;
  char *fname;
  op_status_t status, op_status;

  rm_op.os = op->os;
  rm_op.creds = op->creds;

  status = op_success_status;

  it = osfile_create_object_iter(op->os, op->creds, op->rpath, op->object_regex, op->obj_types, NULL, op->recurse_depth, NULL, 0);

  while (osfile_next_object(it, &fname, &prefix_len) > 0) {
log_printf(15, "removing fname=%s\n", fname);
     if (osaz_object_remove(osf->osaz, op->creds, fname) == 0) {
        status.op_status = OP_STATE_FAILURE;
        status.error_code++;
     } else {
        rm_op.src_path = fname;
        op_status = osfile_remove_object_fn(&rm_op, 0);
        if (op_status.op_status != OP_STATE_SUCCESS) {
           status.op_status = OP_STATE_FAILURE;
           status.error_code++;
        }
     }

     free(fname);
  }

  osfile_destroy_object_iter(it);

  return(status);
}

//***********************************************************************
// osfile_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************


op_generic_t *osfile_remove_regex_object(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_remove_regex_op_t *op;

  type_malloc(op, osfile_remove_regex_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->rpath = path;
  op->object_regex = object_regex;
  op->recurse_depth = recurse_depth;
  op->obj_types = obj_types;
  return(new_thread_pool_op(osf->tpc, NULL, osfile_remove_regex_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_regex_object_set_multiple_attrs - Recursivley sets the fixed attibutes
//***********************************************************************

op_status_t osfile_regex_object_set_multiple_attrs_fn(void *arg, int id)
{
  osfile_regex_object_attr_op_t *op = (osfile_regex_object_attr_op_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  os_object_iter_t *it;
  char *fname;
  op_status_t status, op_status;
  osfile_attr_op_t op_attr;
  osfile_fd_t *fd;
  osfile_open_op_t op_open;
  int prefix_len;

  op_attr.os = op->os;
  op_attr.creds = op->creds;
  op_attr.fd = fd; //** filled if for each object
  op_attr.key = op->key;
  op_attr.val = op->val;
  op_attr.v_size = op->v_size;
  op_attr.n = op->n_keys;

  op_open.os = op->os;
  op_open.creds = op->creds;
  op_open.path = NULL;  //** Filled in for each open
  op_open.id = op->id;
  op_open.fd = &fd;
  op_open.mode = OS_MODE_READ_IMMEDIATE;
  op_open.id = NULL;
  op_open.uuid = 0;
  get_random(&(op_open.uuid), sizeof(op_open.uuid));
  op_open.max_wait = 0;

  status = op_success_status;

  it = osfile_create_object_iter(op->os, op->creds, op->rpath, op->object_regex, op->object_types, NULL, op->recurse_depth, NULL, 0);

  while (osfile_next_object(it, &fname, &prefix_len) > 0) {
     if (osaz_object_access(osf->osaz, op->creds, fname, OS_MODE_WRITE_IMMEDIATE) == 0) {
        status.op_status = OP_STATE_FAILURE;
        status.error_code += op->n_keys;
     } else {
        op_open.path = strdup(fname);
        op_status = osfile_open_object_fn(&op_open, 0);
        if (op_status.op_status != OP_STATE_SUCCESS) {
           status.op_status = OP_STATE_FAILURE;
           status.error_code += op->n_keys;
        } else {
           op_attr.fd = fd;
           op_status = osf_set_multiple_attr_fn(&op_attr, 0);
           if (op_status.op_status != OP_STATE_SUCCESS) {
              status.op_status = OP_STATE_FAILURE;
              status.error_code++;
           }

           op_open.cfd = fd;
           osfile_close_object_fn((void *)&op_open, 0);  //** Got to close it as well
        }
     }

     free(fname);
  }

  osfile_destroy_object_iter(it);

  return(status);
}

//***********************************************************************
// osfile_regex_object_set_multiple_attrs - Does a bulk regex change attr.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************


op_generic_t *osfile_regex_object_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_regex_object_attr_op_t *op;

  type_malloc(op, osfile_regex_object_attr_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->id = id;
  op->rpath = path;
  op->object_regex = object_regex;
  op->recurse_depth = recurse_depth;
  op->key = key;
  op->val = val;
  op->v_size = v_size;
  op->n_keys = n_attrs;
  op->object_types = object_types;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_regex_object_set_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_exists_fn - Check for file type and if it exists
//***********************************************************************

op_status_t osfile_exists_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  char fname[OS_PATH_MAX];
  op_status_t status = op_success_status;

  if (osaz_object_access(osf->osaz, op->creds, op->src_path, OS_MODE_READ_IMMEDIATE) == 0)  return(op_failure_status);

  snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
  status.error_code = os_local_filetype(fname);
log_printf(15, "fname=%s  ftype=%d\n", fname, status.error_code);
  if (status.error_code == 0) status.op_status = OP_STATE_FAILURE;

  return(status);
}

//***********************************************************************
//  osfile_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

op_generic_t *osfile_exists(object_service_fn_t *os, creds_t *creds, char *path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  if (path == NULL) return(gop_dummy(op_failure_status));

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(path);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_exists_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}



//***********************************************************************
// osfile_create_object_fn - Does the actual object creation
//***********************************************************************

op_status_t osfile_create_object_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  FILE *fd;
  int err;
  char *dir, *base;
  char fname[OS_PATH_MAX];
  char fattr[OS_PATH_MAX];
  apr_thread_mutex_t *lock;

  if (osaz_object_create(osf->osaz, op->creds, op->src_path) == 0)  return(op_failure_status);

  snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);

log_printf(0, "base=%s src=%s fname=%s\n", osf->file_path, op->src_path, fname);

  lock = osf_retrieve_lock(op->os, op->src_path, NULL);
  osf_obj_lock(lock);

  if (op->type == OS_OBJECT_FILE) {
     fd = fopen(fname, "w");
     if (fd == NULL) {
        osf_obj_unlock(lock);
        return(op_failure_status);
     }

     fclose(fd);

     //** Also need to make the attributes directory
     os_path_split(fname, &dir, &base);
     snprintf(fattr, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     err = mkdir(fattr, DIR_PERMS);
     if (err != 0) {
        log_printf(0, "Error creating object attr directory! path=%s full=%s\n", op->src_path, fattr);
        safe_remove(op->os, fname);
        free(dir); free(base);
        osf_obj_unlock(lock);
        return(op_failure_status);
     } else {
        free(dir); free(base);
     }
  } else {  //** Directory object
     err = mkdir(fname, DIR_PERMS);
     if (err != 0) {
        osf_obj_unlock(lock);
        return(op_failure_status);
     }

     //** Also need to make the attributes directory
     snprintf(fattr, OS_PATH_MAX, "%s/%s", fname, FILE_ATTR_PREFIX);
     err = mkdir(fattr, DIR_PERMS);
     if (err != 0) {
        log_printf(0, "Error creating object attr directory! path=%s full=%s\n", op->src_path, fattr);
        safe_remove(op->os, fname);
        osf_obj_unlock(lock);
        return(op_failure_status);
     }
  }

  osf_obj_unlock(lock);

  return(op_success_status);
}

//***********************************************************************
// osfile_create_object - Creates an object
//***********************************************************************

op_generic_t *osfile_create_object(object_service_fn_t *os, creds_t *creds, char *path, int type, char *id)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(path);
  op->type = type;
  op->id = (id != NULL) ? strdup(id) : NULL;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_create_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_symlink_object_fn - Symlink two objects
//***********************************************************************

op_status_t osfile_symlink_object_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  osfile_mk_mv_rm_t dop;
  op_status_t status;
  char sfname[OS_PATH_MAX];
  char dfname[OS_PATH_MAX];
  int err;

  if (osaz_object_create(osf->osaz, op->creds, op->dest_path) == 0) return(op_failure_status);

  //** Verify the source exists
//  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
//  err = os_local_filetype(sfname);
//  if (err == 0) {
//     log_printf(15, "ERROR source file missing sfname=%s dfname=%s\n", op->src_path, op->dest_path);
//     return(op_failure_status);
//  }

  //** Create the object like normal
  dop.os = op->os;
  dop.creds = op->creds;
  dop.src_path = op->dest_path;
  dop.type = OS_OBJECT_FILE;
  dop.id = op->id;
  status = osfile_create_object_fn(&dop, id);
  if (status.op_status != OP_STATE_SUCCESS) {
     log_printf(15, "Failed creating the dest object: %s\n", op->dest_path);
     return(op_failure_status);
  }

  //** Now remove the placeholder and replace it with the link
//  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
  if (op->src_path[0] == '/') {
     snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
  } else {
     snprintf(sfname, OS_PATH_MAX, "%s", op->src_path);
  }
  snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, op->dest_path);

log_printf(15, "sfname=%s dfname=%s\n", sfname, dfname);
  err = safe_remove(op->os, dfname);
  if (err != 0) log_printf(15, "Failed removing dest place holder %s  err=%d\n", dfname, err);

  err = symlink(sfname, dfname);
  if (err != 0) log_printf(15, "Failed making symlink %s -> %s  err=%d\n", sfname, dfname, err);

  return((err == 0) ? op_success_status : op_failure_status);
}


//***********************************************************************
// osfile_symlink_object - Generates a symbolic link object operation
//***********************************************************************

op_generic_t *osfile_symlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  //** Make sure the files are different
  if (strcmp(src_path, dest_path) == 0) {  return(gop_dummy(op_failure_status)); }

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(src_path);
  op->dest_path = strdup(dest_path);
  op->id = (id == NULL) ? NULL : strdup(id);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_symlink_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osf_file2hardlink - Converts a normal file to a hardlink version
//***********************************************************************

int osf_file2hardlink(object_service_fn_t *os, char *src_path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  int slot, i;
  ex_id_t id;
  char *sattr, *hattr;
  char fullname[OS_PATH_MAX], sfname[OS_PATH_MAX];

  //** IF the src is a symlink we need to get that 

  //** Pick a hardlink location
  id = 0;
  get_random(&id, sizeof(id));
  slot = atomic_counter(&(osf->hardlink_count)) % osf->hardlink_dir_size;
  snprintf(fullname, OS_PATH_MAX, "%s/%d/" XIDT, osf->hardlink_path, slot, id);
  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, src_path);
  hattr = object_attr_dir(os, "", fullname, OS_OBJECT_FILE);
  sattr = object_attr_dir(os, osf->file_path, src_path, OS_OBJECT_FILE);

  //** Move the src attr dir to the hardlink location
  i = rename(sattr, hattr);
log_printf(0, "rename(%s,%s)=%d\n", sattr, hattr, i);

  if (i != 0) {
     log_printf(0, "rename(%s,%s) FAILED!\n", sattr, hattr);
     free(hattr); free(sattr);
     return(1);
  }

  //** Link the src attr dir with the hardlink
  i = symlink(hattr, sattr);
log_printf(0, "symlink(%s,%s)=%d!\n", hattr, sattr, i);
  if (i != 0) {
     log_printf(0, "symlink(%s,%s) FAILED!\n", hattr, sattr);
     free(hattr); free(sattr);
     return(1);
  }
  free(hattr); free(sattr);


  //** Move the source to the hardlink proxy
  i = rename(sfname, fullname);
log_printf(0, "rename(%s,%s)=%d\n", sfname, fullname, i);
  if (i != 0) {
     log_printf(0, "rename(%s,%s) FAILED!\n", sfname, fullname);
     return(1);
  }

  //** Link the src file to the hardlink proxy
  i = link(fullname, sfname);
log_printf(0, "link(%s,%s)=%d\n", fullname, sfname, i);
  if (i != 0) {
     log_printf(0, "link(%s,%s) FAILED!\n", fullname, sfname);
     return(1);
  }

  return(0);
}


//***********************************************************************
// resolve_hardlink - DEtermines which object in the hard link dir the object
//  points to
//***********************************************************************

char *resolve_hardlink(object_service_fn_t *os, char *src_path, int add_prefix)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  char *hpath, *tmp;
  char buffer[OS_PATH_MAX];
  int n, i;

  if (add_prefix == 1) {
     hpath = object_attr_dir(os, osf->file_path, src_path, OS_OBJECT_FILE);
  } else {
     hpath = object_attr_dir(os, "", src_path, OS_OBJECT_FILE);
  }

  n = readlink(hpath, buffer, OS_PATH_MAX-1);
  if (n <= 0) {
     log_printf(0, "Readlink error!  src_path=%s hpath=%s\n", src_path, hpath);
     return(NULL);
  }
  free(hpath);

  buffer[n] = 0;
  log_printf(15, "file_path=%s fullname=%s link=%s\n", osf->file_path, src_path, buffer);

//  offset = osf->hardlink_path_len;
//  hpath = &(buffer[osf->hardlink_path_len]);
  hpath = buffer;
  tmp = strstr(hpath, FILE_ATTR_PREFIX "/" FILE_ATTR_PREFIX);
  n = FILE_ATTR_PREFIX_LEN + 1 + FILE_ATTR_PREFIX_LEN;
  for (i=0; tmp[i+n] != 0; i++) tmp[i] = tmp[i+n];
  tmp[i] = 0;

  log_printf(15, "fullname=%s link=%s\n", src_path, tmp);

  return(strdup(hpath));
}

//***********************************************************************
// osfile_hardlink_object_fn - hard links two objects
//***********************************************************************

op_status_t osfile_hardlink_object_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  op_status_t status;
  apr_thread_mutex_t *hlock, *dlock;
  int hslot, dslot;
  char sfname[OS_PATH_MAX];
  char dfname[OS_PATH_MAX];
  char *sapath, *dapath, *link_path;
  int err, ftype;

  if ((osaz_object_access(osf->osaz, op->creds, op->src_path, OS_MODE_READ_IMMEDIATE) == 0) ||
      (osaz_object_create(osf->osaz, op->creds, op->dest_path) == 0)) return(op_failure_status);

  //** Verify the source exists
  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
  ftype = os_local_filetype(sfname);
  if (ftype == 0) {
     log_printf(15, "ERROR source file missing sfname=%s dfname=%s\n", op->src_path, op->dest_path);
     return(op_failure_status);
  }

  //** Check if the source is already a hardlink
  if ((ftype & OS_OBJECT_HARDLINK) == 0) { //** If not convert it to a hard link
     err = osf_file2hardlink(op->os, op->src_path);
     if (err != 0) {
        log_printf(15, "ERROR converting source file to a hard link sfname=%s\n", op->src_path);
       return(op_failure_status);
     }

  }

  //** Resolve the hardlink by looking at the src objects attr path
  link_path = resolve_hardlink(op->os, op->src_path, 1);
  if (link_path == NULL) {
     log_printf(15, "ERROR resolving src hard link sfname=%s dfname=%s\n", op->src_path, op->dest_path);
     free(link_path);
     return(op_failure_status);
  }

  //** Make the dest path
  snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, op->dest_path);

  //** Acquire the locks
  hlock = osf_retrieve_lock(op->os, link_path, &hslot);
  dlock = osf_retrieve_lock(op->os, dfname, &dslot);
  if (hslot < dslot) {
    apr_thread_mutex_lock(hlock);
    apr_thread_mutex_lock(dlock);
  } else if (hslot > dslot) {
    apr_thread_mutex_lock(dlock);
    apr_thread_mutex_lock(hlock);
  } else {
    apr_thread_mutex_lock(hlock);
  }

  //** Hardlink the proxy
  if (link(link_path, dfname) != 0) {
     log_printf(15, "ERROR making proxy hardlink link_path=%s sfname=%s dfname=%s\n", link_path, op->src_path, dfname);
     status = op_failure_status;
     goto finished;
  }

  //** Symlink the attr dirs together
  sapath = object_attr_dir(op->os, "", link_path, OS_OBJECT_FILE);
  dapath = object_attr_dir(op->os, osf->file_path, op->dest_path, OS_OBJECT_FILE);
  if (symlink(sapath, dapath) != 0) {
     unlink(dfname);
     free(sapath);  free(dapath);
     log_printf(15, "ERROR making proxy hardlink link_path=%s sfname=%s dfname=%s\n", link_path, op->src_path, op->dest_path);
     status = op_failure_status;
     goto finished;
  }
  free(sapath);  free(dapath);

  status = op_success_status;

finished:
  apr_thread_mutex_unlock(hlock);
  if (hslot != dslot)  apr_thread_mutex_unlock(dlock);
  free(link_path);

  return(status);
}


//***********************************************************************
// osfile_hardlink_object - Generates a hard link object operation
//***********************************************************************

op_generic_t *osfile_hardlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  //** Make sure the files are different
  if (strcmp(src_path, dest_path) == 0) {  return(gop_dummy(op_failure_status)); }

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(src_path);
  op->dest_path = strdup(dest_path);
  op->id = (id == NULL) ? NULL : strdup(id);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_hardlink_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_move_object_fn - Actually Moves an object
//***********************************************************************

op_status_t osfile_move_object_fn(void *arg, int id)
{
  osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  int ftype;
  char sfname[OS_PATH_MAX];
  char dfname[OS_PATH_MAX];
  char *dir, *base;
  int err;

  if ((osaz_object_remove(osf->osaz, op->creds, op->src_path) == 0) ||
      (osaz_object_create(osf->osaz, op->creds, op->dest_path) == 0)) return(op_failure_status);

  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
  snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, op->dest_path);

  ftype = os_local_filetype(sfname);

  err = rename(sfname, dfname);  //** Move the file/dir
  log_printf(15, "sfname=%s dfname=%s err=%d\n", sfname, dfname, err);

  if ((ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) && (err==0)) { //** File move
     //** Also need to move the attributes entry
     os_path_split(sfname, &dir, &base);
     snprintf(sfname, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     free(dir); free(base);
     os_path_split(dfname, &dir, &base);
     snprintf(dfname, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     free(dir); free(base);

     log_printf(15, "ATTR sfname=%s dfname=%s\n", sfname, dfname);

     err = rename(sfname, dfname);
  }

  return((err == 0) ? op_success_status : op_failure_status);
}

//***********************************************************************
// osfile_move_object - Generates a move object operation
//***********************************************************************

op_generic_t *osfile_move_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_mk_mv_rm_t *op;

  type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = strdup(src_path);
  op->dest_path = strdup(dest_path);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_move_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}


//***********************************************************************
// osfile_copy_multiple_attrs_fn - Actually copies the object attrs
//***********************************************************************

op_status_t osfile_copy_multiple_attrs_fn(void *arg, int id)
{
  osfile_copy_attr_t *op = (osfile_copy_attr_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  op_status_t status;
  apr_thread_mutex_t *lock_src, *lock_dest;
  void *val;
  int v_size;
  int slot_src, slot_dest;
  int i, err, atype;

  //** Lock the individual objects based on their slot positions to avoid a deadlock
  lock_src = osf_retrieve_lock(op->os, op->fd_src->object_name, &slot_src);
  lock_dest = osf_retrieve_lock(op->os, op->fd_dest->object_name, &slot_dest);
  if (slot_src < slot_dest) {
     osf_obj_lock(lock_src);
     osf_obj_lock(lock_dest);
  } else if (slot_src > slot_dest) {
     osf_obj_lock(lock_dest);
     osf_obj_lock(lock_src);
  } else {  //** Same slot so only need to lock one
     lock_dest = NULL;
     osf_obj_lock(lock_src);
  }


  status = op_success_status;
  for (i=0; i<op->n; i++) {
  log_printf(15, " fsrc=%s (lock=%d) fdest=%s (lock=%d)   n=%d key_src[0]=%s key_dest[0]=%s\n", op->fd_src->object_name, slot_src, op->fd_dest->object_name, slot_dest, op->n, op->key_src[i], op->key_dest[i]);
    if ((osaz_attr_access(osf->osaz, op->creds, op->fd_src->object_name, op->key_src[i], OS_MODE_READ_IMMEDIATE) == 1) &&
        (osaz_attr_create(osf->osaz, op->creds, op->fd_dest->object_name, op->key_dest[i]) == 1)) {

        v_size = -osf->max_copy;
        val = NULL;
        err = osf_get_attr(op->os, op->creds, op->fd_src, op->key_src[i], &val, &v_size, &atype);
        if (err == 0) {
           err = osf_set_attr(op->os, op->creds, op->fd_dest, op->key_dest[i], val, v_size, &atype);
           free(val);
           if (err != 0) {
              status.op_status = OP_STATE_FAILURE;
              status.error_code++;
           }
        } else {
           status.op_status = OP_STATE_FAILURE;
           status.error_code++;
        }
    } else {
      status.op_status = OP_STATE_FAILURE;
      status.error_code++;
    }
  }

  osf_obj_unlock(lock_src);
  if (lock_dest != NULL) osf_obj_unlock(lock_dest);

  log_printf(15, "fsrc=%s fdest=%s err=%d\n", op->fd_src->object_name, op->fd_dest->object_name, status.error_code);

  return(status);
}

//***********************************************************************
// osfile_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

op_generic_t *osfile_copy_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_copy_attr_t *op;

  type_malloc_clear(op, osfile_copy_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd_src = (osfile_fd_t *)fd_src;
  op->fd_dest = (osfile_fd_t *)fd_dest;
  op->key_src = key_src;
  op->key_dest = key_dest;
  op->n = n;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_copy_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_copy_attr - Generates a copy object attribute operation
//***********************************************************************

op_generic_t *osfile_copy_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_copy_attr_t *op;

  type_malloc_clear(op, osfile_copy_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd_src = (osfile_fd_t *)fd_src;
  op->fd_dest = (osfile_fd_t *)fd_dest;
  op->key_src = &(op->single_src);  op->single_src = key_src;
  op->key_dest = &(op->single_dest);  op->single_dest = key_dest;
  op->n = 1;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_copy_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_symlink_multiple_attrs_fn - Actually links the multiple attrs
//***********************************************************************

op_status_t osfile_symlink_multiple_attrs_fn(void *arg, int id)
{
  osfile_copy_attr_t *op = (osfile_copy_attr_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  op_status_t status;
  apr_thread_mutex_t *lock_dest;
  char sfname[OS_PATH_MAX];
  char dfname[OS_PATH_MAX];
  int slot_dest;
  int i, err;

  //** Lock the source
  lock_dest = osf_retrieve_lock(op->os, op->fd_dest->object_name, &slot_dest);
  osf_obj_lock(lock_dest);

  log_printf(15, " fsrc[0]=%s fdest=%s (lock=%d)   n=%d key_src[0]=%s key_dest[0]=%s\n", op->src_path[0], op->fd_dest->object_name, slot_dest, op->n, op->key_src[0], op->key_dest[0]);

  status = op_success_status;
  for (i=0; i<op->n; i++) {
    if (osaz_attr_create(osf->osaz, op->creds, op->fd_dest->object_name, op->key_dest[i]) == 1) {

       osf_make_attr_symlink(op->os, sfname, op->src_path[i], op->key_src[i]);
       snprintf(dfname, OS_PATH_MAX, "%s/%s", op->fd_dest->attr_dir, op->key_dest[i]);

log_printf(15, "sfname=%s dfname=%s\n", sfname, dfname);

       err = symlink(sfname, dfname);
       if (err != 0) {
          log_printf(15, "Failed making symlink %s -> %s  err=%d\n", sfname, dfname, err);
          status.op_status = OP_STATE_FAILURE;
          status.error_code++;
       }

    } else {
      status.op_status = OP_STATE_FAILURE;
      status.error_code++;
    }
  }

  osf_obj_unlock(lock_dest);

  log_printf(15, "fsrc[0]=%s fdest=%s err=%d\n", op->src_path[0], op->fd_dest->object_name, status.error_code);

  return(status);
}

//***********************************************************************
// osfile_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

op_generic_t *osfile_symlink_multiple_attrs(object_service_fn_t *os, creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_copy_attr_t *op;

  type_malloc_clear(op, osfile_copy_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = src_path;
  op->fd_dest = (osfile_fd_t *)fd_dest;
  op->key_src = key_src;
  op->key_dest = key_dest;
  op->n = n;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_symlink_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_symlink_attr - Generates a link attribute operation
//***********************************************************************

op_generic_t *osfile_symlink_attr(object_service_fn_t *os, creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_copy_attr_t *op;

  type_malloc_clear(op, osfile_copy_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->src_path = &(op->single_path); op->single_path = src_path;
  op->fd_dest = (osfile_fd_t *)fd_dest;
  op->key_src = &(op->single_src);  op->single_src = key_src;
  op->key_dest = &(op->single_dest);  op->single_dest = key_dest;
  op->n = 1;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_symlink_multiple_attrs_fn, (void *)op, free, 1));
}


//***********************************************************************
// osfile_move_multiple_attrs_fn - Actually Moves the object attrs
//***********************************************************************

op_status_t osfile_move_multiple_attrs_fn(void *arg, int id)
{
  osfile_move_attr_t *op = (osfile_move_attr_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  os_virtual_attr_t *va1, *va2;
  op_status_t status;
  apr_thread_mutex_t *lock;
  int i, err;
  char sfname[OS_PATH_MAX];
  char dfname[OS_PATH_MAX];

  lock = osf_retrieve_lock(op->os, op->fd->object_name, NULL);
  osf_obj_lock(lock);

  status = op_success_status;
  for (i=0; i<op->n; i++) {
    if ((osaz_attr_create(osf->osaz, op->creds, op->fd->object_name, op->key_new[i]) == 1) &&
        (osaz_attr_remove(osf->osaz, op->creds, op->fd->object_name, op->key_old[i]) == 1)) {

        //** Do a Virtual Attr check
        va1 = apr_hash_get(osf->vattr_hash, op->key_old[i], APR_HASH_KEY_STRING);
        va2 = apr_hash_get(osf->vattr_hash, op->key_new[i], APR_HASH_KEY_STRING);
        if ((va1 != NULL) || (va2 != NULL)) {
          err = 1;
        } else {
           snprintf(sfname, OS_PATH_MAX, "%s/%s", op->fd->attr_dir, op->key_old[i]);
           snprintf(dfname, OS_PATH_MAX, "%s/%s", op->fd->attr_dir, op->key_new[i]);
           err = rename(sfname, dfname);
        }

        if (err != 0) {
           status.op_status = OP_STATE_FAILURE;
           status.error_code++;
        }
    } else {
      status.op_status = OP_STATE_FAILURE;
      status.error_code++;
    }
  }

  osf_obj_unlock(lock);

  return(status);
}

//***********************************************************************
// osfile_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

op_generic_t *osfile_move_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_move_attr_t *op;

  type_malloc_clear(op, osfile_move_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)fd;
  op->key_old = key_old;
  op->key_new = key_new;
  op->n = n;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_move_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_move_attr - Generates a move object attribute operation
//***********************************************************************

op_generic_t *osfile_move_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_move_attr_t *op;

  type_malloc_clear(op, osfile_move_attr_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)fd;
  op->key_old = &(op->single_old);  op->single_old = key_old;
  op->key_new = &(op->single_new);  op->single_new = key_new;
  op->n = 1;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_move_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osf_get_attr - Gets the attribute given the name and base directory
//***********************************************************************

int osf_get_attr(object_service_fn_t *os, creds_t *creds, osfile_fd_t *ofd, char *attr, void **val, int *v_size, int *atype)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  os_virtual_attr_t *va;
  list_iter_t it;
  char *ca;
  FILE *fd;
  char fname[OS_PATH_MAX];
  int n, bsize;

//log_printf(15, "PTR val=%p *val=%p\n", val, *val);

  if (osaz_attr_access(osf->osaz, creds, ofd->object_name, attr, OS_MODE_READ_BLOCKING) == 0) { *atype = 0; return(1); }

  //** Do a Virtual Attr check
  //** Check the prefix VA's first
  it = list_iter_search(osf->vattr_prefix, attr, -1);
  list_next(&it, (list_key_t **)&ca, (list_data_t **)&va);

  if (va != NULL) {
     n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
//     log_printf(15, "va=%s attr=%s n=%d\n", va->attribute, attr, n);
//int d=strncmp(attr, va->attribute, n);
//     log_printf(15, "strncmp=%d\n", d);
     if (strncmp(attr, va->attribute, n) == 0) {  //** Prefix matches
        return(va->get(va, os, creds, ofd, attr, val, v_size, atype));
     }
  }

  //** Now check the normal VA's
  va = apr_hash_get(osf->vattr_hash, attr, APR_HASH_KEY_STRING);
  if (va != NULL) { return(va->get(va, os, creds, ofd, attr, val, v_size, atype)); }


  //** Lastly look at the actual attributes
  n = osf_resolve_attr_path(os, fname, ofd->object_name, attr, ofd->ftype, atype, 20);
//  snprintf(fname, OS_PATH_MAX, "%s/%s", ofd->attr_dir, attr);
log_printf(15, "fname=%s *v_size=%d resolve=%d\n", fname, *v_size, n);
  if (n != 0) {
     if (*v_size < 0) *val = NULL;
     *v_size = -1;
     return(1);
  }

  *atype = os_local_filetype(fname);

  fd = fopen(fname, "r");
  if (fd == NULL) {
     if (*v_size < 0) *val = NULL;
     *v_size = -1;
     return(1);
  }

  if (*v_size < 0) { //** Need to determine the size
     fseek(fd, 0L, SEEK_END);
     n = ftell(fd);
     fseek(fd, 0L, SEEK_SET);
     *v_size = (n > (-*v_size)) ? -*v_size : n;
     bsize = *v_size + 1;
log_printf(15, " adjusting v_size=%d n=%d\n", *v_size, n);
     *val = malloc(bsize);
  } else {
     bsize = *v_size;
  }

  *v_size = fread(*val, 1, *v_size, fd);
//  if (add_term == 1) (*val)[*v_size] = 0;  //** Add a NULL terminator in case it may be a string
  if (bsize > *v_size) {ca = (char *)(*val); ca[*v_size] = 0; } //** Add a NULL terminator in case it may be a string

log_printf(15, "PTR val=%p *val=%s\n", val, (char *)(*val));

  fclose(fd);

  return(0);
}

//***********************************************************************
// osf_get_ma_links - Does the actual attribute retreival when links are
//       encountered
//***********************************************************************

op_status_t osf_get_ma_links(void *arg, int id, int first_link)
{
  osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
  int err, i, atype, n_locks;
  apr_thread_mutex_t *lock_table[op->n+1];
  op_status_t status;

  status = op_success_status;

  osf_multi_lock(op->os, op->creds, op->fd, op->key, op->n, first_link, lock_table, &n_locks);

  err = 0;
  for (i=0; i<op->n; i++) {
    err += osf_get_attr(op->os, op->creds, op->fd, op->key[i], (void **)&(op->val[i]), &(op->v_size[i]), &atype);
if (op->v_size[i] > 0) {
  log_printf(15, "PTR i=%d key=%s val=%s v_size=%d\n", i, op->key[i], (char *)op->val[i], op->v_size[i]);
} else {
  log_printf(15, "PTR i=%d key=%s val=NULL v_size=%d\n", i, op->key[i], op->v_size[i]);
}
  }

  osf_multi_unlock(lock_table, n_locks);

  if (err != 0) status = op_failure_status;

  return(status);
}


//***********************************************************************
// osf_get_multiple_attr_fn - Does the actual attribute retreival
//***********************************************************************

op_status_t osf_get_multiple_attr_fn(void *arg, int id)
{
  osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
//  apr_time_t date;
//  char timestamp[OS_PATH_MAX];
  int err, i, j, atype, v_start[op->n], oops;
  op_status_t status;
  apr_thread_mutex_t *lock;

  status = op_success_status;

  lock = osf_retrieve_lock(op->os, op->fd->object_name, NULL);
  osf_obj_lock(lock);

  err = 0;
  oops = 0;
  for (i=0; i<op->n; i++) {
    v_start[i] = op->v_size[i];
    err += osf_get_attr(op->os, op->creds, op->fd, op->key[i], (void **)&(op->val[i]), &(op->v_size[i]), &atype);
if (op->v_size[i] != 0) {
   log_printf(15, "PTR i=%d key=%s val=%s v_size=%d atype=%d err=%d\n", i, op->key[i], (char *)op->val[i], op->v_size[i], atype, err);
} else {
   log_printf(15, "PTR i=%d key=%s val=NULL v_size=%d atype=%d err=%d\n", i, op->key[i], op->v_size[i], atype, err);
}
    if ((atype & OS_OBJECT_SYMLINK) > 0) {  oops=1;  break; }
  }

  //** Update the access time attribute
//  date = apr_time_sec(apr_time_now());
//  snprintf(timestamp, OS_PATH_MAX, TT "|%s|%s", date, cred_get_id(op->creds), op->fd->id);
//  lowlevel_set_attr(op->os, op->fd->attr_dir, "system.access", timestamp, strlen(timestamp));

  osf_obj_unlock(lock);

  if (oops == 1) { //** Multi object locking required
    for (j=0; j<=i; j++) {  //** Clean up any data allocated
       if (v_start[i] < 0) {
          if (op->val[i] != NULL) { free(op->val[i]); op->val[i] = NULL; }
       }
       op->v_size[i] = v_start[i];
    }

    return(osf_get_ma_links(arg, id, i));
  }

//  if (err != 0) status = op_failure_status;

  return(status);
}

//***********************************************************************
// osfile_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *osfile_get_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_attr_op_t *op;

  type_malloc(op, osfile_attr_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)ofd;
  op->key = &(op->key_tmp);
  op->key_tmp = key;
  op->val = val;
  op->v_size = v_size;
  op->n = 1;

log_printf(15, "PTR val=%p op->val=%p\n", val, op->val);

  return(new_thread_pool_op(osf->tpc, NULL, osf_get_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *osfile_get_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_attr_op_t *op;

  type_malloc(op, osfile_attr_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)ofd;
  op->key = key;
  op->val = val;
  op->v_size= v_size;
  op->n = n;

  return(new_thread_pool_op(osf->tpc, NULL, osf_get_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lowlevel_set_attr - Lowlevel routione to set an attribute without cred checks
//     Designed for use with timestamps or other auto touched fields
//***********************************************************************

int lowlevel_set_attr(object_service_fn_t *os, char *attr_dir, char *attr, void *val, int v_size)
{
  FILE *fd;
  char fname[OS_PATH_MAX];

  snprintf(fname, OS_PATH_MAX, "%s/%s", attr_dir, attr);
  if (v_size < 0) { //** Want to remove the attribute
     safe_remove(os, fname);
  } else {
     fd = fopen(fname, "w");
     if (fd == NULL) return(-1);
     if (v_size > 0) fwrite(val, v_size, 1, fd);
     fclose(fd);
  }

  return(0);
}

//***********************************************************************
// osf_set_attr - Sets the attribute given the name and base directory
//***********************************************************************

int osf_set_attr(object_service_fn_t *os, creds_t *creds, osfile_fd_t *ofd, char *attr, void *val, int v_size, int *atype)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  list_iter_t it;
  FILE *fd;
  os_virtual_attr_t *va;
  int n;
  char *ca;
  char fname[OS_PATH_MAX];

  if (osaz_attr_access(osf->osaz, creds, ofd->object_name, attr, OS_MODE_READ_BLOCKING) == 0) {*atype = 0; return(1); }

  //** Do a Virtual Attr check
  //** Check the prefix VA's first
  it = list_iter_search(osf->vattr_prefix, attr, -1);
  list_next(&it, (list_key_t **)&ca, (list_data_t **)&va);
  if (va != NULL) {
     n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
     if (strncmp(attr, va->attribute, n) == 0) {  //** Prefix matches
        return(va->set(va, os, creds, ofd, attr, val, v_size, atype));
     }
  }

  //** Now check the normal VA's
  va = apr_hash_get(osf->vattr_hash, attr, APR_HASH_KEY_STRING);
  if (va != NULL) { return(va->set(va, os, creds, ofd, attr, val, v_size, atype)); }

  if (v_size < 0) { //** Want to remove the attribute
     if (osaz_attr_remove(osf->osaz, creds, ofd->object_name, attr) == 0) return(1);
     snprintf(fname, OS_PATH_MAX, "%s/%s", ofd->attr_dir, attr);
     safe_remove(os, fname);
     return(0);
  }

  n = osf_resolve_attr_path(os, fname, ofd->object_name, attr, ofd->ftype, atype, 20);
  if (n != 0) {
     return(1);
  }

  //** Store the value
  if (os_local_filetype(fname) != OS_OBJECT_FILE) {
     if (osaz_attr_create(osf->osaz, creds, ofd->object_name, attr) == 0) return(1);
  }
  fd = fopen(fname, "w");
//log_printf(15, "fd=%p\n", fd);
if (fd == NULL) log_printf(0, "ERROR opening attr file attr=%s val=%s v_size=%d fname=%s\n", attr, val, v_size, fname);
  if (fd == NULL) return(-1);
  if (v_size > 0) fwrite(val, v_size, 1, fd);
  fclose(fd);

  return(0);
}

//***********************************************************************
// osf_set_ma_links - Does the actual attribute setting when links are
//       encountered
//***********************************************************************

op_status_t osf_set_multiple_attr_fn(void *arg, int id)
//op_status_t osf_set_ma_links(void *arg, int id, int first_link)
{
  osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
  int err, i, atype, n_locks;
  apr_thread_mutex_t *lock_table[op->n+1];
  op_status_t status;

  status = op_success_status;

  osf_multi_lock(op->os, op->creds, op->fd, op->key, op->n, 0, lock_table, &n_locks);

  err = 0;
  for (i=0; i<op->n; i++) {
    err += osf_set_attr(op->os, op->creds, op->fd, op->key[i], op->val[i], op->v_size[i], &atype);
  }

  osf_multi_unlock(lock_table, n_locks);

  if (err != 0) status = op_failure_status;

  return(status);
}

//***********************************************************************
// osfile_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

op_generic_t *osfile_set_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char *key, void *val, int v_size)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_attr_op_t *op;

  type_malloc(op, osfile_attr_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)ofd;
  op->key = &(op->key_tmp);
  op->key_tmp = key;
  op->val = &(op->val_tmp);
  op->val_tmp = val;
  op->v_size = &(op->v_tmp);
  op->v_tmp = v_size;
  op->n = 1;

  return(new_thread_pool_op(osf->tpc, NULL, osf_set_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

op_generic_t *osfile_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_attr_op_t *op;

  type_malloc(op, osfile_attr_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->fd = (osfile_fd_t *)ofd;
  op->key = key;
  op->val = val;
  op->v_size = v_size;
  op->n = n;

  return(new_thread_pool_op(osf->tpc, NULL, osf_set_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_next_attr - Returns the next matching attribute
//***********************************************************************

int osfile_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
  osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
  osfile_priv_t *osf = (osfile_priv_t *)it->os->priv;
  int i, n, atype;
  apr_ssize_t klen;
  os_virtual_attr_t *va;
  struct dirent *entry;
  os_regex_table_t *rex = it->regex;


//log_printf(15, "va_index=%p\n", it->va_index);

  //** Check the VA's 1st
  while (it->va_index != NULL) {
     apr_hash_this(it->va_index, (const void **)key, &klen, (void **)&va);
     it->va_index = apr_hash_next(it->va_index);
     for (i=0; i<rex->n; i++) {
        n = (rex->regex_entry[i].fixed == 1) ? strcmp(rex->regex_entry[i].expression, va->attribute) : regexec(&(rex->regex_entry[i].compiled), va->attribute, 0, NULL, 0);
        if (n == 0) { //** got a match
           if (osaz_attr_access(osf->osaz, it->creds, it->fd->object_name, va->attribute, OS_MODE_READ_BLOCKING) == 1) {
              *v_size = it->v_max;
              osf_get_attr(it->fd->os, it->creds, it->fd, va->attribute, val, v_size, &atype);
              *key = strdup(va->attribute);
              return(0);
           }
        }
     }

//     it->va_index = apr_hash_next(it->va_index);
  }

  while ((entry = readdir(it->d)) != NULL) {
     for (i=0; i<rex->n; i++) {
//       n = regexec(&(rex->regex_entry[i].compiled), entry->d_name, 0, NULL, 0);
       n = (rex->regex_entry[i].fixed == 1) ? strcmp(rex->regex_entry[i].expression, entry->d_name) : regexec(&(rex->regex_entry[i].compiled), entry->d_name, 0, NULL, 0);
log_printf(15, "key=%s match=%d\n", entry->d_name, n);
       if (n == 0) {
          if ((strncmp(entry->d_name, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) ||
              (strcmp(entry->d_name, ".") == 0) || (strcmp(entry->d_name, "..") == 0)) n = 1;
       }

       if (n == 0) { //** got a match
          if (osaz_attr_access(osf->osaz, it->creds, it->fd->object_name, entry->d_name, OS_MODE_READ_BLOCKING) == 1) {
             *v_size = it->v_max;
             osf_get_attr(it->fd->os, it->creds, it->fd, entry->d_name, val, v_size, &atype);
             *key = strdup(entry->d_name);
log_printf(15, "key=%s val=%s\n", *key, (char *)(*val));
             return(0);
          }
       }
     }
  }

  return(-1);
}

//***********************************************************************
// osfile_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//***********************************************************************

os_attr_iter_t *osfile_create_attr_iter(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, os_regex_table_t *attr, int v_max)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_priv_t *osf = (osfile_priv_t *)fd->os->priv;
  osfile_attr_iter_t *it;

  type_malloc_clear(it, osfile_attr_iter_t, 1);

  it->os = os;
  it->va_index = apr_hash_first(osf->mpool, osf->vattr_hash);
  it->d = opendir(fd->attr_dir);
  it->regex = attr;
  it->fd = fd;
  it->creds = creds;
  it->v_max = v_max;

  return((os_attr_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_attr_iter - Destroys an attribute iterator
//***********************************************************************

void osfile_destroy_attr_iter(os_attr_iter_t *oit)
{
  osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
  if (it->d != NULL) closedir(it->d);

  free(it);
}

//***********************************************************************
// osfile_next_object - Returns the iterators next matching object
//***********************************************************************

int osfile_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
  osf_object_iter_t *it = (osf_object_iter_t *)oit;
  osfile_open_op_t op;
  osfile_attr_op_t aop;
  op_status_t status;
  int ftype;

  ftype = osf_next_object(it, fname, prefix_len);
  log_printf(15, " MATCH=%s\n", *fname);

  if (*fname != NULL) {
     if (it->n_list < 0) {  //** ATtr regex mode
        if (it->it_attr != NULL) {
           if (*(it->it_attr) != NULL) osfile_destroy_attr_iter(*(it->it_attr));
           if (it->fd != NULL) {
              op.os = it->os;
              op.cfd = it->fd;
              status = osfile_close_object_fn((void *)&op, 0);
              it->fd = NULL;
           }

log_printf(15, "making new iterator\n");
           op.os = it->os;
           op.creds = it->creds;
           op.path = strdup(*fname);
           op.fd = (osfile_fd_t **)&(it->fd);
           op.mode = OS_MODE_READ_IMMEDIATE;
           op.id = NULL;
           op.max_wait = 0;
           op.uuid = 0;
           get_random(&(op.uuid), sizeof(op.uuid));
           status = osfile_open_object_fn(&op, 0);
           if (status.op_status != OP_STATE_SUCCESS) return(-1);

log_printf(15, "after object open\n");
           *(it->it_attr) = osfile_create_attr_iter(it->os, it->creds, it->fd, it->attr, it->v_max);
        }
     } else if (it->n_list > 0) {  //** Fixed list mode
        op.os = it->os;
        op.creds = it->creds;
        op.path = strdup(*fname);
        op.fd = (osfile_fd_t **)&(it->fd);
        op.mode = OS_MODE_READ_IMMEDIATE;
        op.id = NULL;
        op.max_wait = 0;
        op.uuid = 0;
        get_random(&(op.uuid), sizeof(op.uuid));
        status = osfile_open_object_fn(&op, 0);
        if (status.op_status != OP_STATE_SUCCESS) return(-1);

        aop.os = it->os;
        aop.creds = it->creds;
        aop.fd = (osfile_fd_t *)it->fd;
        aop.key = it->key;

//        if (it->v_fixed != 1) {
//           for (i=0; i < it->n_list; i++) {  //** Free any allocated memory before the next call
//              if (it->v_size_user[i] < 0) {
//                 if (it->val[i] != NULL) free(it->val[i]);
//              }
//           }
//        }

        aop.val = it->val;
        aop.v_size = it->v_size;
        memcpy(it->v_size, it->v_size_user, sizeof(int)*it->n_list);
        aop.n = it->n_list;
        status = osf_get_multiple_attr_fn(&aop, 0);

        op.os = it->os;
        op.cfd = it->fd;
        status = osfile_close_object_fn((void *)&op, 0);
        it->fd = NULL;

     }

     return(ftype);
  }

  return(0);
}


//***********************************************************************
// osfile_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//***********************************************************************

os_object_iter_t *osfile_create_object_iter(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
                     os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osf_object_iter_t *it;
  osf_obj_level_t *itl;
  int i;

  type_malloc_clear(it, osf_object_iter_t, 1);

  it->os = os;
  it->table = path;
  it->object_regex = object_regex;
  it->recurse_depth = recurse_depth;
  it->max_level = path->n + recurse_depth;
  it->creds = creds;
  it->v_max = v_max;
  it->attr = attr;
  it->it_attr = it_attr;
  if (it_attr != NULL) *it_attr = NULL;
  it->n_list = (it_attr == NULL) ? 0 : -1;  //**  Using the attr iter if -1
  it->recurse_stack = new_stack();
  it->object_types = object_types;

  type_malloc_clear(it->level_info, osf_obj_level_t, it->table->n);
  for (i=0; i<it->table->n; i++) {
    itl = &(it->level_info[i]);
    itl->firstpass = 1;
    itl->preg = &(path->regex_entry[i].compiled);
    if (path->regex_entry[i].fixed == 1) {
       itl->fragment = path->regex_entry[i].expression;
       itl->fixed_prefix = path->regex_entry[i].fixed_prefix;
    }
  }

  if (it->table->n == 1) { //** Single level so check if a fixed path and if so tweak things
    if ((itl->fragment != NULL) && (itl->fixed_prefix > 0)) itl->fixed_prefix--;
  }

  if (object_regex != NULL) it->object_preg = &(object_regex->regex_entry[0].compiled);

  if (it->table->n > 0) {
     itl = &(it->level_info[0]);
     itl->path[0] = '\0';
     itl->d = my_opendir(osf->file_path, itl->fragment);
     itl->curr_pos = my_telldir(itl->d);
     itl->firstpass = 1;
  }

  return((os_object_iter_t *)it);
}

//***********************************************************************
// osfile_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *osfile_create_object_iter_alist(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
                     int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
//  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osf_object_iter_t *it;
  int i;

  //** Use the regex attr version to make the base struct
  it = osfile_create_object_iter(os, creds, path, object_regex, object_types, NULL, recurse_depth, NULL, 0);
  if (it == NULL) return(NULL);

  if (n_keys < 1) return(it);

  //** Tweak things for the fixed key list
  it->n_list = n_keys;
  it->key = key;
  it->val = val;
  it->v_size = v_size;
  type_malloc(it->v_size_user, int, it->n_list);
  memcpy(it->v_size_user, v_size, sizeof(int)*it->n_list);

  it->v_fixed = 1;
  for (i=0; i < n_keys; i++) {
    if (v_size[i] < 0) { it->v_fixed = 0; break; }
  }

  return(it);
}

//***********************************************************************
// osfile_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void osfile_destroy_object_iter(os_object_iter_t *oit)
{
  osf_object_iter_t *it = (osf_object_iter_t *)oit;
  osf_obj_level_t *itl;
  osfile_open_op_t open_op;

  int i;

  //** Close any open directories
  for (i=0; i<it->table->n; i++) {
    if (it->level_info[i].d != NULL) my_closedir(it->level_info[i].d);
  }

  while ((itl = (osf_obj_level_t *)pop(it->recurse_stack)) != NULL) {
     my_closedir(itl->d);
     free(itl);
  }

  if (it->it_attr != NULL) {
     if (*it->it_attr != NULL) osfile_destroy_attr_iter(*(it->it_attr));
  }

  if (it->fd != NULL) {
     open_op.cfd = it->fd;
     open_op.os = it->os;
     osfile_close_object_fn(&open_op, 0);
  }

//  if ((it->v_fixed != 1) && (it->n_list > 0)) {
//     for (i=0; i < it->n_list; i++) {  //** Free any allocated memory before the next call
//        if (it->v_size_user[i] < 0) {
//           if (it->val[i] != NULL) free(it->val[i]);
//        }
//     }
//  }

  if (it->v_size_user != NULL) free(it->v_size_user);

  free_stack(it->recurse_stack, 1);
  free(it->level_info);
  free(it);
}


//***********************************************************************
// osfile_free_open - Frees an open object
//***********************************************************************
void osfile_free_open(void *arg)
{
  osfile_open_op_t *op = (osfile_open_op_t *)arg;

  if (op->path != NULL) free(op->path);
  if (op->id != NULL) free(op->id);

  free(op);
}

//***********************************************************************
// osfile_open_object - Opens an object
//***********************************************************************


op_status_t osfile_open_object_fn(void *arg, int id)
{
  osfile_open_op_t *op = (osfile_open_op_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  osfile_fd_t *fd;
  int ftype, err;
  char fname[OS_PATH_MAX];
  op_status_t status;
  apr_thread_mutex_t *lock;

  log_printf(15, "Attempting to open object=%s\n", op->path);

  *op->fd = NULL;
  snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->path);
  ftype = os_local_filetype(fname);
  if (ftype <= 0) { return(op_failure_status); }

  if (osaz_object_access(osf->osaz, op->creds, op->path, op->mode) == 0)  {
     return(op_failure_status);
  }

  lock = osf_retrieve_lock(op->os, op->path, NULL);
  osf_obj_lock(lock);

  type_malloc(fd, osfile_fd_t, 1);

  fd->os = op->os;
  fd->ftype = ftype;
  fd->mode = op->mode;
  fd->object_name = op->path;
  fd->id = op->id;
  fd->uuid = op->uuid;

  fd->attr_dir = object_attr_dir(op->os, osf->file_path, fd->object_name, ftype);

  osf_obj_unlock(lock);

  err = full_object_lock(fd, op->max_wait);  //** Do a full lock if needed
log_printf(15, "full_object_lock=%d fname=%s\n uuid=" LU "\n", err, fd->object_name, fd->uuid);
  if (err != 0) {  //** Either a timeout or abort occured
     *(op->fd) = NULL;

     free(fd->attr_dir);
     free(fd);
     status = op_failure_status;
  } else {
     *(op->fd) = (os_fd_t *)fd;
     op->path = NULL;  //** This is now used by the fd
     op->id = NULL;
     status = op_success_status;
  }

  return(status);
}

//***********************************************************************
//  osfile_open_object - Makes the open file op
//***********************************************************************

op_generic_t *osfile_open_object(object_service_fn_t *os, creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_open_op_t *op;

  type_malloc(op, osfile_open_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->path = strdup(path);
  op->fd = (osfile_fd_t **)pfd;
  op->max_wait = max_wait;
  op->mode = mode;
  op->id = (id == NULL) ? strdup(osf->host_id) : strdup(id);
  op->uuid = 0;
  get_random(&(op->uuid), sizeof(op->uuid));

  return(new_thread_pool_op(osf->tpc, NULL, osfile_open_object_fn, (void *)op, osfile_free_open, 1));
}

//***********************************************************************
// osfile_abort_open_object_fn - Performs the actual open abort operation
//***********************************************************************

op_status_t osfile_abort_open_object_fn(void *arg, int id)
{
  osfile_open_op_t *op = (osfile_open_op_t *)arg;
  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  op_status_t status;
  fobj_lock_t *fol;
  fobj_lock_task_t *handle;

  if (op->mode == OS_MODE_READ_IMMEDIATE) return(op_success_status);

  apr_thread_mutex_lock(osf->fobj_lock);

  fol = list_search(osf->fobj_table, op->path);

  //** Find the task in the pending list and remove it
  status = op_failure_status;
  move_to_top(fol->stack);
  while ((handle = (fobj_lock_task_t *)get_ele_data(fol->stack)) != NULL) {
     if (handle->fd->uuid == op->uuid) {
        delete_current(fol->stack, 1, 0);
        status = op_success_status;
        handle->abort = 1;
        apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released
        break;
     }
     move_down(fol->stack);
  }

  apr_thread_mutex_unlock(osf->fobj_lock);

  return(status);
}


//***********************************************************************
//  osfile_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

op_generic_t *osfile_abort_open_object(object_service_fn_t *os, op_generic_t *gop)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  thread_pool_op_t *tpop = gop_get_tp(gop);

  return(new_thread_pool_op(osf->tpc, NULL, osfile_abort_open_object_fn, tpop->arg, NULL, 1));
}


//***********************************************************************
// osfile_close_object - Closes an object
//***********************************************************************

op_status_t osfile_close_object_fn(void *arg, int id)
{
  osfile_open_op_t *op = (osfile_open_op_t *)arg;

  if (op->cfd == NULL) return(op_success_status);

  full_object_unlock(op->cfd);

  free(op->cfd->object_name);
  free(op->cfd->attr_dir);
  free(op->cfd->id);
  free(op->cfd);

  return(op_success_status);
}

//***********************************************************************
//  osfile_close_object - Makes the open file op
//***********************************************************************

op_generic_t *osfile_close_object(object_service_fn_t *os, os_fd_t *ofd)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_open_op_t *op;

  type_malloc(op, osfile_open_op_t, 1);

  op->os = os;
  op->cfd = (osfile_fd_t *)ofd;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_close_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_cred_init - Intialize a set of credentials
//***********************************************************************

creds_t *osfile_cred_init(object_service_fn_t *os, int type, void **args)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  creds_t *creds;

  creds = authn_cred_init(osf->authn, type, args);

  //** Right now this is filled with dummy routines until we get an official authn/authz implementation
  an_cred_set_id(creds, args[1]);

  return(creds);
}

//***********************************************************************
// osf_fsck_check_file - Checks the file integrity
//***********************************************************************

int osf_fsck_check_file(object_service_fn_t *os, creds_t *creds, char *fname, int dofix)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  char fullname[OS_PATH_MAX];
  char *faname;
  int ftype;
  FILE *fd;

  //** Check if we can access it.  If not flag success and return
  if (osaz_object_access(osf->osaz, creds, fname, OS_MODE_READ_IMMEDIATE) != 1) return(OS_FSCK_GOOD);

  //** Make sure the proxy entry exists
  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fname);
  ftype = os_local_filetype(fullname);
  if (ftype == 0) {
     if (dofix == OS_FSCK_MANUAL) return(OS_FSCK_MISSING_OBJECT);
     if (dofix == OS_FSCK_REMOVE) {
        //** Remove the FA dir
        osf_object_remove(os, fullname);
        return(OS_FSCK_GOOD);
     }

log_printf(15, "repair  fullname=%s\n", fullname);
     fd = fopen(fullname, "w");
     if (fd == NULL) return(OS_FSCK_MISSING_OBJECT);
     fclose(fd);

     ftype = OS_OBJECT_FILE;
  }

log_printf(15, "fullname=%s\n", fullname);

  //** Make sure the FA directory exists
  faname = object_attr_dir(os, osf->file_path, fname, ftype);
  ftype = os_local_filetype(faname);
log_printf(15, "faname=%s ftype=%d\n", faname, ftype);

  if (ftype != OS_OBJECT_DIR) {
     if (dofix == OS_FSCK_MANUAL) { free(faname); return(OS_FSCK_MISSING_ATTR); }
     if (dofix == OS_FSCK_REMOVE) {
        //** Remove the FA dir
        osf_object_remove(os, fullname);
        free(faname);
        return(OS_FSCK_GOOD);
     }

     ftype = mkdir(faname, DIR_PERMS);
     if (ftype != 0) { free(faname); return(OS_FSCK_MISSING_ATTR); }
  }

  free(faname);
  return(OS_FSCK_GOOD);
}

//***********************************************************************
// osf_fsck_check_dir - Checks the dir integrity
//***********************************************************************

int osf_fsck_check_dir(object_service_fn_t *os, creds_t *creds, char *fname, int dofix)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  char *faname;
  int ftype;

  //** Check if we can access it.  If not flag success and return
  if (osaz_object_access(osf->osaz, creds, fname, OS_MODE_READ_IMMEDIATE) != 1) return(OS_FSCK_GOOD);

  //** Make sure the FA directory exists
  faname = object_attr_dir(os, osf->file_path, fname, OS_OBJECT_DIR);
//  snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, faname);
  ftype = os_local_filetype(faname);
log_printf(15, "fname=%s faname=%s ftype=%d\n", fname, faname, ftype);
  if ((ftype & OS_OBJECT_DIR) == 0) {
     if (dofix == OS_FSCK_MANUAL) { free(faname); return(OS_FSCK_MISSING_ATTR); }
     if (dofix == OS_FSCK_REMOVE) {
        //** Remove the FA dir
        osf_object_remove(os, fname);
        free(faname);
        return(OS_FSCK_GOOD);
     }

     ftype = mkdir(faname, DIR_PERMS);
     if (ftype != 0) { free(faname); return(OS_FSCK_MISSING_ATTR); }
  }

  free(faname);
  return(OS_FSCK_GOOD);
}


//***********************************************************************
// osf_next_fsck - Returns the next object to check
//***********************************************************************

int osf_next_fsck(os_fsck_iter_t *oit, char **fname)
{
  osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;
  osfile_priv_t *osf = (osfile_priv_t *)it->os->priv;
  int prefix_len;
  char fullname[OS_PATH_MAX];
  char *faname;
  struct dirent *entry;

  int atype;

  if (it->ad != NULL) {  //** Checking attribute dir
    while ((entry = readdir(it->ad)) != NULL) {
      if (strncmp(entry->d_name, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) {  //** Got a match
         snprintf(fullname, OS_PATH_MAX, "%s/%s", it->ad_path, &(entry->d_name[FILE_ATTR_PREFIX_LEN]));
         log_printf(15, "ad_path=%s fname=%s d_name=%s\n", it->ad_path, fullname, entry->d_name);
         *fname = strdup(fullname);
         return(OS_OBJECT_FILE);
      }
    }

log_printf(15, "free(ad_path=%s)\n", it->ad_path);
    free(it->ad_path);
    it->ad_path = NULL;
    closedir(it->ad);
    it->ad = NULL;
  }

  //** Use the object iterator
  atype = os_next_object(it->os, it->it, fname, &prefix_len);

  if (atype & OS_OBJECT_DIR) {  //** Got a directory so prep scanning it for next round
     faname = object_attr_dir(it->os, osf->file_path, *fname, OS_OBJECT_DIR);
//     snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, *fname);
     it->ad = opendir(faname);
log_printf(15, "ad_path faname=%s ad=%p\n", faname, it->ad);
     free(faname);
     if (it->ad != NULL) it->ad_path = strdup(*fname);
//log_printf(15, "strdup(ad_path=%s)\n", it->ad_path);
  }

  return(atype);
}

//***********************************************************************
// osfile_fsck_object_check - Resolves the error with the problem object
//***********************************************************************

int osfile_fsck_object_check(object_service_fn_t *os, creds_t *creds, char *fname, int ftype, int resolution)
{
  int err;

  log_printf(15, "mode=%d ftype=%d fname=%s\n", resolution, ftype, fname);
  if (ftype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) {
    err = osf_fsck_check_file(os, creds, fname, resolution);
  } else {
    err = osf_fsck_check_dir(os, creds, fname, resolution);
  }

  return(err);
}

//***********************************************************************
//  osfile_fsck_object_fn - Does the actual object checking
//***********************************************************************

op_status_t osfile_fsck_object_fn(void *arg, int id)
{
  osfile_open_op_t *op = (osfile_open_op_t *)arg;
//  osfile_priv_t *osf = (osfile_priv_t *)op->os->priv;
  op_status_t status;

  status = op_success_status;

  status.error_code = osfile_fsck_object_check(op->os, op->creds, op->path, op->uuid, op->mode);

  return(status);
}

//***********************************************************************
//  osfile_fsck_object - Allocates space for the object check
//***********************************************************************

op_generic_t *osfile_fsck_object(object_service_fn_t *os, creds_t *creds, char *fname, int ftype, int resolution)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_open_op_t *op;

  type_malloc_clear(op, osfile_open_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->path = fname;
  op->mode = resolution;
  op->uuid = ftype;   //** We store the ftype here

  return(new_thread_pool_op(osf->tpc, NULL, osfile_fsck_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_next_fsck - Returns the next problem object
//***********************************************************************

int osfile_next_fsck(object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
  osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;
  char *fname;
  int atype, err;

  while ((atype = osf_next_fsck(oit, &fname)) != 0) {
     if (atype & (OS_OBJECT_FILE|OS_OBJECT_SYMLINK)) {   //** File object
        err = osf_fsck_check_file(it->os, it->creds, fname, OS_FSCK_MANUAL);
     } else {   //** Directory object
        err = osf_fsck_check_dir(it->os, it->creds, fname, OS_FSCK_MANUAL);
     }

     if (err != OS_FSCK_GOOD) {
         *bad_atype = atype;
         *bad_fname = fname;
         return(err);
     }

     free(fname);
  }

  *bad_atype = 0;
  *bad_fname = NULL;
  return(OS_FSCK_FINISHED);
}

//***********************************************************************
// osfile_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *osfile_create_fsck_iter(object_service_fn_t *os, creds_t *creds, char *path, int mode)
{
  osfile_fsck_iter_t *it;

  type_malloc_clear(it, osfile_fsck_iter_t, 1);

  it->os = os;
  it->creds = creds;
  it->path = strdup(path);
  it->mode = mode;

  it->regex = os_path_glob2regex(it->path);
  it->it = os_create_object_iter(os, creds, it->regex, NULL, OS_OBJECT_ANY, NULL, 10000, NULL, 0);
  if (it->it == NULL) {
     log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
     return(NULL);
  }

  return((os_fsck_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void osfile_destroy_fsck_iter(object_service_fn_t *os, os_fsck_iter_t *oit)
{
  osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;

  os_destroy_object_iter(os, it->it);

  if (it->ad != NULL) closedir(it->ad);
  if (it->ad_path != NULL) {log_printf(15, "free(ad_path=%s)\n", it->ad_path); free(it->ad_path); }

  os_regex_table_destroy(it->regex);
  free(it->path);
  free(it);

  return;
}

//***********************************************************************
// osfile_cred_destroy - Destroys a set ot credentials
//***********************************************************************

void osfile_cred_destroy(object_service_fn_t *os, creds_t *creds)
{
//  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
//  char *user;

//  user = cred_get_id(creds);
//  if (user != NULL) free(user);

  an_cred_destroy(creds);
}

//***********************************************************************
// osfile_destroy
//***********************************************************************

void osfile_destroy(object_service_fn_t *os)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  int i;

//  i = atomic_dec(_path_parse_count);
//  if (i <= 0) {
//     apr_thread_mutex_destroy(_path_parse_lock);
//     apr_pool_destroy(_path_parse_pool);
//  }

  for (i=0; i<osf->internal_lock_size; i++) {
     apr_thread_mutex_destroy(osf->internal_lock[i]);
  }
  free(osf->internal_lock);

  apr_thread_mutex_destroy(osf->fobj_lock);
  list_destroy(osf->fobj_table);
  list_destroy(osf->vattr_prefix);
  destroy_pigeon_coop(osf->fobj_pc);
  destroy_pigeon_coop(osf->task_pc);

  osaz_destroy(osf->osaz);
  authn_destroy(osf->authn);

  apr_pool_destroy(osf->mpool);

  free(osf->host_id);
  free(osf->base_path);
  free(osf->file_path);
  free(osf->hardlink_path);
  free(osf);
  free(os);
}

//***********************************************************************
//  object_service_file_create - Creates a file backed OS
//***********************************************************************

object_service_fn_t *object_service_file_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
  object_service_fn_t *os;
  osfile_priv_t *osf;
  osaz_create_t *osaz_create;
  authn_create_t *authn_create;
  char pname[OS_PATH_MAX], pattr[OS_PATH_MAX];
  char *atype, *asection;
  int i, err;

  if (section == NULL) section = "osfile";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osf, osfile_priv_t, 1);
  os->priv = (void *)osf;

  osf->tpc = lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);
  osf->base_path = NULL;
  if (fd == NULL) {
     osf->base_path = strdup("./osfile");
     osaz_create = lookup_service(ess, OSAZ_AVAILABLE, OSAZ_TYPE_FAKE);
     osf->osaz = (*osaz_create)(ess, NULL, NULL, os);
     authn_create = lookup_service(ess, AUTHN_AVAILABLE, AUTHN_TYPE_FAKE);
     osf->authn = (*authn_create)(ess, NULL, NULL);
     osf->internal_lock_size = 200;
     osf->max_copy = 1024*1024;
     osf->hardlink_dir_size = 256;
  } else {
     osf->base_path = inip_get_string(fd, section, "base_path", "./osfile");
     osf->internal_lock_size = inip_get_integer(fd, section, "lock_table_size", 200);
     osf->max_copy = inip_get_integer(fd, section, "max_copy", 1024*1024);
     osf->hardlink_dir_size = inip_get_integer(fd, section, "hardlink_dir_size", 256);
     asection = inip_get_string(fd, section, "authz", NULL);
     atype = (asection == NULL) ? strdup(OSAZ_TYPE_FAKE) : inip_get_string(fd, asection, "type", OSAZ_TYPE_FAKE);
     osaz_create = lookup_service(ess, OSAZ_AVAILABLE, atype);
     osf->osaz = (*osaz_create)(ess, fd, asection, os);
     free(atype);  free(asection);
     if (osf->osaz == NULL) {
        free(osf->base_path);
        free(osf);
        free(os);
        return(NULL);
     }

     asection = inip_get_string(fd, section, "authn", NULL);
     atype = (asection == NULL) ? strdup(AUTHN_TYPE_FAKE) : inip_get_string(fd, asection, "type", AUTHN_TYPE_FAKE);
     authn_create = lookup_service(ess, AUTHN_AVAILABLE, atype);
     osf->authn = (*authn_create)(ess, fd, asection);
     free(atype); free(asection);
     if (osf->osaz == NULL) {
        free(osf->base_path);
        osaz_destroy(osf->osaz);
        free(osf);
        free(os);
        return(NULL);
     }
  }

  snprintf(pname, OS_PATH_MAX, "%s/%s", osf->base_path, "file");
  osf->file_path = strdup(pname);  osf->file_path_len = strlen(osf->file_path);
  snprintf(pname, OS_PATH_MAX, "%s/%s", osf->base_path, "hardlink");
  osf->hardlink_path = strdup(pname); osf->hardlink_path_len = strlen(osf->hardlink_path);

  apr_pool_create(&osf->mpool, NULL);
  type_malloc_clear(osf->internal_lock, apr_thread_mutex_t *, osf->internal_lock_size);
  for (i=0; i<osf->internal_lock_size; i++) {
    apr_thread_mutex_create(&(osf->internal_lock[i]), APR_THREAD_MUTEX_DEFAULT, osf->mpool);
  }

  apr_thread_mutex_create(&(osf->fobj_lock), APR_THREAD_MUTEX_DEFAULT, osf->mpool);
  osf->fobj_table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  osf->fobj_pc = new_pigeon_coop("fobj_pc", 50, sizeof(fobj_lock_t), osf->mpool, fobj_lock_new, fobj_lock_free);
  osf->task_pc = new_pigeon_coop("fobj_task_pc", 50, sizeof(fobj_lock_task_t), osf->mpool, fobj_lock_task_new, fobj_lock_task_free);

  osf->base_path_len = strlen(osf->base_path);

  //** Get the default host ID for opens
  char hostname[1024];
  apr_gethostname(hostname, sizeof(hostname), osf->mpool);
  osf->host_id = strdup(hostname);

  //** Make and install the virtual attributes
  osf->vattr_hash = apr_hash_make(osf->mpool);
  osf->vattr_prefix = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);

  osf->lock_va.attribute = "os.lock";
  osf->lock_va.priv = os;
  osf->lock_va.get = va_lock_get_attr;
  osf->lock_va.set = va_null_set_attr;
  osf->lock_va.get_link = va_null_get_link_attr;

  osf->link_va.attribute = "os.link";
  osf->link_va.priv = os;
  osf->link_va.get = va_link_get_attr;
  osf->link_va.set = va_null_set_attr;
  osf->link_va.get_link = va_null_get_link_attr;

  osf->link_count_va.attribute = "os.link_count";
  osf->link_count_va.priv = os;
  osf->link_count_va.get = va_link_count_get_attr;
  osf->link_count_va.set = va_null_set_attr;
  osf->link_count_va.get_link = va_null_get_link_attr;

  osf->type_va.attribute = "os.type";
  osf->type_va.priv = os;
  osf->type_va.get = va_type_get_attr;
  osf->type_va.set = va_null_set_attr;
  osf->type_va.get_link = va_null_get_link_attr;

  osf->create_va.attribute = "os.create";
  osf->create_va.priv = os;
  osf->create_va.get = va_create_get_attr;
  osf->create_va.set = va_null_set_attr;
  osf->create_va.get_link = va_null_get_link_attr;

  apr_hash_set(osf->vattr_hash, osf->lock_va.attribute, APR_HASH_KEY_STRING, &(osf->lock_va));
  apr_hash_set(osf->vattr_hash, osf->link_va.attribute, APR_HASH_KEY_STRING, &(osf->link_va));
  apr_hash_set(osf->vattr_hash, osf->link_count_va.attribute, APR_HASH_KEY_STRING, &(osf->link_count_va));
  apr_hash_set(osf->vattr_hash, osf->type_va.attribute, APR_HASH_KEY_STRING, &(osf->type_va));
  apr_hash_set(osf->vattr_hash, osf->create_va.attribute, APR_HASH_KEY_STRING, &(osf->create_va));

  osf->attr_link_pva.attribute = "os.attr_link";
  osf->attr_link_pva.priv = (void *)(long)strlen(osf->attr_link_pva.attribute);
  osf->attr_link_pva.get = va_attr_link_get_attr;
  osf->attr_link_pva.set = va_null_set_attr;
  osf->attr_link_pva.get_link = va_attr_link_get_attr;

  osf->attr_type_pva.attribute = "os.attr_type";
  osf->attr_type_pva.priv = (void *)(long)(strlen(osf->attr_type_pva.attribute));
  osf->attr_type_pva.get = va_attr_type_get_attr;
  osf->attr_type_pva.set = va_null_set_attr;
  osf->attr_type_pva.get_link = va_null_get_link_attr;

  osf->timestamp_pva.attribute = "os.timestamp";
  osf->timestamp_pva.priv = (void *)(long)(strlen(osf->timestamp_pva.attribute));
  osf->timestamp_pva.get = va_timestamp_get_attr;
  osf->timestamp_pva.set = va_timestamp_set_attr;
  osf->timestamp_pva.get_link = va_timestamp_get_link_attr;

  list_insert(osf->vattr_prefix, osf->attr_link_pva.attribute, &(osf->attr_link_pva));
  list_insert(osf->vattr_prefix, osf->attr_type_pva.attribute, &(osf->attr_type_pva));
  list_insert(osf->vattr_prefix, osf->timestamp_pva.attribute, &(osf->timestamp_pva));

  os->type = OS_TYPE_FILE;

  os->destroy_service = osfile_destroy;
  os->cred_init = osfile_cred_init;
  os->cred_destroy = osfile_cred_destroy;
  os->exists = osfile_exists;
  os->create_object = osfile_create_object;
  os->remove_object = osfile_remove_object;
  os->remove_regex_object = osfile_remove_regex_object;
  os->move_object = osfile_move_object;
  os->symlink_object = osfile_symlink_object;
  os->hardlink_object = osfile_hardlink_object;
  os->create_object_iter = osfile_create_object_iter;
  os->create_object_iter_alist = osfile_create_object_iter_alist;
  os->next_object = osfile_next_object;
  os->destroy_object_iter = osfile_destroy_object_iter;
  os->open_object = osfile_open_object;
  os->close_object = osfile_close_object;
  os->abort_open_object = osfile_abort_open_object;
  os->get_attr = osfile_get_attr;
  os->set_attr = osfile_set_attr;
  os->symlink_attr = osfile_symlink_attr;
  os->copy_attr = osfile_copy_attr;
  os->get_multiple_attrs = osfile_get_multiple_attrs;
  os->set_multiple_attrs = osfile_set_multiple_attrs;
  os->copy_multiple_attrs = osfile_copy_multiple_attrs;
  os->symlink_multiple_attrs = osfile_symlink_multiple_attrs;
  os->move_attr = osfile_move_attr;
  os->move_multiple_attrs = osfile_move_multiple_attrs;
  os->regex_object_set_multiple_attrs = osfile_regex_object_set_multiple_attrs;
  os->create_attr_iter = osfile_create_attr_iter;
  os->next_attr = osfile_next_attr;
  os->destroy_attr_iter = osfile_destroy_attr_iter;

  os->create_fsck_iter = osfile_create_fsck_iter;
  os->destroy_fsck_iter = osfile_destroy_fsck_iter;
  os->next_fsck = osfile_next_fsck;
  os->fsck_object = osfile_fsck_object;

  //** Check if everything is copacetic with the root dir
  if (os_local_filetype(osf->base_path) <= 0) {
    log_printf(0, "Base Path doesn't exist!  base_path=%s\n", osf->base_path);
    os_destroy(os);
    os = NULL;
    return(NULL);
  }

  if (os_local_filetype(osf->file_path) <= 0) {
    log_printf(0, "File Path doesn't exist!  file_path=%s\n", osf->file_path);
    os_destroy(os);
    os = NULL;
    return(NULL);
  }

  if (os_local_filetype(osf->hardlink_path) <= 0) {
    log_printf(0, "Hard link Path doesn't exist!  hardlink_path=%s\n", osf->hardlink_path);
    os_destroy(os);
    os = NULL;
    return(NULL);
  }

  snprintf(pname, OS_PATH_MAX, "%s/%s", osf->file_path, FILE_ATTR_PREFIX);
  if (os_local_filetype(pname) <= 0) {  //** Missing attr directory for base so create it
     i = mkdir(pname, DIR_PERMS);
     if (i != 0) {
        log_printf(0, "Base path attributes directory cannot be created! base_path_attr=%s\n", pname);
        os_destroy(os);
        os = NULL;
        return(NULL);
     }


  }

  //** Make sure al lthe hardlink dirs exist
  for (i=0; i<osf->hardlink_dir_size; i++) {
     snprintf(pname, OS_PATH_MAX, "%s/%d", osf->hardlink_path, i);
     if (os_local_filetype(pname) == 0) {
        err = mkdir(pname, DIR_PERMS);
        if (err != 0) {
           log_printf(0, "Error creating hardlink directory! full=%s\n", pname);
           os_destroy(os);
           os = NULL;
           return(NULL);
        }

        //** Also need to make the attributes directory
        snprintf(pattr, OS_PATH_MAX, "%s/%s", pname, FILE_ATTR_PREFIX);
        err = mkdir(pattr, DIR_PERMS);
        if (err != 0) {
           log_printf(0, "Error creating object attr directory! full=%s\n", pattr);
           os_destroy(os);
           os = NULL;
           return(NULL);
        }
     }
  }

  return(os);
}


//***********************************************************************
//  local_next_object - returns the next local object
//***********************************************************************

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len)
{
  return(osf_next_object(it->oit, myfname, prefix_len));
}


//***********************************************************************
//  Dummy OSAZ routine for the local iter
//***********************************************************************

int local_osaz_access(os_authz_t *osa, creds_t *c, char *path, int mode)
{
  return(1);
}

//***********************************************************************
// create_local_object_iter - Creates a local object iterator
//***********************************************************************

local_object_iter_t *create_local_object_iter(os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth)
{
  local_object_iter_t *it;
  osfile_priv_t *osf;

  type_malloc_clear(it, local_object_iter_t, 1);

  //** Make a bare bones os_file object
  type_malloc_clear(it->os, object_service_fn_t, 1);
  type_malloc_clear(osf, osfile_priv_t, 1);
  type_malloc_clear(osf->osaz, os_authz_t, 1);
  it->os->priv = (void *)osf;
  osf->file_path = "";
  osf->osaz->object_access = local_osaz_access;

  it->oit = osfile_create_object_iter(it->os, NULL, path, object_regex, object_types, NULL, recurse_depth, NULL, 0);

  return(it);
}

//***********************************************************************
// destroy_local_object_iter -Destroys the loca file iter
//***********************************************************************

void destroy_local_object_iter(local_object_iter_t *it)
{
  osfile_priv_t *osf = (osfile_priv_t *)it->os->priv;

  osfile_destroy_object_iter(it->oit);

  free(osf->osaz);
  free(osf);
  free(it->os);
  free(it);
}
