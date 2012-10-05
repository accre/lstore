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

#define _log_module_index 212


#include <assert.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <sys/stat.h>
#include <unistd.h>
#include <math.h>
#include "lio_fuse.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "append_printf.h"
#include "string_token.h"


#define lfs_lock(lfs)  apr_thread_mutex_lock((lfs)->lock)
#define lfs_unlock(lfs)  apr_thread_mutex_unlock((lfs)->lock)
#define dentry_name(entry) &((entry)->fname[(entry)->name_start])

lio_fuse_t *lfs_gc = NULL;
struct fuse_lowlevel_ops lfs_gc_llops;

int ino_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b);
static skiplist_compare_t ino_compare = {.fn=ino_compare_fn, .arg=NULL };

#define _inode_key_size 6
static char *_inode_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count" };

#define _tape_key_size  2
static char *_tape_keys[] = { "system.owner", "system.exnode" };

typedef struct {
  lio_fuse_t *lfs;
  os_object_iter_t *it;
  os_regex_table_t *path_regex;
  char *val[_inode_key_size];
  int v_size[_inode_key_size];
  fuse_ino_t root;
  Stack_t *stack;
  lio_dentry_t dot_entry;
  lio_dentry_t dotdot_entry;
  int state;
} lfs_dir_iter_t;

typedef struct {
  char *buf;
  lio_fuse_t *lfs;
  tbuffer_t tbuf;
  ex_iovec_t exv;
  callback_t cb;
  int rw_mode;
  fuse_req_t req;
  ex_off_t size;
  ex_off_t off;
  ex_id_t ino;
} lfs_rw_cb_t;

//*************************************************************************
//  ino_compare_fn  - FUSE inode comparison function
//*************************************************************************

int ino_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b)
{
  fuse_ino_t *al = (fuse_ino_t *)a;
  fuse_ino_t *bl = (fuse_ino_t *)b;

//log_printf(15, "a=%lu b=%lu\n", *al, *bl);
  if (*al<*bl) {
     return(-1);
  } else if (*al == *bl) {
     return(0);
  }

  return(1);
}


//*************************************************************************
// _lfs_inode_lookup - Looks up an inode.
//*************************************************************************

lio_inode_t * _lfs_inode_lookup(lio_fuse_t *lfs, fuse_ino_t ino)
{
  lio_inode_t *inode;

  inode = list_search(lfs->ino_index, (list_key_t *)&ino);

  log_printf(15, "looking up ino=%lu inode=%p\n", ino, inode);
  return(inode);
}


//*************************************************************************
// _lfs_dentry_lookup - Looks up a dentry.
//*************************************************************************

lio_dentry_t * _lfs_dentry_lookup(lio_fuse_t *lfs, char *fname)
{
  lio_dentry_t *entry;

  if (strcmp(fname, "/") == 0) {
     entry = list_search(lfs->fname_index, (list_key_t *)"");
  } else {
     entry = list_search(lfs->fname_index, (list_key_t *)fname);
  }

  log_printf(15, "looking up fname=%s entry=%p\n", fname, entry);
  return(entry);
}

//*************************************************************************
// _lfs_dentry_insert - Adds a dentry to the file table
//*************************************************************************

int _lfs_dentry_insert(lio_fuse_t *lfs, lio_dentry_t *entry)
{
  int i;
  char *fname;
  lio_inode_t *inode;

  //** Find where the local file name starts
  fname = entry->fname;
  i = 0;
  while (fname[i] != 0) {
    if (fname[i] == '/') entry->name_start = i+1;
    i++;
  }

log_printf(15, "fname=%s name_start=%d name=%s\n", entry->fname, entry->name_start, dentry_name(entry));
  list_insert(lfs->fname_index, (list_key_t *)entry->fname, (list_data_t *)entry);

  //** got to add it to the inode dentry list
  inode = entry->inode;
log_printf(15, "inode=" XIDT " entry1=%p\n", inode->ino, inode->entry1);
  if (inode->entry1 == NULL) {
log_printf(15, "added to entry1\n");
     inode->entry1 = entry;  //** Simple add
  } else {
log_printf(15, "added to dentry_stack\n");
     if (inode->dentry_stack == NULL) inode->dentry_stack = new_stack();
     push(inode->dentry_stack, entry);
  }
  return(0);
}

//*************************************************************************
// _lfs_dentry_remove - Removes the fname's dentry
//*************************************************************************

int _lfs_dentry_remove(lio_fuse_t *lfs, lio_dentry_t *entry)
{
  lio_dentry_t *e2;
  lio_inode_t *inode;
  int move;

  inode = entry->inode;
  e2 = inode->entry1;
  move = 1;
log_printf(1, "fname=%s ino=" XIDT " inode->entry1->fname=%s\n", entry->fname, inode->ino, inode->entry1->fname);

  if (e2 != entry) {  //** Got to scan the list fora match
     move = 0;
     if (inode->dentry_stack != NULL) {
        move_to_top(inode->dentry_stack);
        while ((e2 = get_ele_data(inode->dentry_stack)) != NULL) {
           if (e2 == entry) break;
           move_down(inode->dentry_stack);
        }
     }
  }

  if (e2 == NULL) {
     log_printf(0, "Missing dentry! fname=%s ino=" XIDT " inode->entry1->fname=%s\n", entry->fname, inode->ino, inode->entry1->fname);
     return(1);
  }

  list_remove(lfs->fname_index, (list_key_t *)e2->fname, (list_data_t *)e2);

  if (move == 1) {
log_printf(15, "removed from entry1\n");
     inode->entry1 = NULL;
     if (inode->dentry_stack != NULL) inode->entry1 = pop(inode->dentry_stack);
  } else {
log_printf(15, "removed from dentry_stack\n");
     delete_current(inode->dentry_stack, 1, 0);
  }

  free(e2->fname);
  free(e2);

  return(0);
}


//*************************************************************************
// _lfs_inode_insert - Inserts the given file into the inode table and returns
//    the assigned inode.
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_insert(lio_fuse_t *lfs, lio_inode_t *inode, lio_dentry_t *entry)
{

  log_printf(15, "inserting ino=" XIDT "\n", inode->ino);
  list_insert(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);
  if (entry != NULL) _lfs_dentry_insert(lfs, entry);

  return;
}

//*************************************************************************
// _lfs_inode_remove- Removes the given inode so it can't be looked up
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_remove(lio_fuse_t *lfs, lio_inode_t *inode)
{
  lio_dentry_t *entry;

  log_printf(15, "ino=" XIDT " fname=%s\n", inode->ino, inode->entry1->fname);

  list_remove(lfs->fname_index, (list_key_t *)inode->entry1->fname, (list_data_t *)inode->entry1);
  if (inode->dentry_stack != NULL) {
     move_to_top(inode->dentry_stack);
     while ((entry = get_ele_data(inode->dentry_stack)) != NULL) {
        list_remove(lfs->fname_index, (list_key_t *)entry->fname, (list_data_t *)entry);
        move_down(inode->dentry_stack);
     }
//     free_stack(inode->dentry_stack, 0);
  }

  list_remove(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);

  return;
}

//*************************************************************************
// lfs_inode_destroy-  Destroys the given inode.  It doesn't remove it from the list
//*************************************************************************

void lfs_inode_destroy(lio_fuse_t *lfs, lio_inode_t *inode)
{
  lio_dentry_t *entry;
  if (inode->entry1 != NULL) {
     log_printf(15, "ino=" XIDT " fname=%s fuse=%d lfs=%d\n", inode->ino, inode->entry1->fname, inode->fuse_count, inode->lfs_count);
  } else {
     log_printf(15, "ino=" XIDT " fuse=%d lfs=%d\n", inode->ino, inode->fuse_count, inode->lfs_count);
  }

  entry = inode->entry1;
  if (entry != NULL) {
    free(entry->fname);
    free(entry);
  }

  if (inode->dentry_stack != NULL) {
     while ((entry = pop(inode->dentry_stack)) != NULL) {
        free(entry->fname);
        free(entry);
     }
     free_stack(inode->dentry_stack, 0);
  }

  free(inode);
  return;
}

//*************************************************************************
// lfs_inode_access - Mark it as being accessed so it wont be deleted
//*************************************************************************

void _lfs_inode_access(lio_fuse_t *lfs, lio_inode_t *inode, int n_fuse, int n_lfs)
{

  inode->fuse_count += n_fuse;
  inode->lfs_count += n_lfs;

log_printf(1, "inode=" XIDT " fuse=%d (%d) lfs=%d (%d) flagged=%d\n", inode->ino, inode->fuse_count, n_fuse, inode->lfs_count, n_lfs, inode->flagged_object);

//log_printf(15, "fname=%s fuse=%d lfs=%d flagged=%d ino=" XIDT " ptr=%p\n", inode->entry1->fname, inode->fuse_count, inode->lfs_count, inode->flagged_object, inode->ino, inode);
  return;
}

//*************************************************************************
// lfs_inode_release - Release the object which may result in it's removal
//*************************************************************************

void _lfs_inode_release(lio_fuse_t *lfs, lio_inode_t *inode, int n_fuse, int n_lfs)
{
  int err, drop, ndentry;
  lio_dentry_t *entry;

  inode->fuse_count -= n_fuse;
  inode->lfs_count -= n_lfs;

log_printf(1, "inode=" XIDT " fuse=%d (%d) lfs=%d (%d) flagged=%d\n", inode->ino, inode->fuse_count, n_fuse, inode->lfs_count, n_lfs, inode->flagged_object);
log_printf(15, "fname=%s fuse=%d lfs=%d flagged=%d ino=" XIDT " ptr=%p\n", inode->entry1->fname, inode->fuse_count, inode->lfs_count, inode->flagged_object, inode->ino, inode);
if ((inode->lfs_count < 0) || (inode->fuse_count < 0)) {
log_printf(15, "NEGATVIE fname=%s fuse=%d lfs=%d flagged=%d ino=" XIDT "\n", inode->entry1->fname, inode->fuse_count, inode->lfs_count, inode->flagged_object, inode->ino);
}

//  if ((inode->lfs_count > 0) || (inode->fuse_count > 0)) return;
  if (inode->lfs_count > 0) return; //** The LFS counts signify open files, etc
  if (inode->fuse_count > 0) {  //** Have FUSE counts so only proceed if not marked for deletion
     if (inode->flagged_object != LFS_INODE_DELETE) return;
  }

  if (inode->flagged_object == LFS_INODE_DELETE) { //** Go ahead and remove it
    drop = 0;
//    if ((stack_size(inode->remove_stack) >= inode->nlinks) || ((inode->ftype & OS_OBJECT_DIR) > 0)) {
    ndentry = (inode->dentry_stack == NULL) ? 1 : stack_size(inode->dentry_stack);
    if ((stack_size(inode->remove_stack) >= ndentry) || ((inode->ftype & OS_OBJECT_DIR) > 0)) {
       _lfs_inode_remove(lfs, inode);  //** Remove it from the table so it's not accidentally accessed again
       drop = 1;
    }

log_printf(1, "Removing fname=%s remove_stack=%d ndentry=%d drop=%d ftype=%d\n", inode->entry1->fname, stack_size(inode->remove_stack), ndentry, drop, inode->ftype);
    //** Now go ahead and remove it
    while ((entry = pop(inode->remove_stack)) != NULL) {
       lfs_unlock(lfs);  //** Release the global lock while we delete things

       err = gop_sync_exec(lio_remove_object(lfs->lc, lfs->lc->creds, entry->fname, NULL, inode->ftype));
log_printf(1, "remove err=%d entry->fname=%s\n", err, entry->fname);
       if (entry->req != NULL) {
          if (err == OP_STATE_SUCCESS) {
             fuse_reply_err(entry->req, 0);
          } else if ((inode->ftype & OS_OBJECT_DIR) > 0) { //** Most likey the dirs not empty
             fuse_reply_err(entry->req, ENOTEMPTY);
          } else {
             fuse_reply_err(entry->req, EBUSY);  //** Otherwise throw a generic error
          }
       }

       lfs_lock(lfs);  //** Get it back
       inode->nlinks--;

       _lfs_dentry_remove(lfs, entry);
    }

    free_stack(inode->remove_stack, 0);  inode->remove_stack = NULL;
    inode->flagged_object = LFS_INODE_OK;
    if (drop == 1) lfs_inode_destroy(lfs, inode);
  } else {
    _lfs_inode_remove(lfs, inode);  //** Just remove it from the table
    lfs_inode_destroy(lfs, inode);
  }

  return;
}


//*************************************************************************
// ftype_lio2fuse - Converts a LIO filetype to fuse
//*************************************************************************
mode_t ftype_lio2fuse(int ftype)
{
  mode_t mode;

  mode = 0;
  if (ftype & OS_OBJECT_SYMLINK) {
     mode = S_IFLNK | 0444;
  } else if (ftype & OS_OBJECT_DIR) {
     mode = S_IFDIR | 0755;
  } else {
     mode = S_IFREG | 0444;
  }

  return(mode);
}

//*************************************************************************
// _lfs_parse_inode_vals - Parses the inode values received
//*************************************************************************

void _lfs_parse_inode_vals(lio_fuse_t *lfs, lio_inode_t *inode, char **val, int *v_size)
{
  int i;

  if (val[0] != NULL) {
     inode->ino = 0; sscanf(val[0], XIDT, &(inode->ino));
  } else {
     if (inode->ino != 0) {
        generate_ex_id(&(inode->ino));
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT "\n", inode->ino);
     } else {
        log_printf(0, "Missing inode using the old value! ino=" XIDT "\n", inode->ino);
     }
  }

  if (val[1] != NULL) {
     lio_get_timestamp(val[1], &(inode->modify_data_ts), NULL);
  } else {
    inode->modify_data_ts = 0;
  }

  if (val[2] != NULL) {
    lio_get_timestamp(val[2], &(inode->modify_attr_ts), NULL);
  } else {
    inode->modify_attr_ts = 0;
  }

log_printf(15, "data_ts=%s att_ts=%s ino=" XIDT "\n", val[1], val[2], inode->ino);

  if (inode->fh == NULL) {
     inode->size = 0;
     if (val[3] != NULL) sscanf(val[3], XIDT, &(inode->size));
  } else {
     inode->size = segment_size(inode->fh->seg);
  }

  inode->ftype = 0;
  if (val[4] != NULL) sscanf(val[4], "%d", &(inode->ftype));

  inode->nlinks = 0;
  if (val[5] != NULL) sscanf(val[5], "%d", &(inode->nlinks));

  for (i=0; i<_inode_key_size; i++) {
     if (val[i] != NULL) free(val[i]);
  }

  //** Update the recheck time
  inode->recheck_time = apr_time_now() + lfs->entry_to * APR_USEC_PER_SEC;
}


//*************************************************************************
// _lfs_pending_inode_wait - Waits for the inode to complete loading
//*************************************************************************

void _lfs_pending_inode_wait(lio_fuse_t *lfs, lio_inode_t *inode)
{
  int slot;

  inode->pending_count++;
  slot = inode->ino % lfs->cond_count;
  if (inode->pending_update > 0) {  //** Spin until updated
     //** Wait for the update to complete by waiting for the lock to be released
     apr_thread_cond_wait(lfs->inode_cond[slot], lfs->lock);
     while (inode->pending_update > 0) {
         apr_thread_cond_wait(lfs->inode_cond[slot], lfs->lock);
     }
  }
  inode->pending_count--;
}


//*************************************************************************
//  _lfs_load_inode_entry - Loads an Inode and dentry from the OS
//    NOTE: The curr_inode can be null and a new struc is created and
//          the new dentry is returned.
//*************************************************************************

lio_dentry_t *_lfs_load_inode_entry(lio_fuse_t *lfs, char *fname, lio_inode_t *curr_inode)
{
  int v_size[_inode_key_size];
  char *val[_inode_key_size];
  char *myfname;
  int i, err, slot, updating;
  ex_id_t start_ino;
  lio_inode_t *inode, *tinode;
  lio_dentry_t *entry;

  //** Check if another thread is doing the load if so wait
  err = OP_STATE_SUCCESS;
  inode = NULL;
  updating = 0;
  if (curr_inode != NULL) {
     slot = curr_inode->ino % lfs->cond_count;
     if (curr_inode->pending_update > 0) {  //** Spin until updated
        _lfs_pending_inode_wait(lfs, curr_inode);
        inode = curr_inode;
        goto dentry_only;
     }

     curr_inode->pending_update = 1;  //** Let everyone know an update is being performed
     inode = curr_inode;
     updating = 1;
  } else {  //** New inode so put it
     inode = list_search(lfs->new_inode_list, fname); //** Look it up on the new pending inode list
     if (inode == NULL) {  //** I'm the 1st so add it to the list
        type_malloc_clear(inode, lio_inode_t, 1);
        slot = atomic_inc(lfs->counter) % lfs->cond_count;
        inode->pending_update = 1;
        list_insert(lfs->new_inode_list, fname, (list_data_t *)inode);
        updating = 2;
     } else {  //** Wait on them to complete
       _lfs_pending_inode_wait(lfs, inode);
       free(fname);
//       if (inode->flagged_object == LFS_INODE_DELETE) {
//          if (inode->pending_count == 0) lfs_inode_destroy(lfs, inode);
//          inode = NULL;
//       }
       goto dentry_only;
     }
  }

  lfs_unlock(lfs);  //** Release the global lock

  for (i=0; i<_inode_key_size; i++) {
     v_size[i] = -lfs->lc->max_attr;
     val[i] = NULL;
  }

  //** Get the attributes
  myfname = (strcmp(fname, "") == 0) ? "/" : fname;
  err = lioc_get_multiple_attrs(lfs->lc, lfs->lc->creds, myfname, NULL, _inode_keys, (void **)val, v_size, _inode_key_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     inode->flagged_object = LFS_INODE_DROP;
     goto finished;
  }

  start_ino = inode->ino;

  log_printf(15, "Parsing info for fname=%s\n", myfname);
  _lfs_parse_inode_vals(lfs, inode, val, v_size);

  if (start_ino == FUSE_ROOT_ID) {
     inode->ino = start_ino;
  }

finished:
  lfs_lock(lfs);

  if (updating == 1) { //** Let everyone know the update is complete
    inode->pending_update = 0;
    apr_thread_cond_broadcast(lfs->inode_cond[slot]);
  } else if (updating == 2) {
    inode->pending_update = 0;
    list_remove(lfs->new_inode_list, fname, inode);  //** Remove it form the list
    apr_thread_cond_broadcast(lfs->inode_cond[slot]);
  }

dentry_only:
  entry = NULL;
  if (err == OP_STATE_SUCCESS) {
    entry = _lfs_dentry_lookup(lfs, fname);
    tinode = _lfs_inode_lookup(lfs, inode->ino);
    if (entry == NULL) {
       type_malloc_clear(entry, lio_dentry_t, 1);
       entry->fname = fname;
       entry->inode = (tinode == NULL) ? inode : tinode;
       _lfs_dentry_insert(lfs, entry);
    }

    if (curr_inode == NULL) {
       if (tinode != NULL) {
          lfs_inode_destroy(lfs, inode);  //** HArd link and already in the cache
       } else {
          _lfs_inode_insert(lfs, inode, NULL);
       }
    }
  } else {
     if ((inode->pending_count == 0) && (curr_inode == NULL)) { lfs_inode_destroy(lfs, inode); free(fname); }
  }

  return(entry);
}

//*************************************************************************
// lfs_fill_stat - Fills a stat structure with the inode info
//*************************************************************************

void lfs_fill_stat(struct stat *stat, lio_inode_t *inode)
{

  memset(stat, 0, sizeof(struct stat));
  stat->st_ino = inode->ino;
  if (inode->fh != NULL) inode->size = segment_size(inode->fh->seg);
  stat->st_size = inode->size;
  stat->st_mode = ftype_lio2fuse(inode->ftype);
  stat->st_nlink = inode->nlinks;
  stat->st_mtime = inode->modify_data_ts;
  stat->st_ctime = inode->modify_attr_ts;
}


//*************************************************************************
// lfs_stat - Does a stat on the file/dir
//*************************************************************************

void lfs_stat(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lio_inode_t *inode;
  struct stat stat;

  log_printf(15, "ino=%lu\n", ino); flush_log();

  lfs_lock(lfs_gc);

  inode = _lfs_inode_lookup(lfs_gc, ino);
  if (inode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  if (apr_time_now() > inode->recheck_time) {  //** Got to reload the info
     _lfs_load_inode_entry(lfs_gc, inode->entry1->fname, inode);
  }

  if (inode->remove_stack != NULL) {
     if (stack_size(inode->remove_stack) >= inode->nlinks) {
        lfs_unlock(lfs_gc);
        fuse_reply_err(req, ENOENT);
     }
  }

  lfs_fill_stat(&stat, inode);
  lfs_unlock(lfs_gc);
  fuse_reply_attr(req, &stat, lfs_gc->attr_to);
}

//*************************************************************************
//  lfs_forget - Dec the inode ref count
//*************************************************************************

void lfs_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
  lio_inode_t *inode;

  log_printf(1, "ino=%lu forget=%lu\n", ino, nlookup); flush_log();

  lfs_lock(lfs_gc);

  inode = _lfs_inode_lookup(lfs_gc, ino);
  if (inode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_none(req);
     return;
  }

  _lfs_inode_release(lfs_gc, inode, nlookup, 0);
  lfs_unlock(lfs_gc);

  fuse_reply_none(req);
  return;
}


//*************************************************************************
// lfs_lookup - Looks up a file
//*************************************************************************

void lfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param fentry;
  lio_inode_t *inode, *pinode;
  lio_dentry_t *entry;
  char fullname[OS_PATH_MAX];
  char *tmp;
//  int to_remove;

  log_printf(1, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs_gc);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs_gc, parent);
  if (pinode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->entry1->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->entry1->fname, name, fullname);

  entry = _lfs_dentry_lookup(lfs_gc, fullname);
  if (entry == NULL) {  //** Don't have it so need to load it
     tmp = strdup(fullname);
     entry = _lfs_load_inode_entry(lfs_gc, tmp, NULL);
     if (entry == NULL) { //** File doesn't exist!
       log_printf(15, "File doesn't exist! fname=%s\n", fullname);
       lfs_unlock(lfs_gc);
       fuse_reply_err(req, ENOENT);
       return;
     }
  } else if (apr_time_now() > entry->inode->recheck_time) { //** Need to recheck stuff
     _lfs_load_inode_entry(lfs_gc, entry->fname, entry->inode);
  }

  inode = entry->inode;

//  to_remove = 0;
//  if (inode->remove_stack != NULL) {
//     if (stack_size(inode->remove_stack) >= inode->nlinks) to_remove = 1;
//  }

  if (entry->flagged == LFS_INODE_OK) {
     //** Update the ref count
     _lfs_inode_access(lfs_gc, inode, 1, 0);

     //** Form the response
     fentry.ino = inode->ino;
     fentry.generation = 0;
     fentry.attr_timeout = lfs_gc->attr_to;
     fentry.entry_timeout = lfs_gc->entry_to;

     lfs_fill_stat(&(fentry.attr), inode);

log_printf(1, "fullname=%s ino=" XIDT " ftype=%d\n", inode->entry1->fname, inode->ino, inode->ftype);

     lfs_unlock(lfs_gc);

     fuse_reply_entry(req, &fentry);
  } else {
     log_printf(15, "File doesn't exist! fname=%s\n", fullname);
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
  }
}


//*************************************************************************
// lfs_readdir - Returns the next file in the directory
//*************************************************************************

void  lfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
  int ftype, prefix_len, n, i, bpos, skip;
  char *fname, *buf;
  lio_inode_t *inode, *tinode;
  lio_dentry_t *entry;
  struct stat stbuf;

  int off2 = off;
  ex_off_t size2= size;
  log_printf(1, "ino=%lu off=%d size=" XOT " stack_size=%d\n", ino, off2, size2, stack_size(dit->stack)); flush_log();

  if (dit == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  memset(&stbuf, 0, sizeof(stbuf));

  n = stack_size(dit->stack);
  if (n>off) { //** Rewind
    move_to_top(dit->stack);
    for (i=0; i<off; i++) move_down(dit->stack);
    entry = get_ele_data(dit->stack);
    while (entry->flagged != LFS_INODE_OK) {  //** SKip over deleted entries
       move_down(dit->stack);
       entry = get_ele_data(dit->stack);
       if (entry == NULL) break;
    }

    if (entry != NULL) {
       lfs_fill_stat(&stbuf, entry->inode);
       n = fuse_add_direntry(req, NULL, 0, dentry_name(entry), NULL, 0);
       type_malloc(buf, char, n);
       fuse_add_direntry(req, buf, n, dentry_name(entry), &stbuf, off+1);
       fuse_reply_buf(req, buf, n);
       free(buf);
      return;
    }
  }

  //** Make sure we're at a valid point
  if (off > n+1) {
     fuse_reply_err(req, EBADSLT);
     return;
  }

  type_malloc_clear(buf, char, size);
  bpos=0;
  for (;;) {
     //** If we made it here then grab the next file and look it up.
     ftype = os_next_object(dit->lfs->lc->os, dit->it, &fname, &prefix_len);
     if (ftype <= 0) { //** No more files
        if (bpos == 0) {
           fuse_reply_buf(req, NULL, 0);
        } else {
           fuse_reply_buf(req, buf, bpos+1);
        }
        free(buf);
        return;
     }

     //** Check if the file already exists
     lfs_lock(dit->lfs);
     entry = _lfs_dentry_lookup(dit->lfs, fname);

     if (entry == NULL) { //** Doesn't exist so add it
       type_malloc_clear(entry, lio_dentry_t, 1);
       type_malloc_clear(inode, lio_inode_t, 1);
       entry->fname = fname;
       entry->inode = inode;
       _lfs_parse_inode_vals(dit->lfs, inode, dit->val, dit->v_size);

       //** If it's a hardlink we may already have the inode
       tinode = _lfs_inode_lookup(dit->lfs, inode->ino);
       if (tinode == NULL) {
          _lfs_inode_insert(dit->lfs, inode, entry);
       } else {  //** Inode already loaded so just add the dentry
         entry->inode= tinode;
         _lfs_dentry_insert(dit->lfs, entry);
         lfs_inode_destroy(dit->lfs, inode);  //** Destroy the redundant inode
       }
     } else {
       for (i=0; i<_inode_key_size; i++) if (dit->val[i] != NULL) free(dit->val[i]);
       free(fname);
     }

log_printf(1, "next pino=" XIDT " fname=%s ftype=%d prefix_len=%d ino=" XIDT " ftype=%d\n", dit->dot_entry.inode->ino,  entry->fname, ftype, prefix_len, entry->inode->ino, entry->inode->ftype);

     _lfs_inode_access(dit->lfs, entry->inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk
     lfs_fill_stat(&stbuf, entry->inode);
     skip = entry->flagged;

     lfs_unlock(dit->lfs);

     move_to_bottom(dit->stack);
     insert_below(dit->stack, entry);
     off++;

     if (skip == LFS_INODE_OK) {
        n = fuse_add_direntry(req, NULL, 0, dentry_name(entry), NULL, 0);
        if ((bpos+n) >= size) { //** Filled the buffer
           lfs_unlock(dit->lfs);
           fuse_reply_buf(req, buf, bpos+1);
           free(buf);
           return;
        }

        fuse_add_direntry(req, &(buf[bpos]), n, dentry_name(entry), &stbuf, off);
        bpos += n;
     }
  }

}


//*************************************************************************
// lfs_opendir - FUSE opendir call
//*************************************************************************

void lfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit;
  lio_dentry_t *entry;
  lio_inode_t *inode;
  char *dir, *file;
  char path[OS_PATH_MAX];
  int i;

  log_printf(1, "ino=%lu\n", ino); flush_log();

  //** First find the inode
  lfs_lock(lfs_gc);
  inode = _lfs_inode_lookup(lfs_gc, ino);

  if (inode == NULL) {  //** Can't find the inode so kick out
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  if ((inode->ftype & OS_OBJECT_DIR) == 0) {  //** Not a directory
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOTDIR);
     return;
  }

  //** If we made it here we have a valid inode
  type_malloc_clear(dit, lfs_dir_iter_t, 1);

  for (i=0; i<_inode_key_size; i++) {
    dit->v_size[i] = -lfs_gc->lc->max_attr;
    dit->val[i] = NULL;
  }

  dit->lfs = lfs_gc;
  snprintf(path, OS_PATH_MAX, "%s/*", inode->entry1->fname);
  dit->path_regex = os_path_glob2regex(path);

  dit->it = os_create_object_iter_alist(dit->lfs->lc->os, dit->lfs->lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY, 0, _inode_keys, (void **)dit->val, dit->v_size, _inode_key_size);

  dit->stack = new_stack();

  _lfs_inode_access(lfs_gc, inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk

  entry = _lfs_dentry_lookup(lfs_gc, inode->entry1->fname);
  dit->dot_entry = *entry;
  dit->dot_entry.fname = ".";
  dit->dot_entry.name_start = 0;

  if (strcmp(inode->entry1->fname, "") == 0) {
     inode = _lfs_inode_lookup(lfs_gc, FUSE_ROOT_ID);
     entry = inode->entry1;
  } else {
     os_path_split(inode->entry1->fname, &dir, &file);
     entry = _lfs_dentry_lookup(lfs_gc, dir);
     if (entry == NULL) {  //** Got to load it
        entry = _lfs_load_inode_entry(lfs_gc, dir, NULL);
        inode = entry->inode;
     } else {
       inode = entry->inode;
       free(dir);
     }
     free(file);
  }

  _lfs_inode_access(lfs_gc, inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk
  dit->dotdot_entry = *entry;
  dit->dotdot_entry.fname = "..";
  dit->dotdot_entry.name_start = 0;


  insert_below(dit->stack, &dit->dot_entry);
  insert_below(dit->stack, &dit->dotdot_entry);

  lfs_unlock(lfs_gc);

  dit->state = 0;

  //** Compose our reply
  fi->fh = (uint64_t)dit;
  fuse_reply_open(req, fi);
  return;
}

//*************************************************************************
// lfs_closedir - Closes the opendir file handle
//*************************************************************************

void lfs_closedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
  lio_dentry_t *entry;

  log_printf(1, "ino=%lu START\n", ino); flush_log();

  if (dit == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  //** Cyle through releasing all the inodes
  lfs_lock(dit->lfs);
  while ((entry = (lio_dentry_t *)pop(dit->stack)) != NULL) {
log_printf(0, "pino=%lu fname=%s\n", ino, entry->fname); flush_log();
//     if ((strcmp(dentry->fname, ".") == 0) || (strcmp(dentry->fname, "..") == 0)) {
//        inode = _lfs_inode_lookup(dit->lfs, inode->ino, NULL);  //** Resolve . and .. correctly
//     }
     _lfs_inode_release(dit->lfs, entry->inode, 0, 1);
  }
  lfs_unlock(dit->lfs);

  free_stack(dit->stack, 0);

  os_destroy_object_iter(dit->lfs->lc->os, dit->it);
  os_regex_table_destroy(dit->path_regex);
  free(dit);

  log_printf(1, "ino=%lu END\n", ino); flush_log();

  fuse_reply_err(req, 0);
  return;
}

//*************************************************************************
// lfs_object_create
//*************************************************************************

int lfs_object_create(lio_fuse_t *lfs, fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, int ftype)
{
  struct fuse_entry_param fentry;
  lio_inode_t *inode, *pinode;
  lio_dentry_t *entry;
  char fullname[OS_PATH_MAX];
  char *tmp;
  int err, n;

  log_printf(1, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs, parent);
  if (pinode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return(1);
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->entry1->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->entry1->fname, name, fullname);

  entry = _lfs_dentry_lookup(lfs, fullname);
  if (entry != NULL) { //** Oops it already exists
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return(1);
  }

  n = lioc_exists(lfs->lc, lfs->lc->creds, fullname);
  if (n != 0) {  //** File already exists
     log_printf(15, "File already exist! fname=%s\n", fullname);
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return(1);
   }

  //** If we made it here it's a new file or dir
  //** Create the new object
  tmp = strdup(fullname);  //** _lfs_load_inode_entry will remove this on failure automatically
  err = gop_sync_exec(lio_create_object(lfs->lc, lfs->lc->creds, tmp, ftype, NULL, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Error creating object! fname=%s\n", fullname);
     lfs_unlock(lfs);
     if (strlen(fullname) > 3900) {  //** Probably a path length issue
        fuse_reply_err(req, ENAMETOOLONG);
     } else {
        fuse_reply_err(req, EREMOTEIO);
     }
     return(1);
  }

  //** Load the inode
  entry = _lfs_load_inode_entry(lfs, tmp, NULL);
  if (entry == NULL) { //** File doesn't exist!
     log_printf(15, "File doesn't exist! fname=%s\n", fullname);
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return(1);
  }

  inode = entry->inode;

  //** Update the ref count
  _lfs_inode_access(lfs_gc, inode, 1, 0);

  //** Form the response
  fentry.ino = inode->ino;
  fentry.generation = 0;
  fentry.attr_timeout = lfs_gc->attr_to;
  fentry.entry_timeout = lfs_gc->entry_to;

  lfs_fill_stat(&(fentry.attr), inode);

log_printf(15, "fullname=%s ino=" XIDT " ftype=%d\n", inode->entry1->fname, inode->ino, inode->ftype);

  lfs_unlock(lfs);

  fuse_reply_entry(req, &fentry);

  return(0);
}

//*************************************************************************
// lfs_rename - Renames a file
//*************************************************************************

void lfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname)
{
  lio_fuse_t *lfs = lfs_gc;
  lio_inode_t *s_inode, *d_inode;
  lio_dentry_t *d_entry, *s_entry;
  char fullname[OS_PATH_MAX];
  int err;

  log_printf(1, "parent_ino=%lu name=%s new_parent_ino=%lu newname=%s\n", parent, name, newparent, newname); flush_log();

  lfs_lock(lfs);

  //** Lookup the parents
  s_inode = _lfs_inode_lookup(lfs, parent);
  if (s_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  d_inode = _lfs_inode_lookup(lfs, newparent);
  if (d_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", s_inode->entry1->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", s_inode->entry1->fname, name, fullname);

  s_entry = _lfs_dentry_lookup(lfs, fullname);
  if (s_entry == NULL) { //** Oops it doesn't exist
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  //** Do the same for the dest
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", d_inode->entry1->fname, newname);
  d_entry = _lfs_dentry_lookup(lfs, fullname);
  if (d_entry != NULL) { //** Oops it already exists and we don;t currently support this scenario
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  //** Now do the move
  err = gop_sync_exec(lio_move_object(lfs->lc, lfs->lc->creds, s_entry->fname, fullname));
  if (err != OP_STATE_SUCCESS) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EIO);
     return;
  }

  //** Update the dentry name
  s_inode = s_entry->inode;
  _lfs_dentry_remove(lfs, s_entry);
  type_malloc_clear(s_entry, lio_dentry_t, 1);
  s_entry->fname = strdup(fullname);
  s_entry->inode = s_inode;
  _lfs_dentry_insert(lfs, s_entry);

  lfs_unlock(lfs);

  fuse_reply_err(req, 0);
  return;
}

//*************************************************************************
// lfs_hardlink - Creates a hardlink to an existing file
//*************************************************************************

void lfs_hardlink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname)
{
  lio_fuse_t *lfs = lfs_gc;
  lio_inode_t *d_inode, *p_inode;
  lio_dentry_t *entry;
  struct fuse_entry_param fentry;
  char fullname[OS_PATH_MAX];
  int err;

  log_printf(1, "ino=%lu newparent_ino=%lu newname=%s\n", ino, newparent, newname); flush_log();

  lfs_lock(lfs);

  //** Lookup the dest inode
  d_inode = _lfs_inode_lookup(lfs, ino);
  if (d_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Lookup the parent
  p_inode = _lfs_inode_lookup(lfs, newparent);
  if (p_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Make sure the new entry doesn't exist
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", p_inode->entry1->fname, newname);
  entry = _lfs_dentry_lookup(lfs, fullname);
  if (entry != NULL) { //** Oops it already exists
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  //** Now do the hard link
  err = gop_sync_exec(lio_link_object(lfs->lc, lfs->lc->creds, 0, d_inode->entry1->fname, fullname, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EIO);
     return;
  }

  //** Update the ref count
  _lfs_inode_access(lfs, d_inode, 1, 0);

  //** Form the response
  fentry.ino = d_inode->ino;
  fentry.generation = 0;
  fentry.attr_timeout = lfs_gc->attr_to;
  fentry.entry_timeout = lfs_gc->entry_to;

  lfs_fill_stat(&(fentry.attr), d_inode);

  lfs_unlock(lfs);

  fuse_reply_entry(req, &fentry);
  return;
}


//*************************************************************************
// lfs_mknod - Makes a regular file
//*************************************************************************

void lfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
  log_printf(1, "parent=%lu name=%s\n", parent, name); flush_log();

  lfs_object_create(lfs_gc, req, parent, name, mode, OS_OBJECT_FILE);
}


//*************************************************************************
// lfs_mkdir - Makes a directory
//*************************************************************************

void lfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
  log_printf(1, "parent=%lu name=%s mode=%d\n", parent, name, mode); flush_log();

  lfs_object_create(lfs_gc, req, parent, name, mode, OS_OBJECT_DIR);
}

//*****************************************************************
//  lfs_object_remove - Removes a file or directory
//*****************************************************************

void lfs_object_remove(lio_fuse_t *lfs, fuse_req_t req, fuse_ino_t parent, const char *name)
{
  lio_inode_t *inode, *pinode;
  lio_dentry_t *entry;
  char fullname[OS_PATH_MAX];
  int defer_reply, err, ndentry;

  log_printf(15, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs, parent);
  if (pinode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->entry1->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->entry1->fname, name, fullname);

  entry = _lfs_dentry_lookup(lfs, fullname);
  if (entry == NULL) { //** Oops it doesn't exist
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  inode = entry->inode;
  defer_reply = inode->ftype & OS_OBJECT_DIR;

  ndentry = 1;
  if (inode->dentry_stack != NULL) ndentry = stack_size(inode->dentry_stack) + 1;
defer_reply = 100;
log_printf(15, "dentry_size=%d nlinks=%d defer_reply=%d ftype=%d\n", ndentry, inode->nlinks, defer_reply, inode->ftype);
  if ((ndentry > 1) && (inode->nlinks>1) && (defer_reply == 0)) {  //** Got more entries so safe to delete this one now
     lfs_unlock(lfs);  //** Release the global lock while we delete things

     err = gop_sync_exec(lio_remove_object(lfs->lc, lfs->lc->creds, entry->fname, NULL, inode->ftype));
log_printf(1, "remove err=%d\n", err);
     if (err == OP_STATE_SUCCESS) {
        fuse_reply_err(req, 0);
     } else if ((inode->ftype & OS_OBJECT_DIR) > 0) { //** Most likey the dirs not empty
        fuse_reply_err(req, ENOTEMPTY);
     } else {
        fuse_reply_err(req, EBUSY);  //** Otherwise throw a generic error
     }

     lfs_lock(lfs);  //** Get it back
     inode->nlinks--;
     _lfs_dentry_remove(lfs, entry);
     lfs_unlock(lfs);
     return;
  }

  //** Deferred removal
log_printf(1, "deffered removal. entry=%s\n", entry->fname);
  inode->flagged_object = LFS_INODE_DELETE;  //** Mark it for deletion
  entry->flagged = LFS_INODE_DELETE;
//  entry->req = req;
fuse_reply_err(req, 0);

  if (inode->remove_stack == NULL) inode->remove_stack = new_stack();
  push(inode->remove_stack, entry);

  _lfs_inode_release(lfs, inode, 0, 0);  //** Release it which should trigger the removal

  lfs_unlock(lfs);
}

//*****************************************************************
//  lfs_unlink - Remove a file
//*****************************************************************

void lfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  log_printf(1, "parent=%lu name=%s\n", parent, name); flush_log();

  lfs_object_remove(lfs_gc, req, parent, name);
}

//*****************************************************************
//  lfs_rmdir - Remove a directory
//*****************************************************************

void lfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  log_printf(1, "parent=%lu name=%s\n", parent, name); flush_log();

  lfs_object_remove(lfs_gc, req, parent, name);
}

//*****************************************************************
// lfs_set_tape_attr - Disburse the tape attribute
//*****************************************************************

void lfs_set_tape_attr(lio_fuse_t *lfs, lio_inode_t *inode, char *mytape_val, int tape_size)
{
  char *val[_tape_key_size], *tape_val, *bstate, *tmp;
  int v_size[_tape_key_size];
  int n, i, fin, ex_key, err;
  exnode_exchange_t *exp;
  exnode_t *ex, *cex;
  exnode_abstract_set_t my_ess;

  type_malloc(tape_val, char, tape_size+1);
  memcpy(tape_val, mytape_val, tape_size);
  tape_val[tape_size] = 0;  //** Just to be safe with the string/prints routines

log_printf(15, "fname=%s tape_size=%d\n", inode->entry1->fname, tape_size);
log_printf(15, "Tape attribute follows:\n%s\n", tape_val);

  //** The 1st key should be n_keys
  tmp = string_token(tape_val, "=\n", &bstate, &fin);
  if (strcmp(tmp, "n_keys") != 0) { //*
     log_printf(0, "ERROR parsing tape attribute! Missing n_keys! fname=%s\n", inode->entry1->fname);
     log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
     free(tape_val);
     return;
  }

  sscanf(string_token(NULL, "=\n", &bstate, &fin), "%d", &n);
  if (n != _tape_key_size) {
     log_printf(0, "ERROR parsing n_keys size fname=%s\n", inode->entry1->fname);
     log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
     free(tape_val);
     return;
  }

  for (i=0; i<_tape_key_size; i++) { v_size[i] = 0; }

  //** Parse the sizes
  for (i=0; i<_tape_key_size; i++) {
    tmp = string_token(NULL, "=\n", &bstate, &fin);
    if (strcmp(tmp, _tape_keys[i]) == 0) {
       sscanf(string_token(NULL, "=\n", &bstate, &fin), "%d", &(v_size[i]));
       if (v_size[i] < 0) {
          log_printf(0, "ERROR parsing key=%s size=%d fname=%s\n", tmp, v_size[i], inode->entry1->fname);
          log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
          free(tape_val);
          return;
       }
    } else {
      log_printf(0, "ERROR Missing key=%s\n", _tape_keys[i]);
      log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
      free(tape_val);
      return;
    }
  }

  //** Split out the attributes
  n = 0;
  for (i=0; i<_tape_key_size; i++) {
      val[i] = NULL;
      if (v_size[i] > 0) {
         type_malloc(val[i], char, v_size[i]+1);
         memcpy(val[i], &(bstate[n]), v_size[i]);
         val[i][v_size[i]] = 0;
         n = n + v_size[i];
         log_printf(15, "fname=%s key=%s val=%s\n", inode->entry1->fname, _tape_keys[i], val[i]);
      }
  }

  //** Just need to process the exnode
  ex_key = 1;  //** tape_key index for exnode
  if (v_size[ex_key] > 0) {
     //** If this has a caching segment we need to disable it from being adding
     //** to the global cache table cause there could be multiple copies of the
     //** same segment being serialized/deserialized.
     my_ess = *(lfs->lc->ess);
     my_ess.cache = NULL;

     //** Deserialize it
     exp = exnode_exchange_create(EX_TEXT);
     exp->text = val[ex_key];
     ex = exnode_create();
     exnode_deserialize(ex, exp, &my_ess);
     free(val[ex_key]); val[ex_key] = NULL;
     exp->text = NULL;

     //** Execute the clone operation
     err = gop_sync_exec(exnode_clone(lfs->lc->tpc_unlimited, ex, lfs->lc->da, &cex, NULL, CLONE_STRUCTURE, lfs->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR cloning parent fname=%s\n", inode->entry1->fname);
     }

     //** Serialize it for storage
     exnode_serialize(cex, exp);
     val[ex_key] = exp->text;
     v_size[ex_key] = strlen(val[ex_key]);
     exp->text = NULL;
     exnode_exchange_destroy(exp);
     exnode_destroy(ex);
     exnode_destroy(cex);
  }

  //** Store them
  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, _tape_keys, (void **)val, v_size, _tape_key_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating exnode! fname=%s\n", inode->entry1->fname);
  }

  //** Clean up
  free(tape_val);
  for (i=0; i<_tape_key_size; i++) { if (val[i] != NULL) free(val[i]); }

  return;
}

//*****************************************************************
// lfs_get_tape_attr - Retreives the tape attribute
//*****************************************************************

void lfs_get_tape_attr(lio_fuse_t *lfs, lio_inode_t *inode, char **tape_val, int *tape_size)
{
  char *val[_tape_key_size];
  int v_size[_tape_key_size];
  int n, i, j, used;
  int hmax= 1024;
  char *buffer, header[hmax];

  *tape_val = NULL;
  *tape_size = 0;

  for (i=0; i<_tape_key_size; i++) {
    val[i] = NULL;
    v_size[i] = -lfs->lc->max_attr;
  }
  i = lioc_get_multiple_attrs(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, _tape_keys, (void **)val, v_size, _tape_key_size);
  if (i != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", inode->entry1->fname);
     return;
  }

  //** Figure out how much space we need
  n = 0;
  used = 0;
  append_printf(header, &used, hmax, "n_keys=%d\n", _tape_key_size);
  for (i=0; i<_tape_key_size; i++) {
     j = (v_size[i] > 0) ? v_size[i] : 0;
     n = n + 1 + j;
     append_printf(header, &used, hmax, "%s=%d\n", _tape_keys[i], j);
  }

  //** Copy all the data into the buffer;
  n = n + used;
  type_malloc_clear(buffer, char, n);
  n = used;
  memcpy(buffer, header, used);
  for (i=0; i<_tape_key_size; i++) {
     if (v_size[i] > 0) {
        memcpy(&(buffer[n]), val[i], v_size[i]);
        n = n + v_size[i];
        free(val[i]);
     }
  }

  *tape_val = buffer;
  *tape_size = n;
  return;
}

//*****************************************************************
// lfs_removexattr - Removes an extended attribute
//*****************************************************************

void lfs_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  lio_inode_t *inode;
//  char *val;
  int v_size, err;
  lio_fuse_t *lfs = lfs_gc;

  log_printf(1, "ino=%lu attr_name=%s\n", ino, name); flush_log();

  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) { fuse_reply_err(req, 0); return; }

  //** Lookup the inode
  lfs_lock(lfs);
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  v_size = -1;
//  val = NULL;
  err = lioc_set_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, (char *)name, NULL, v_size);
  if (err != OP_STATE_SUCCESS) {
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  fuse_reply_err(req, 0);

finished:
  lfs_unlock(lfs);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);
}


//*****************************************************************
// lfs_setxattr - Sets a extended attribute
//*****************************************************************

void lfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *fval, size_t size, int flags)
{
  lio_inode_t *inode;
  char *val;
  int v_size, err;
  lio_fuse_t *lfs = lfs_gc;

  v_size= size;
  log_printf(1, "ino=%lu size=%d attr_name=%s\n", ino, size, name); flush_log();

  //** Lookup the inode
  lfs_lock(lfs);
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  if (flags != 0) { //** Got an XATTR_CREATE/XATTR_REPLACE
     v_size = 0;
     val = NULL;
     err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, (char *)name, (void **)&val, &v_size);
     if (flags == XATTR_CREATE) {
        if (err == OP_STATE_SUCCESS) {
           fuse_reply_err(req, EEXIST);
           goto finished;
        }
     } else if (flags == XATTR_REPLACE) {
       if (err != OP_STATE_SUCCESS) {
          fuse_reply_err(req, ENOATTR);
          goto finished;
       }
     }
  }

  v_size = size;
  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {  //** Got the tape attribute
     lfs_set_tape_attr(lfs, inode, (char *)fval, v_size);
  } else {
     err = lioc_set_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, (char *)name, (void *)fval, v_size);
     if (err != OP_STATE_SUCCESS) {
        fuse_reply_err(req, ENOENT);
        goto finished;
     }
  }

  fuse_reply_err(req, 0);

finished:
  lfs_unlock(lfs);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);
}

//*****************************************************************
// lfs_getxattr - Gets a extended attributes
//*****************************************************************

void lfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size)
{
  lio_inode_t *inode;
  char *val;
  int v_size, err;
  lio_fuse_t *lfs = lfs_gc;

  v_size= size;
  log_printf(1, "ino=%lu size=%d attr_name=%s\n", ino, size, name); flush_log();


  //** Lookup the inode
  lfs_lock(lfs);
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  v_size = (size == 0) ? -lfs->lc->max_attr : -size;
  val = NULL;

  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {  //** Want the tape backup attr
     lfs_get_tape_attr(lfs, inode, &val, &v_size);
  } else {
     err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, (char *)name, (void **)&val, &v_size);
     if (err != OP_STATE_SUCCESS) {
        fuse_reply_err(req, ENOENT);
        goto finished;
     }
  }

  if (v_size < 0) v_size = 0;  //** No attribute

  if (size == 0) {
log_printf(15, "SIZE bpos=%d buf=%s\n", v_size, val);
    fuse_reply_xattr(req, v_size);
  } else if (size >= v_size) {
log_printf(15, "FULL bpos=%d buf=%s\n",v_size, val);
    fuse_reply_buf(req, val, v_size);
  } else {
log_printf(15, "ERANGE bpos=%d buf=%s\n", v_size, val);
    fuse_reply_err(req, ERANGE);
  }
  if (val != NULL) free(val);

finished:
  lfs_unlock(lfs);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);
}

//*****************************************************************
// lfs_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

void lfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  lio_inode_t *inode;
  char *buf, *key, *val;
  int bpos, bufsize, v_size, n, i, err;
  os_regex_table_t *attr_regex;
  os_attr_iter_t *it;
  os_fd_t *fd;
  lio_fuse_t *lfs = lfs_gc;

  bpos= size;
  log_printf(1, "ino=%lu size=%d\n", ino, bpos); flush_log();

  lfs_lock(lfs);

  //** Lookup the inode
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  //** Make an iterator
  attr_regex = os_path_glob2regex("user.*");
  err = gop_sync_exec(os_open_object(lfs->lc->os, lfs->lc->creds, inode->entry1->fname, OS_MODE_READ_IMMEDIATE, lfs->id, &fd, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR: opening file: %s err=%d\n", inode->entry1->fname, err);
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  it = os_create_attr_iter(lfs->lc->os, lfs->lc->creds, fd, attr_regex, 0);
  if (it == NULL) {
     log_printf(15, "ERROR creating iterator for fname=%s\n", inode->entry1->fname);
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  //** Cycle through the keys
  bufsize = 10*1024;
  type_malloc_clear(buf, char, bufsize);
  val = NULL;
  bpos = 0;

  if (lfs->enable_tape == 1) { //** Add the tape attribute
      strcpy(buf, LFS_TAPE_ATTR);
      bpos = strlen(buf) + 1;
  }
  while (os_next_attr(lfs->lc->os, it, &key, (void **)&val, &v_size) == 0) {
     n = strlen(key);
     if ((n+bpos) > bufsize) {
        bufsize = bufsize + n + 10*1024;
        buf = realloc(buf, bufsize);
     }

log_printf(15, "adding key=%s bpos=%d\n", key, bpos);
     for (i=0; ; i++) {
        buf[bpos] = key[i];
        bpos++;
        if (key[i] == 0) break;
     }
     free(key);

     v_size = 0;
  }

  os_destroy_attr_iter(lfs->lc->os, it);
  gop_sync_exec(os_close_object(lfs->lc->os, fd));
  os_regex_table_destroy(attr_regex);

i= size;

//strcpy(buf, "foo");
//bpos=4;

log_printf(15, "bpos=%d size=%d buf=%s\n", bpos, size, buf);
//printf("buffer=");
//for (i=0;i<bpos; i++) printf("[%c]",buf[i]);
//printf("\n");

  if (size == 0) {
log_printf(15, "SIZE bpos=%d buf=%s\n", bpos, buf);
    fuse_reply_xattr(req, bpos);
  } else if (size > bpos) {
log_printf(15, "FULL bpos=%d buf=%s\n", bpos, buf);
    fuse_reply_buf(req, buf, bpos);
  } else {
log_printf(15, "ERANGE bpos=%d buf=%s\n", bpos, buf);
    fuse_reply_err(req, ERANGE);
  }
  free(buf);

finished:
  lfs_unlock(lfs);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);
}

//*****************************************************************
// lfs_load_file_handle - Loads the shared file handle
//*****************************************************************

lio_fuse_file_handle_t *lfs_load_file_handle(lio_fuse_t *lfs, lio_inode_t *inode)
{
  lio_fuse_file_handle_t *fh;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  char *ex_data;
  int v_size, err;

  log_printf(15, "loading exnode in=" XIDT " fname=%s\n", inode->ino, inode->entry1->fname);

  //** Get the exnode
  v_size = -lfs->lc->max_attr;
  err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, "system.exnode", (void **)&ex_data, &v_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving exnode! path=%s\n", inode->entry1->fname);
     return(NULL);
  }

  lfs_lock(lfs);  //** Want to make sure I'm the only one doing the loading

  if (inode->fh != NULL) {  //** Someone beat me to it so use theirs
     inode->fh->ref_count++;
     _lfs_inode_access(lfs, inode, 0, 1);
     lfs_unlock(lfs);
     return(inode->fh);
  }

  //** Load it
  exp = exnode_exchange_create(EX_TEXT);
  exp->text = ex_data;
  ex = exnode_create();
  exnode_deserialize(ex, exp, lfs->lc->ess);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     log_printf(0, "No default segment!  Aborting! fname=%s\n", inode->entry1->fname);
     exnode_destroy(ex);
     lfs_unlock(lfs);
     return(NULL);
  }

  //** Make the shared handle
  type_malloc_clear(fh, lio_fuse_file_handle_t, 1);
  fh->inode = inode;
  fh->ex = ex;
  fh->seg = seg;
  fh->lfs = lfs;
  fh->ref_count = 1;
  inode->fh = fh;

  lfs_unlock(lfs);  //** Now we can release the lock

  exnode_exchange_destroy(exp);  //** Clean up

  return(fh);
}

//*****************************************************************
// lfs_myopen - Opens a file for I/O
//*****************************************************************

int lfs_myopen(lio_fuse_t *lfs, fuse_ino_t ino, int flags, lio_fuse_fd_t **myfd)
{
  lio_inode_t *inode;
  lio_fuse_fd_t *fd;
  lio_fuse_file_handle_t *fh;

  log_printf(15, "ino=%lu\n", ino); flush_log();

  *myfd = NULL;

  lfs_lock(lfs);

  //** Lookup the inode
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(ENOENT);
  }

  fh = inode->fh;
  if (fh != NULL) fh->ref_count++;
  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  if (fh == NULL) { //** Not currently open so need to load it
     fh = lfs_load_file_handle(lfs, inode);
  }

  if (fh == NULL) { //** Failed getting the shared file handle so return an error
     return(ENOENT);
  }

  //** Make the file handle
  type_malloc_clear(fd, lio_fuse_fd_t, 1);
  fd->fh = fh;
  fd->mode = O_RDONLY;
  if (flags & O_RDONLY) {
     fd->mode = LFS_READ_MODE;
  } else if (flags & O_WRONLY) {
     fd->mode = LFS_WRITE_MODE;
  } else if (flags & O_RDWR) {
     fd->mode = LFS_READ_MODE | LFS_WRITE_MODE;
  }

  *myfd = fd;
  return(0);
}


//*****************************************************************
// lfs_open - Opens a file for I/O
//*****************************************************************

void lfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lio_fuse_fd_t *fd;
  int err;

  log_printf(1, "ino=%lu dio=%d\n", ino, fi->direct_io); flush_log();

//fi->direct_io = 1;

  err = lfs_myopen(lfs_gc, ino, fi->flags, &fd);

  if (err == 0) {
     fi->fh = (uint64_t)fd;
     err = fuse_reply_open(req, fi);
log_printf(1, "ino=%lu fuse_reply_open=%d\n", ino, err);
  } else {
     fuse_reply_err(req, err);
  }

  return;
}

//*****************************************************************
// lfs_myclose - Closes a file
//*****************************************************************

int lfs_myclose(lio_fuse_fd_t *fd)
{
  lio_fuse_t *lfs;
  lio_fuse_file_handle_t *fh;
  exnode_exchange_t *exp;
  ex_off_t ssize;
  char buffer[32];
  char *key[] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data"};
  char *val[3];
  int err, v_size[3];

  log_printf(1, "ino=" XIDT " modified=%d count=%d\n", fd->fh->inode->ino, fd->fh->modified, fd->fh->ref_count); flush_log();

  //** Get the handles
  fh = fd->fh;
  lfs = fh->lfs;

  free(fd);  //** Free the fd

  lfs_lock(lfs);

  //** Dec the ref count and see if we return early
  fh->ref_count--;

  if (fh->ref_count > 0) {
     _lfs_inode_release(lfs, fh->inode, 0, 1);
     lfs_unlock(lfs);
     return(0);
  }

  lfs_unlock(lfs);

  //** Flush and truncate everything which could take some time
  err = gop_sync_exec(segment_flush(fh->seg, lfs->lc->da, 0, segment_size(fh->seg)+1, lfs->lc->timeout));
  err = gop_sync_exec(segment_truncate(fh->seg, lfs->lc->da, segment_size(fh->seg), lfs->lc->timeout));

  //** Now acquire the global lock and check again before tearing down the segemnt
  lfs_lock(lfs);

  if (fh->ref_count > 0) {  //** Oops someone opend the file while I was waiting
     _lfs_inode_release(lfs, fh->inode, 0, 1);
     lfs_unlock(lfs);
     return(0);
  }

  fh->inode->fh = NULL;

  //** Ok no one has the file opened so teardown the segment/exnode
  //** IF not modified just tear down and clean up
  if (fh->modified == 0) {
     exnode_destroy(fh->ex);
     _lfs_inode_release(lfs, fh->inode, 0, 1);
     lfs_unlock(lfs);
     free(fh);
     return(0);
  }

  //** Serialize the exnode
  exp = exnode_exchange_create(EX_TEXT);
  exnode_serialize(fh->ex, exp);
  ssize = segment_size(fh->seg);

  exnode_destroy(fh->ex);

  lfs_unlock(lfs);  //** Now we can release the lock while we update the object

  //** Update the OS exnode
  val[0] = exp->text;  v_size[0] = strlen(val[0]);
  sprintf(buffer, XOT, ssize);
  val[1] = buffer; v_size[1] = strlen(val[1]);
  val[2] = NULL; v_size[2] = 0;
  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, fh->inode->entry1->fname, NULL, key, (void **)val, v_size, 3);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating exnode! fname=%s\n", fh->inode->entry1->fname);
  }

  lfs_lock(lfs);  //** Do the final release
  _lfs_inode_release(lfs, fh->inode, 0, 1);
  lfs_unlock(lfs);

  //** Clean up
  free(fh);
  exnode_exchange_destroy(exp);

  return(0);
}

//*****************************************************************
// lfs_release - Closes a file
//*****************************************************************

void lfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lio_fuse_fd_t *fd;

  log_printf(1, "ino=%lu\n", ino); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  fuse_reply_err(req, lfs_myclose(fd));
  return;
}

//*************************************************************************
// lfs_setattr - Sets the stat info
//*************************************************************************

//#define FUSE_SET_ATTR_MODE	(1 << 0)
//#define FUSE_SET_ATTR_UID	(1 << 1)
//#define FUSE_SET_ATTR_GID	(1 << 2)
//#define FUSE_SET_ATTR_SIZE	(1 << 3)
//#define FUSE_SET_ATTR_ATIME	(1 << 4)
//#define FUSE_SET_ATTR_MTIME	(1 << 5)
//#define FUSE_SET_ATTR_ATIME_NOW	(1 << 7)
//#define FUSE_SET_ATTR_MTIME_NOW	(1 << 8)


void lfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
  lio_inode_t *inode;
  lio_fuse_t *lfs = lfs_gc;
  lio_fuse_fd_t *fd;
  ex_off_t ts;
  int n, err;
  char buf[2][1024];
  char *key[2];
  char *val[2];
  int v_size[2];

  log_printf(1, "ino=%lu\n", ino); flush_log();

  lfs_lock(lfs);

  //** Lookup the inode
  inode = _lfs_inode_lookup(lfs, ino);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  n = 0;
  if (to_set & FUSE_SET_ATTR_SIZE) {
     ts = attr->st_size;
     log_printf(15, "adjusting size=" XOT "\n", ts);
     lfs_myopen(lfs, ino, O_WRONLY, &fd);
     if (fd == NULL) {
        fuse_reply_err(req, EBADF);
        lfs_lock(lfs);
        _lfs_inode_release(lfs, inode, 0, 1);
        lfs_unlock(lfs);
        return;
     }

     log_printf(15, "calling truncate\n");
     err = gop_sync_exec(segment_truncate(fd->fh->seg, lfs->lc->da, attr->st_size, lfs->lc->timeout));
     log_printf(15, "segment_truncate=%d\n", err);

     inode->size = attr->st_size;
     atomic_set(fd->fh->modified, 1);

     lfs_myclose(fd);
  }

  if (to_set & FUSE_SET_ATTR_MTIME) {
    key[n] = "system.modify_attr";
    val[n] = buf[n];
    ts = attr->st_mtime;
    snprintf(val[n], 1024, XOT "|%s", ts, lfs->id);
    v_size[n] = strlen(val[n]);
    n++;
  }

  if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
    key[n] = "os.timestamp.system.modify_attr";
    val[n] = lfs->id;
    v_size[n] = strlen(val[n]);
    n++;
  }

  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, key, (void **)val, v_size, n);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating stat! fname=%s\n", inode->entry1->fname);
     fuse_reply_err(req, EBADE);
     lfs_lock(lfs);
     _lfs_inode_release(lfs, inode, 0, 1);
     lfs_unlock(lfs);
     return;
  }


  lfs_lock(lfs);
  lfs_fill_stat(attr, inode);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  fuse_reply_attr(req, attr, lfs->attr_to);

  return;
}

//*****************************************************************
//  lfs_rw_cb - READ/WRITE callback
//*****************************************************************

void lfs_rw_cb(void *arg, int op_status)
{
  lfs_rw_cb_t *op = (lfs_rw_cb_t *)arg;

log_printf(1, " ino=" XIDT " mode=%d off=" XOT " size=" XOT " status=%d\n", op->ino, op->rw_mode, op->off, op->size, op_status);
  if (op->rw_mode == 0) { //** Read op
     if (op_status == OP_STATE_SUCCESS) {
       fuse_reply_buf(op->req, op->buf, op->size);
     } else {
       fuse_reply_err(op->req, EIO);
     }
     free(op->buf);

  } else { //** Write op
     if (op_status == OP_STATE_SUCCESS) {
        fuse_reply_err(op->req, EIO);
     } else {
        fuse_reply_write(op->req, op->size);
     }
  }

  free(op);

//  apr_thread_mutex_lock(op->lfs->rw_lock);
//  push(op->lfs->rw_stack, op);
//  apr_thread_mutex_unlock(op->lfs->rw_lock);

  return;
}

//*****************************************************************
//  lfs_rw_thread - Handles the R/W cleanup of the callbacks
//*****************************************************************

void *OLD_lfs_rw_thread(apr_thread_t *th, void *data)
{
  lio_fuse_t *lfs = (lio_fuse_t *)data;
  void *p;

  apr_thread_mutex_lock(lfs->rw_lock);
  while (lfs->shutdown == 0) {
    while ((p = pop(lfs->rw_stack)) != NULL) {
       free(p);
    }
    apr_thread_mutex_unlock(lfs->rw_lock);
    usleep(100000);
    apr_thread_mutex_lock(lfs->rw_lock);
  }

  while ((p = pop(lfs->rw_stack)) != NULL) {
     free(p);
  }
  apr_thread_mutex_unlock(lfs->rw_lock);

  return(NULL);
}

//****************************************************
void *lfs_rw_thread(apr_thread_t *th, void *data)
{
  lio_fuse_t *lfs = (lio_fuse_t *)data;
  op_generic_t *gop;
  int n;

return(NULL);
  apr_thread_mutex_lock(lfs->rw_lock);
  while (lfs->shutdown == 0) {
    apr_thread_mutex_unlock(lfs->rw_lock);
    n = 0;
    while ((gop = opque_waitany(lfs->q)) != NULL) {
       n++;
       gop_free(gop, OP_DESTROY);
    }
    log_printf(1, "Destroyed %d ops\n", n);
    usleep(1000000);
    apr_thread_mutex_lock(lfs->rw_lock);
  }

  apr_thread_mutex_unlock(lfs->rw_lock);

  while ((gop = opque_waitany(lfs->q)) != NULL) {
    gop_free(gop, OP_DESTROY);
  }

  return(NULL);
}


//*****************************************************************
// lfs_read - Reads data from a file
//*****************************************************************

void NEW_lfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  lfs_rw_cb_t *op;
  op_generic_t *gop;
  callback_t *cb;
  ex_off_t ssize, pend;
ex_off_t t1, t2;
  t1 = size; t2 = off;
  log_printf(1, "ino=%lu size=" XOT " off=" XOT "\n", ino, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  lfs = fd->fh->lfs;

  //** Do the read op
  type_malloc_clear(op, lfs_rw_cb_t, 1);
  type_malloc_clear(cb, callback_t, 1);
  ssize = segment_size(fd->fh->seg);
  pend = off + size;
  if (pend > ssize) size = ssize - off;  //** Tweak the size based on how much data there is
  type_malloc(op->buf, char, size);
  op->rw_mode = 0;
  op->lfs = fd->fh->lfs;
  op->size = size;
  op->off = off;
  op->req = req;
  tbuffer_single(&(op->tbuf), size, op->buf);
  ex_iovec_single(&(op->exv), off, size);
  gop = segment_read(fd->fh->seg, lfs->lc->da, 1, &(op->exv), &(op->tbuf), 0, lfs->lc->timeout);
//  gop_set_auto_destroy(gop, 1);
  callback_set(cb, lfs_rw_cb, op);
  gop_callback_append(gop, cb);

  opque_add(lfs->q, gop);
  return;
}

//*****************************************************************
// lfs_write - Writes data to a file
//*****************************************************************

void NEW_lfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
  return;
}

//-----------------------------------------------------------------------
//*****************************************************************
// OLD_lfs_read - Reads data from a file
//*****************************************************************
//-----------------------------------------------------------------------

void lfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  char *buf;
  tbuffer_t tbuf;
  ex_iovec_t exv;
  int err;
  ex_off_t ssize, pend;

ex_off_t t1, t2;
  t1 = size; t2 = off;
  log_printf(1, "ino=%lu size=" XOT " off=" XOT "\n", ino, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  lfs = fd->fh->lfs;

  //** Do the read op
  ssize = segment_size(fd->fh->seg);
  pend = off + size;
//log_printf(0, "ssize=" XOT " off=" XOT " len=" XOT " pend=" XOT "\n", ssize, off, size, pend);
  if (pend > ssize) size = ssize - off;  //** Tweak the size based on how much data there is
//log_printf(0, "tweaked len=" XOT "\n", size);
  if (size <= 0) { fuse_reply_buf(req, NULL, 0); return; }
  type_malloc(buf, char, size);
  tbuffer_single(&tbuf, size, buf);
  ex_iovec_single(&exv, off, size);
  err = gop_sync_exec(segment_read(fd->fh->seg, lfs->lc->da, 1, &exv, &tbuf, 0, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR with read! fname=%s\n", fd->fh->inode->entry1->fname);
     free(buf);
     fuse_reply_err(req, EIO);
     return;
  }

  fuse_reply_buf(req, buf, size);

  free(buf);
  return;
}

//-----------------------------------------------------------------------
//*****************************************************************
// OLD_lfs_write - Writes data to a file
//*****************************************************************
//-----------------------------------------------------------------------

void lfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  tbuffer_t tbuf;
  ex_iovec_t exv;
  int err;

ex_off_t t1, t2;
  t1 = size; t2 = off;
  log_printf(1, "ino=%lu size=" XOT " off=" XOT "\n", ino, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  atomic_set(fd->fh->modified, 1);

  lfs = fd->fh->lfs;

  //** Do the write op
  tbuffer_single(&tbuf, size, (char *)buf);
  ex_iovec_single(&exv, off, size);
  err = gop_sync_exec(segment_write(fd->fh->seg, lfs->lc->da, 1, &exv, &tbuf, 0, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     fuse_reply_err(req, EIO);
     return;
  }

  fuse_reply_write(req, size);
  return;
}

//*****************************************************************
// lfs_flush - Flushes any data to backing store
//*****************************************************************

void lfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  int err;

  log_printf(1, "ino=%lu\n", ino); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  lfs = fd->fh->lfs;

  err = gop_sync_exec(segment_flush(fd->fh->seg, lfs->lc->da, 0, segment_size(fd->fh->seg)+1, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     fuse_reply_err(req, EIO);
     return;
  }

  fuse_reply_err(req, 0);
  return;
}

//*****************************************************************
//  lfs_readlink - Reads the object symlink
//*****************************************************************

void lfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
  lio_fuse_t *lfs = lfs_gc;
  lio_inode_t *inode;
  int err;
  char *val;
  int v_size;

  log_printf(15, "ino=%lu\n", ino); flush_log();

  lfs_lock(lfs_gc);

  inode = _lfs_inode_lookup(lfs_gc, ino);
  if (inode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  if ((inode->ftype & OS_OBJECT_SYMLINK) == 0) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EINVAL);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  val = NULL;
  v_size = -lfs->lc->max_attr;
  err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->entry1->fname, NULL, "os.link", (void *)&val, &v_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", inode->entry1->fname);
     fuse_reply_err(req, EIO);
  } else if (v_size > 0) {
     fuse_reply_readlink(req, val);
     free(val);
  } else {
     fuse_reply_err(req, EIO);
  }

  lfs_lock(lfs);
  _lfs_inode_release(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  return;
}


//*****************************************************************
//  lfs_symlink - Makes a symbolic link
//*****************************************************************

void lfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name)
{
  lio_fuse_t *lfs = lfs_gc;
  lio_inode_t *p_inode;
  lio_dentry_t *entry;
  struct fuse_entry_param fentry;
  char fullname[OS_PATH_MAX];
  int err;

  log_printf(1, "parent=%lu name=%s link=%s\n", parent, name, link); flush_log();

  lfs_lock(lfs);

  //** Lookup the parent
  p_inode = _lfs_inode_lookup(lfs, parent);
  if (p_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Make sure the new entry doesn't exist
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", p_inode->entry1->fname, name);
  entry = _lfs_dentry_lookup(lfs, fullname);
  if (entry != NULL) { //** Oops it already exists
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  lfs_unlock(lfs);

  //** Now do the sym link
  err = gop_sync_exec(lio_link_object(lfs->lc, lfs->lc->creds, 1, (char *)link, fullname, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EIO);
     return;
  }

  //** Load the the new entry
  lfs_lock(lfs);
  entry = _lfs_load_inode_entry(lfs, strdup(fullname), NULL);
  if (entry == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EIO);
     return;
  }

  //** Update the ref count
  _lfs_inode_access(lfs_gc, entry->inode, 1, 0);

  //** Form the response
  fentry.ino = entry->inode->ino;
  fentry.generation = 0;
  fentry.attr_timeout = lfs_gc->attr_to;
  fentry.entry_timeout = lfs_gc->entry_to;

  lfs_fill_stat(&(fentry.attr), entry->inode);

  lfs_unlock(lfs);

  fuse_reply_entry(req, &fentry);
  return;
}


//*****************************************************************
//*****************************************************************
//*****************************************************************

#define STUB_PARENT  { log_printf(15, "STUB parent=%lu name=%s\n", parent, name); flush_log(); fuse_reply_err(req, ENOSYS); return; }
#define STUB_INO  { log_printf(15, "STUB ino=%lu\n", ino); flush_log(); fuse_reply_err(req, ENOSYS); return; }


void lfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) STUB_INO
void lfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) STUB_INO
void lfs_statfs(fuse_req_t req, fuse_ino_t ino) STUB_INO
void lfs_access(fuse_req_t req, fuse_ino_t ino, int mask) STUB_INO
void lfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) STUB_PARENT
void lfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) STUB_INO
void lfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep) STUB_INO
void lfs_bmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize, uint64_t idx) STUB_INO
//void lfs_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, void *arg, struct fuse_file_info *fi, unsigned *flagsp, const void *in_buf, size_t in_bufsz, size_t out_bufszp) STUB_INO
void lfs_poll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct fuse_pollhandle *ph) STUB_INO


//*************************************************************************
//  lio_fuse_init - Creates a lowlevel fuse handle for use
//     NOTE:  This returns a generic object but FUSE doesn't support passing
//         a generic context to the FUSE called routines so as a result
//         all the routines use the lfs_gc global context.  The current setup
//         is used in the hopes that the change is made to support this in the future.
//*************************************************************************

lio_fuse_t *lio_fuse_init(lio_config_t *lc)
{
  lio_fuse_t *lfs;
  struct fuse_lowlevel_ops *ll;
  lio_inode_t *inode;
  lio_dentry_t *entry;
  char *section =  "lfs";
  double n;
  int p;

  log_printf(15, "START\n");
  type_malloc_clear(lfs, lio_fuse_t, 1);

  lfs->lc = lc;

  //** Load config info
  lfs->entry_to = inip_get_double(lc->ifd, section, "entry_timout", 1.0);
  lfs->attr_to = inip_get_double(lc->ifd, section, "stat_timout", 1.0);
  lfs->inode_cache_size = inip_get_integer(lc->ifd, section, "inode_cache_size", 1000000);
  lfs->cond_count = inip_get_integer(lc->ifd, section, "cond_size", 100);
  lfs->enable_tape = inip_get_integer(lc->ifd, section, "enable_tape", 0);

  apr_pool_create(&(lfs->mpool), NULL);
  apr_thread_mutex_create(&(lfs->lock), APR_THREAD_MUTEX_DEFAULT, lfs->mpool);

  //** Get the default host ID for opens
  char hostname[1024];
  apr_gethostname(hostname, sizeof(hostname), lfs->mpool);
  lfs->id = strdup(hostname);
//lfs->id = NULL;

  n = lfs->inode_cache_size;
  p = log2(n) + 3;
  lfs->ino_index = create_skiplist_full(p, 0.5, 0, &ino_compare, NULL, NULL, NULL);
  lfs->fname_index = create_skiplist_full(p, 0.5, 0, &list_string_compare, NULL, NULL, NULL);
  lfs->new_inode_list = create_skiplist_full(5, 0.5, 0, &list_string_compare, NULL, NULL, NULL);

  //** Make the cond table
  type_malloc_clear(lfs->inode_cond, apr_thread_cond_t *, lfs->cond_count);
  for (p=0; p<lfs->cond_count; p++) {
     apr_thread_cond_create(&(lfs->inode_cond[p]), lfs->mpool);
  }

  //** Insert the root inode but we need to do some munging
  lfs_lock(lfs);
  entry = _lfs_load_inode_entry(lfs, strdup(""), NULL);
  _lfs_inode_remove(lfs, entry->inode);  //** This removes the inode and all dentries
  entry->inode->entry1 = NULL;
  entry->inode->ino = FUSE_ROOT_ID;
  _lfs_inode_insert(lfs, entry->inode, entry);
  _lfs_inode_access(lfs, entry->inode, 1, 1);  //** Always keep the root inode

inode = _lfs_inode_lookup(lfs, FUSE_ROOT_ID);
printf(" root_ino=%lu\n", inode->ino);
  lfs_unlock(lfs);

  lfs->q = new_opque();
  opque_start_execution(lfs->q);
  lfs->rw_stack = new_stack();
  apr_thread_mutex_create(&(lfs->rw_lock), APR_THREAD_MUTEX_DEFAULT, lfs->mpool);
  apr_thread_create(&(lfs->rw_thread), NULL, lfs_rw_thread, (void *)lfs, lfs->mpool);

  //** Make the fn table
  ll = &(lfs_gc_llops);
  ll->opendir = lfs_opendir;
  ll->releasedir = lfs_closedir;
  ll->readdir = lfs_readdir;
  ll->lookup = lfs_lookup;
  ll->forget = lfs_forget;
  ll->getattr = lfs_stat;
  ll->setattr = lfs_setattr;
  ll->rename = lfs_rename;
  ll->mknod = lfs_mknod;
  ll->mkdir = lfs_mkdir;
  ll->unlink = lfs_unlink;
  ll->rmdir = lfs_rmdir;
  ll->listxattr = lfs_listxattr;
  ll->getxattr = lfs_getxattr;
  ll->setxattr = lfs_setxattr;
  ll->removexattr = lfs_removexattr;
  ll->open = lfs_open;
  ll->release = lfs_release;
  ll->read = lfs_read;
  ll->write = lfs_write;
  ll->flush = lfs_flush;
  ll->link = lfs_hardlink;
  ll->readlink = lfs_readlink;
  ll->symlink = lfs_symlink;

  //stubs

//ll->fsync = lfs_fsync;
//ll->fsyncdir = lfs_fsyncdir;
//ll->statfs = lfs_statfs;
//ll->access = lfs_access;
//ll->create = lfs_create;
//ll->getlk = lfs_getlk;
//ll->setlk = lfs_setlk;
//ll->bmap = lfs_bmap;
//--ll->ioctl = lfs_ioctl;
//ll->poll = lfs_poll;

  lfs->llops = *ll;

  log_printf(15, "END\n");

  return(lfs);
}

//*************************************************************************
// lio_fuse_destroy - Destroy a fuse object
//*************************************************************************

void lio_fuse_destroy(lio_fuse_t *lfs)
{
  list_iter_t it;
  ex_id_t *ino;
  lio_inode_t *inode;
  apr_status_t value;
  int err, i;

  log_printf(15, "shutting down\n"); flush_log();


//  _lfs_inode_release(lfs, inode, 0, 1);  //** Release the root

  //** Just dump the fname_index.  We'll clean things up when dumping the ino_index
  list_destroy(lfs->fname_index);
  list_destroy(lfs->new_inode_list);

  //** Cycle through and destroy all the inodes
  it = list_iter_search(lfs->ino_index, NULL, 0);
  while ((err=list_next(&it, (list_key_t **)&ino, (list_data_t **)&inode)) == 0) {
log_printf(0, "destroying ino=" XIDT "\n", *ino); flush_log();
     lfs_inode_destroy(lfs, inode);
  }
  list_destroy(lfs->ino_index);

  for (i=0; i<lfs->cond_count; i++) {
     apr_thread_cond_destroy(lfs->inode_cond[i]);
  }
  free(lfs->inode_cond);

  //** Shut down the RW thread
  apr_thread_mutex_lock(lfs->rw_lock);
  lfs->shutdown=1;
  apr_thread_mutex_unlock(lfs->rw_lock);

  apr_thread_join(&value, lfs->rw_thread);  //** Wait for it to complete

  //** Clean up everything else
  opque_free(lfs->q, OP_DESTROY);
  free_stack(lfs->rw_stack, 1);
  if (lfs->id != NULL) free(lfs->id);
  apr_thread_mutex_destroy(lfs->lock);
  apr_thread_mutex_destroy(lfs->rw_lock);
  apr_pool_destroy(lfs->mpool);

  free(lfs);
}

