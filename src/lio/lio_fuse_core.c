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


#define lfs_lock(lfs)  apr_thread_mutex_lock((lfs)->lock)
#define lfs_unlock(lfs)  apr_thread_mutex_unlock((lfs)->lock)
#define inode_name(ino) &((ino)->fname[(ino)->name_start])

lio_fuse_t *lfs_gc = NULL;
struct fuse_lowlevel_ops lfs_gc_llops;

int ino_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b);
static skiplist_compare_t ino_compare = {.fn=ino_compare_fn, .arg=NULL };

static int _inode_key_size = 5;
static char *_inode_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type" };

typedef struct {
  lio_fuse_t *lfs;
  os_object_iter_t *it;
  os_regex_table_t *path_regex;
  char *val[5];
  int v_size[5];
  fuse_ino_t root;
  Stack_t *stack;
  lio_inode_t dot_ino;
  lio_inode_t dotdot_ino;
  int state;
} lfs_dir_iter_t;

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
// _lfs_inode_lookup - Looks up an inode. IF fname is non-null.  It is used
//     as the lookup index otherwise the ino is used.
//    NOTE:  Locking should be handled externally!
//*************************************************************************

lio_inode_t * _lfs_inode_lookup(lio_fuse_t *lfs, fuse_ino_t ino, char *fname)
{
  lio_inode_t *inode;

  if (fname != NULL) {
     if (strcmp(fname, "/") == 0) {
       inode = list_search(lfs->fname_index, (list_key_t *)"");
     } else {
       inode = list_search(lfs->fname_index, (list_key_t *)fname);
     }
  } else {
     inode = list_search(lfs->ino_index, (list_key_t *)&ino);
  }

  log_printf(15, "looking up ino=%lu fname=%s inode=%p\n", ino, fname, inode);
  return(inode);
}


//*************************************************************************
// _lfs_inode_insert - Inserts the given file into the inode table and returns
//    the assigned inode.
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_insert(lio_fuse_t *lfs, lio_inode_t *inode)
{
  char *fname = inode->fname;
  int i;

  //** Find where the local file name starts
  i = 0;
  while (fname[i] != 0) {
    if (fname[i] == '/') inode->name_start = i+1;
    i++;
  }

log_printf(15, "fname=%s name_start=%d name=%s ino=" XIDT "\n", inode->fname, inode->name_start, inode_name(inode), inode->ino);

  list_insert(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);
  list_insert(lfs->fname_index, (list_key_t *)inode->fname, (list_data_t *)inode);

  return;;
}

//*************************************************************************
// _lfs_inode_remove- Removes the given inode
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_remove(lio_fuse_t *lfs, lio_inode_t *inode)
{

  log_printf(15, "ino=" XIDT " fname=%s\n", inode->ino, inode->fname);

  list_remove(lfs->fname_index, (list_key_t *)inode->fname, (list_data_t *)inode);
  list_remove(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);

//  free(inode->fname);

  return;
}

//*************************************************************************
// lfs_inode_destroy-  Destroys the given inode.  It doesn't remove it from the list
//*************************************************************************

void lfs_inode_destroy(lio_fuse_t *lfs, lio_inode_t *inode)
{

  free(inode->fname);
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

log_printf(15, "fname=%s fuse=%d lfs=%d delete=%d ino=" XIDT "\n", inode->fname, inode->fuse_count, inode->lfs_count, inode->deleted_object, inode->ino);
  return;
}

//*************************************************************************
// lfs_inode_release - Release the object which may result in it's removal
//*************************************************************************

void _lfs_inode_release(lio_fuse_t *lfs, lio_inode_t *inode, int n_fuse, int n_lfs)
{
  int err;

  inode->fuse_count -= n_fuse;
  inode->lfs_count -= n_lfs;


log_printf(15, "fname=%s fuse=%d lfs=%d delete=%d ino=" XIDT "\n", inode->fname, inode->fuse_count, inode->lfs_count, inode->deleted_object, inode->ino);
if ((inode->lfs_count < 0) || (inode->fuse_count < 0)) {
log_printf(15, "NEGATVIE fname=%s fuse=%d lfs=%d delete=%d ino=" XIDT "\n", inode->fname, inode->fuse_count, inode->lfs_count, inode->deleted_object, inode->ino);
}

  if (inode->lfs_count > 0) return;  //** If FUSE triggers a delete it doesn't always do a forget call

  if (inode->deleted_object == 1) { //** Go ahead and remove it
    _lfs_inode_remove(lfs, inode);  //** Remove it from the table so it's not accidentally accessed again
    lfs_unlock(lfs);  //** Release the global lock while we delete things

log_printf(1, "Removing fname=%s\n", inode->fname);
    //** Now go ahead and remove it
    err = gop_sync_exec(lio_remove_object(lfs->lc, lfs->lc->creds, inode->fname, NULL, inode->ftype));
log_printf(1, "remove err=%d\n", err);
    if (inode->req != NULL) {
       if (err == OP_STATE_SUCCESS) {
          fuse_reply_err(inode->req, 0);
       } else if ((inode->ftype & OS_OBJECT_DIR) > 0) { //** Most likey the dirs not empty
          fuse_reply_err(inode->req, ENOTEMPTY);
       } else {
          fuse_reply_err(inode->req, EBUSY);  //** Otherwise throw a generic error
       }
    }

    if (err == OP_STATE_SUCCESS) {
       lfs_inode_destroy(lfs, inode);
    }

    lfs_lock(lfs);  //** Get it back

    if (err != OP_STATE_SUCCESS) {  //** Add it back in if an error occured
       inode->deleted_object = 0;
       inode->req = NULL;
       _lfs_inode_insert(lfs, inode);
    }
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
  if (ftype & OS_OBJECT_LINK) {
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
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT " fname=%s\n", inode->ino, inode->fname);
     } else {
        log_printf(0, "Missing inode using the old value! ino=" XIDT " fname=%s\n", inode->ino, inode->fname);
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

log_printf(15, "fname=%s data_ts=%s att_ts=%s ino=" XIDT "\n", inode->fname, val[1], val[2], inode->ino);

  if (inode->fh == NULL) {
     inode->size = 0;
     if (val[3] != NULL) sscanf(val[3], XIDT, &(inode->size));
  } else {
     inode->size = segment_size(inode->fh->seg);
  }

  inode->ftype = 0;
  if (val[4] != NULL) sscanf(val[4], "%d", &(inode->ftype));

  for (i=0; i<5; i++) {
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
//  _lfs_load_inode_entry - Loads an Inode entry from the OS
//    NOTE: The curr_inode can be null and a new struc is created and
//          returned.  Otherwise curr_inode is returned.  In this case
//          fname should be freed since it is not used for the updated inode case.
//*************************************************************************

lio_inode_t *_lfs_load_inode_entry(lio_fuse_t *lfs, char *fname, lio_inode_t *curr_inode)
{
  int v_size[_inode_key_size];
  char *val[_inode_key_size];
  char *myfname;
  int i, err, slot, updating;
  ex_id_t start_ino;
  lio_inode_t *inode;

  //** Check if another thread is doing the load if so wait
  inode = NULL;
  updating = 0;
  if (curr_inode != NULL) {
     slot = curr_inode->ino % lfs->cond_count;
     if (curr_inode->pending_update > 0) {  //** Spin until updated
        _lfs_pending_inode_wait(lfs, curr_inode);
        return(curr_inode);  //** Return the updated inode
     }

     curr_inode->pending_update = 1;  //** Let everyone know an update is being performed
     inode = curr_inode;
     updating = 1;
  } else {  //** New inode so put it
     inode = list_search(lfs->new_inode_list, fname); //** Look it up on the new pending inode list
     if (inode == NULL) {  //** I'm the 1st so add it to the list
        type_malloc_clear(inode, lio_inode_t, 1);
        slot = atomic_inc(lfs->counter) % lfs->cond_count;
        inode->fname = fname;
        inode->pending_update = 1;
        list_insert(lfs->new_inode_list, fname, (list_data_t *)inode);
        updating = 2;
     } else {  //** Wait on them to complete
       _lfs_pending_inode_wait(lfs, inode);
       free(fname);
       if (inode->deleted_object == 1) {
          if (inode->pending_count == 0) lfs_inode_destroy(lfs, inode);
          inode = NULL;
       }
       return(inode);
     }
  }

  lfs_unlock(lfs);  //** Release the global lock

  for (i=0; i<5; i++) {
     v_size[i] = -lfs->lc->max_attr;
     val[i] = NULL;
  }

  //** Get the attributes
  myfname = (strcmp(fname, "") == 0) ? "/" : fname;
  err = lioc_get_multiple_attrs(lfs->lc, lfs->lc->creds, myfname, NULL, _inode_keys, (void **)val, v_size, _inode_key_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     inode->deleted_object = 1;
     goto finished;
  }

  start_ino = inode->ino;

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
    list_remove(lfs->new_inode_list, inode->fname, inode);  //** Remove it form the list
    apr_thread_cond_broadcast(lfs->inode_cond[slot]);
  }

  if (err == OP_STATE_SUCCESS) {
    _lfs_inode_insert(lfs, inode);
  } else {
     if (inode->pending_count == 0) lfs_inode_destroy(lfs, inode);
     inode = NULL;
  }

  return(inode);
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
  stat->st_nlink = (inode->ftype & OS_OBJECT_DIR) ? 2 : 1;
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

  inode = _lfs_inode_lookup(lfs_gc, ino, NULL);
  if (inode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  if (apr_time_now() > inode->recheck_time) {  //** Got to reload the info
     _lfs_load_inode_entry(lfs_gc, inode->fname, inode);
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

  inode = _lfs_inode_lookup(lfs_gc, ino, NULL);
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
  struct fuse_entry_param entry;
  lio_inode_t *inode, *pinode;
  char fullname[OS_PATH_MAX];
  char *tmp;

  log_printf(1, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs_gc);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs_gc, parent, NULL);
  if (pinode == NULL) {
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->fname, name, fullname);

  inode = _lfs_inode_lookup(lfs_gc, 0, fullname);
  if (inode == NULL) {  //** Don't have it so need to load it
     tmp = strdup(fullname);
     inode = _lfs_load_inode_entry(lfs_gc, tmp, NULL);
     if (inode == NULL) { //** File doesn't exist!
       log_printf(15, "File doesn't exist! fname=%s\n", fullname);
       lfs_unlock(lfs_gc);
       fuse_reply_err(req, ENOENT);
       return;
     }
  } else if (apr_time_now() > inode->recheck_time) { //** Need to recheck stuff
     _lfs_load_inode_entry(lfs_gc, inode->fname, inode);
  }

  //** Update the ref count
  _lfs_inode_access(lfs_gc, inode, 1, 0);

  //** Form the response
  entry.ino = inode->ino;
  entry.generation = 0;
  entry.attr_timeout = lfs_gc->attr_to;
  entry.entry_timeout = lfs_gc->entry_to;

  lfs_fill_stat(&(entry.attr), inode);

log_printf(1, "fullname=%s ino=" XIDT " ftype=%d\n", inode->fname, inode->ino, inode->ftype);

  lfs_unlock(lfs_gc);

  fuse_reply_entry(req, &entry);
}


//*************************************************************************
// lfs_readdir - Returns the next file in the directory
//*************************************************************************

void  lfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
  int ftype, prefix_len, n, i;
  char *fname, *buf;
  lio_inode_t *inode;
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
    inode = get_ele_data(dit->stack);
    while (inode->deleted_object == 1) {  //** SKip over deleted entries
       move_down(dit->stack);
       inode = get_ele_data(dit->stack);
       if (inode == NULL) break;
    }

    if (inode != NULL) {
       lfs_fill_stat(&stbuf, inode);
       n = fuse_add_direntry(req, NULL, 0, inode_name(inode), NULL, 0);
       type_malloc(buf, char, n);
       fuse_add_direntry(req, buf, n, inode->fname, &stbuf, off+1);
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

  //** If we made it here then grab the next file and look it up.
  ftype = os_next_object(dit->lfs->lc->os, dit->it, &fname, &prefix_len);
  if (ftype <= 0) { //** No more files
    fuse_reply_buf(req, NULL, 0);
    return;
  }

  //** Check if the file already exists
  lfs_lock(dit->lfs);
  inode = _lfs_inode_lookup(dit->lfs, 0, fname);

  if (inode == NULL) { //** Doesn't exist so add it
    type_malloc_clear(inode, lio_inode_t, 1);
    inode->fname = fname;
    _lfs_parse_inode_vals(dit->lfs, inode, dit->val, dit->v_size);
    _lfs_inode_insert(dit->lfs, inode);
  } else {
    for (i=0; i<_inode_key_size; i++) if (dit->val[i] != NULL) free(dit->val[i]);
    free(fname);
  }

log_printf(1, "next fname=%s ftype=%d prefix_len=%d ino=" XIDT " ftype=%d\n", inode->fname, ftype, prefix_len, inode->ino, inode->ftype);

  _lfs_inode_access(dit->lfs, inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk

  lfs_fill_stat(&stbuf, inode);
  n = fuse_add_direntry(req, NULL, 0, inode_name(inode), NULL, 0);
  type_malloc(buf, char, n);
  fuse_add_direntry(req, buf, n, inode_name(inode), &stbuf, off+1);
  fuse_reply_buf(req, buf, n);
  free(buf);
  move_to_bottom(dit->stack);
  insert_below(dit->stack, inode);

  lfs_unlock(dit->lfs);
}


//*************************************************************************
// lfs_opendir - FUSE opendir call
//*************************************************************************

void lfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit;
  lio_inode_t *inode;
  char *dir, *file;
  char path[OS_PATH_MAX];
  int i;

  log_printf(1, "ino=%lu\n", ino); flush_log();

  //** First find the inode
  lfs_lock(lfs_gc);
  inode = _lfs_inode_lookup(lfs_gc, ino, NULL);

  if (inode == NULL) {  //** Can't find the inode so kick out
     lfs_unlock(lfs_gc);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** If we made it here we have a valid inode
  type_malloc_clear(dit, lfs_dir_iter_t, 1);

  for (i=0; i<_inode_key_size; i++) {
    dit->v_size[i] = -lfs_gc->lc->max_attr;
    dit->val[i] = NULL;
  }

  dit->lfs = lfs_gc;
  snprintf(path, OS_PATH_MAX, "%s/*", inode->fname);
  dit->path_regex = os_path_glob2regex(path);

  dit->it = os_create_object_iter_alist(dit->lfs->lc->os, dit->lfs->lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY, 0, _inode_keys, (void **)dit->val, dit->v_size, _inode_key_size);

  dit->stack = new_stack();

  dit->dot_ino.fname = ".";
  dit->dot_ino.name_start = 0;
  dit->dot_ino.ino = inode->ino;
  _lfs_inode_access(lfs_gc, inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk

  dit->dot_ino.ftype = OS_OBJECT_DIR;

  dit->dotdot_ino.fname = "..";
  dit->dotdot_ino.name_start = 0;
  dit->dot_ino.ftype = OS_OBJECT_DIR;
  if (inode->ftype ^ OS_OBJECT_DIR) {
     dit->dotdot_ino.ino = inode->ino;
  } else {
    if (strcmp(inode->fname, "") == 0) {
       inode = _lfs_inode_lookup(lfs_gc, FUSE_ROOT_ID, NULL);
    } else {
       os_path_split(inode->fname, &dir, &file);
       inode = _lfs_inode_lookup(lfs_gc, 0, dir);
       if (inode == NULL) {  //** Got to load it
          inode = _lfs_load_inode_entry(lfs_gc, dir, NULL);
       } else {
         free(dir);
       }
       free(file);
    }
    dit->dotdot_ino.ino = inode->ino;
  }

  _lfs_inode_access(lfs_gc, inode, 0, 1);  //** Make sure it's not accidentally deleted during the walk
  insert_below(dit->stack, &dit->dot_ino);
  insert_below(dit->stack, &dit->dotdot_ino);

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
  lio_inode_t *inode;

  log_printf(1, "ino=%lu\n", ino); flush_log();

  if (dit == NULL) {
     fuse_reply_err(req, EBADF);
     return;
  }

  //** Cyle through releasing all the inodes
  lfs_lock(dit->lfs);
  while ((inode = (lio_inode_t *)pop(dit->stack)) != NULL) {
     if ((strcmp(inode->fname, ".") == 0) || (strcmp(inode->fname, "..") == 0)) {
        inode = _lfs_inode_lookup(dit->lfs, inode->ino, NULL);  //** Resolve . and .. correctly
     }
     _lfs_inode_release(dit->lfs, inode, 0, 1);
  }
  lfs_unlock(dit->lfs);

  free_stack(dit->stack, 0);

  os_destroy_object_iter(dit->lfs->lc->os, dit->it);
  os_regex_table_destroy(dit->path_regex);
  free(dit);

  fuse_reply_err(req, 0);
  return;
}

//*************************************************************************
// lfs_object_create
//*************************************************************************

int lfs_object_create(lio_fuse_t *lfs, fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, int ftype)
{
  struct fuse_entry_param entry;
  lio_inode_t *inode, *pinode;
  char fullname[OS_PATH_MAX];
  char *tmp;
  int err;

  log_printf(1, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs, parent, NULL);
  if (pinode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return(1);
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->fname, name, fullname);

  inode = _lfs_inode_lookup(lfs, 0, fullname);
  if (inode != NULL) { //** Oops it already exists
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return(1);
  }

  tmp = strdup(fullname);
log_printf(15, "tmp=%s\n", tmp);
  inode = _lfs_load_inode_entry(lfs, tmp, NULL);
  if (inode != NULL) { //** File already exist!
     log_printf(15, "File already exist! fname=%s\n", fullname);
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     free(tmp);
     return(1);
   }

  //** If we made it here it's a new file or dir
  //** Create the new object
  tmp = strdup(fullname);  //** _lfs_load_inode_entry will remove this on fialure automatically
  err = gop_sync_exec(lio_create_object(lfs->lc, lfs->lc->creds, tmp, ftype, NULL, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Error creating object! fname=%s\n", fullname);
     lfs_unlock(lfs);
     fuse_reply_err(req, EREMOTEIO);
     return(1);
  }

  //** Load the inode
  inode = _lfs_load_inode_entry(lfs, tmp, NULL);
  if (inode == NULL) { //** File doesn't exist!
     log_printf(15, "File doesn't exist! fname=%s\n", fullname);
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return(1);
  }

  //** Form the response
  entry.ino = inode->ino;
  entry.generation = 0;
  entry.attr_timeout = lfs_gc->attr_to;
  entry.entry_timeout = lfs_gc->entry_to;

  lfs_fill_stat(&(entry.attr), inode);

log_printf(15, "fullname=%s ino=" XIDT " ftype=%d\n", inode->fname, inode->ino, inode->ftype);

  lfs_unlock(lfs);

  fuse_reply_entry(req, &entry);

  return(0);
}

//*************************************************************************
// lfs_rename - Renames a file
//*************************************************************************

void lfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname)
{
  lio_fuse_t *lfs = lfs_gc;
  lio_inode_t *sd_inode, *dd_inode, *inode, *dinode;
  char fullname[OS_PATH_MAX];
  int err;

  log_printf(1, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs);

  //** Lookup the parents
  sd_inode = _lfs_inode_lookup(lfs, parent, NULL);
  if (sd_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  dd_inode = _lfs_inode_lookup(lfs, newparent, NULL);
  if (dd_inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", sd_inode->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", sd_inode->fname, name, fullname);

  inode = _lfs_inode_lookup(lfs, 0, fullname);
  if (inode == NULL) { //** Oops it doesn't exist
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  //** Do the same for the dest
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", dd_inode->fname, newname);
  dinode = _lfs_inode_lookup(lfs, 0, fullname);
  if (dinode != NULL) { //** Oops it already exists and we don;t currently support this scenario
     lfs_unlock(lfs);
     fuse_reply_err(req, EEXIST);
     return;
  }

  //** Now do the move
  err = gop_sync_exec(lio_move_object(lfs->lc, lfs->lc->creds, inode->fname, fullname));
  if (err != OP_STATE_SUCCESS) {
     lfs_unlock(lfs);
     fuse_reply_err(req, EIO);
     return;
  }

  //** Update the inode name
  free(inode->fname);
  inode->fname = strdup(fullname);

  lfs_unlock(lfs);

  fuse_reply_err(req, 0);
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
  char fullname[OS_PATH_MAX];
  int defer_reply;

  log_printf(15, "parent_ino=%lu name=%s\n", parent, name); flush_log();

  lfs_lock(lfs);

  //** Lookup the parent
  pinode = _lfs_inode_lookup(lfs, parent, NULL);
  if (pinode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  //** Now see if we have the child
  snprintf(fullname, OS_PATH_MAX-1, "%s/%s", pinode->fname, name);
log_printf(15, "parent=%s name=%s fullname=%s\n", pinode->fname, name, fullname);

  inode = _lfs_inode_lookup(lfs, 0, fullname);
  if (inode == NULL) { //** Oops it doesn't exist
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  defer_reply = inode->ftype & OS_OBJECT_DIR;

  if (defer_reply > 0) inode->req = req;

  inode->deleted_object = 1;  //** Mark it for deletion

  _lfs_inode_release(lfs, inode, 0, 0);  //** Release it which should trigger the removal

  lfs_unlock(lfs);

  //** Reply to the request immediately
  if (defer_reply == 0) fuse_reply_err(req, 0);
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
// lfs_removexattr - Removes an extended attribute
//*****************************************************************

void lfs_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  lio_inode_t *inode;
//  char *val;
  int v_size, err;
  lio_fuse_t *lfs = lfs_gc;

  log_printf(1, "ino=%lu attr_name=%s\n", ino, name); flush_log();

  //** Lookup the inode
  lfs_lock(lfs);
  inode = _lfs_inode_lookup(lfs, ino, NULL);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  v_size = -1;
//  val = NULL;
  err = lioc_set_attr(lfs->lc, lfs->lc->creds, inode->fname, NULL, (char *)name, NULL, v_size);
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
  inode = _lfs_inode_lookup(lfs, ino, NULL);
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
     err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->fname, NULL, (char *)name, (void **)&val, &v_size);
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
  err = lioc_set_attr(lfs->lc, lfs->lc->creds, inode->fname, NULL, (char *)name, (void *)fval, v_size);
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
  inode = _lfs_inode_lookup(lfs, ino, NULL);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

//  v_size = -size;
  v_size = (size == 0) ? -lfs->lc->max_attr : -size;
  val = NULL;
  err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->fname, NULL, (char *)name, (void **)&val, &v_size);
  if (err != OP_STATE_SUCCESS) {
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  if (v_size < 0) v_size = 0;  //** No attribute

  if (size == 0) {
log_printf(15, "SIZE bpos=%d buf=%s\n", v_size, val);
    fuse_reply_xattr(req, v_size);
  } else if (size > v_size) {
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
  char *buf, *key, *val, *fname;
  int bpos, bufsize, v_size, n, i, prefix_len, ftype;
  os_regex_table_t *path_regex, *attr_regex;
  os_object_iter_t *it;
  os_attr_iter_t *ait;
  lio_fuse_t *lfs = lfs_gc;

  bpos= size;
  log_printf(1, "ino=%lu size=%d\n", ino, bpos); flush_log();

  lfs_lock(lfs);

  //** Lookup the inode
  inode = _lfs_inode_lookup(lfs, ino, NULL);
  if (inode == NULL) {
     lfs_unlock(lfs);
     fuse_reply_err(req, ENOENT);
     return;
  }

  _lfs_inode_access(lfs, inode, 0, 1);
  lfs_unlock(lfs);

  //** Make an iterator
  path_regex = os_path_glob2regex(inode->fname);
  attr_regex = os_path_glob2regex("user.*");

  v_size = 0;
  it = os_create_object_iter(lfs->lc->os, lfs->lc->creds, path_regex, NULL, OS_OBJECT_ANY, attr_regex, 0, &ait, v_size);
  if (it == NULL) {
     log_printf(15, "ERROR creating iterator for fname=%s\n", inode->fname);
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  //** Get the 1st and only object
  ftype = os_next_object(lfs->lc->os, it, &fname, &prefix_len);
  if (ftype == 0) {
     log_printf(15, "ERROR getting next object fname=%s\n", inode->fname);
     fuse_reply_err(req, ENOENT);
     goto finished;
  }

  //** Cycle through the keys
  bufsize = 10*1024;
  type_malloc_clear(buf, char, bufsize);
  val = NULL;
  bpos = 0;
  while (os_next_attr(lfs->lc->os, ait, &key, (void **)&val, &v_size) == 0) {
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

  free(fname);
  os_destroy_object_iter(lfs->lc->os, it);
  os_regex_table_destroy(path_regex);
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

  log_printf(15, "loading exnode in=" XIDT " fname=%s\n", inode->ino, inode->fname);

  //** Get the exnode
  v_size = -lfs->lc->max_attr;
  err = lioc_get_attr(lfs->lc, lfs->lc->creds, inode->fname, NULL, "system.exnode", (void **)&ex_data, &v_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving exnode! path=%s\n", inode->fname);
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
     log_printf(0, "No default segment!  Aborting! fname=%s\n", inode->fname);
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
  inode = _lfs_inode_lookup(lfs, ino, NULL);
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

  log_printf(1, "ino=%lu\n", ino); flush_log();

  err = lfs_myopen(lfs_gc, ino, fi->flags, &fd);

  if (err == 0) {
     fi->fh = (uint64_t)fd;
     fuse_reply_open(req, fi);
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
  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, fh->inode->fname, NULL, key, (void **)val, v_size, 3);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating exnode! fname=%s\n", fh->inode->fname);
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
  inode = _lfs_inode_lookup(lfs, ino, NULL);
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
     if (err != OP_STATE_SUCCESS) {
        fuse_reply_err(req, EBADE);
        lfs_lock(lfs);
        atomic_set(fd->fh->modified, 1);
        _lfs_inode_release(lfs, inode, 0, 1);
        lfs_unlock(lfs);
        return;
     }

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
    key[n] = "system.os.timestamp.modify_attr";
    val[n] = lfs->id;
    v_size[n] = strlen(val[n]);
    n++;
  }

  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, inode->fname, NULL, key, (void **)val, v_size, n);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating stat! fname=%s\n", inode->fname);
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
// lfs_read - Reads data from a file
//*****************************************************************

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
  type_malloc(buf, char, size);
  ssize = segment_size(fd->fh->seg);
  pend = off + size;
  if (pend > ssize) size = ssize - off;  //** Tweak the size based on how much data there is
  tbuffer_single(&tbuf, size, buf);
  ex_iovec_single(&exv, off, size);
  err = gop_sync_exec(segment_read(fd->fh->seg, lfs->lc->da, 1, &exv, &tbuf, 0, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     free(buf);
     fuse_reply_err(req, EIO);
     return;
  }

  fuse_reply_buf(req, buf, size);

  free(buf);
  return;
}

//*****************************************************************
// lfs_write - Writes data to a file
//*****************************************************************

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
//*****************************************************************
//*****************************************************************

#define STUB_PARENT  { log_printf(15, "STUB parent=%lu name=%s\n", parent, name); flush_log(); fuse_reply_err(req, ENOSYS); return; }
#define STUB_INO  { log_printf(15, "STUB ino=%lu\n", ino); flush_log(); fuse_reply_err(req, ENOSYS); return; }


void lfs_readlink(fuse_req_t req, fuse_ino_t ino) STUB_INO
void lfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name) STUB_PARENT
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
  inode = _lfs_load_inode_entry(lfs, strdup(""), NULL);
  _lfs_inode_remove(lfs, inode);
  inode->ino = FUSE_ROOT_ID;
  _lfs_inode_insert(lfs, inode);
  _lfs_inode_access(lfs, inode, 1, 0);  //** Always keep the root inode

inode = _lfs_inode_lookup(lfs, 0, "");
printf(" root_ino=%lu\n", inode->ino);
  lfs_unlock(lfs);

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

  //stubs

//ll->readlink = lfs_readlink;
//ll->symlink = lfs_symlink;
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
  int err, i;

  log_printf(15, "shutting down\n"); flush_log();


//  _lfs_inode_release(lfs, inode, 0, 1);  //** Release the root

  //** Just dump the fname_index.  We'll clean things up when dumping the ino_index
  list_destroy(lfs->fname_index);
  list_destroy(lfs->new_inode_list);

  //** Cycle through and destroy all the inodes
  it = list_iter_search(lfs->ino_index, NULL, 0);
  while ((err=list_next(&it, (list_key_t **)&ino, (list_data_t **)&inode)) == 0) {
     if (inode->fname != NULL) free(inode->fname);
     free(inode);
  }
  list_destroy(lfs->ino_index);

  for (i=0; i<lfs->cond_count; i++) {
     apr_thread_cond_destroy(lfs->inode_cond[i]);
  }
  free(lfs->inode_cond);

  //** Clean up everything else
  if (lfs->id != NULL) free(lfs->id);
  apr_thread_mutex_destroy(lfs->lock);
  apr_pool_destroy(lfs->mpool);

  free(lfs);
}

