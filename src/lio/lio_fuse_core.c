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
#include "apr_wrapper.h"

//#define lfs_lock(lfs)  log_printf(0, "lfs_lock\n"); flush_log(); apr_thread_mutex_lock((lfs)->lock)
//#define lfs_unlock(lfs) log_printf(0, "lfs_unlock\n");  flush_log(); apr_thread_mutex_unlock((lfs)->lock)
#define lfs_lock(lfs)    apr_thread_mutex_lock((lfs)->lock)
#define lfs_unlock(lfs)  apr_thread_mutex_unlock((lfs)->lock)

#define dentry_name(entry) &((entry)->fname[(entry)->name_start])

int ino_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b);
static skiplist_compare_t ino_compare = {.fn=ino_compare_fn, .arg=NULL };

#define _inode_key_size 11
#define _inode_fuse_attr_start 7
static char *_inode_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count", "os.link",
                               "security.selinux",  "system.posix_acl_access", "system.posix_acl_default", "security.capability"};

#define _tape_key_size  2
static char *_tape_keys[] = { "system.owner", "system.exnode" };

typedef struct {
  lio_fuse_t *lfs;
  os_object_iter_t *it;
  os_regex_table_t *path_regex;
  char *val[_inode_key_size];
  int v_size[_inode_key_size];
  char *dot_path;
  char *dotdot_path;
  Stack_t *stack;
  int state;
} lfs_dir_iter_t;

//*************************************************************************
//  ino_compare_fn  - FUSE inode comparison function
//*************************************************************************

int ino_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b)
{
  ex_id_t *al = (ex_id_t *)a;
  ex_id_t *bl = (ex_id_t *)b;

//log_printf(15, "a=" XIDT " b=" XIDT "\n", *al, *bl);
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

lio_inode_t * _lfs_inode_lookup(lio_fuse_t *lfs, ex_id_t ino)
{
  lio_inode_t *inode;

  inode = list_search(lfs->ino_index, (list_key_t *)&ino);

  log_printf(15, "looking up ino=%lu inode=%p\n", ino, inode);
  return(inode);
}

//*************************************************************************
// _lfs_inode_insert - Inserts the given file into the inode table and returns
//    the assigned inode.
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_insert(lio_fuse_t *lfs, lio_inode_t *inode)
{

  log_printf(15, "inserting ino=" XIDT "\n", inode->ino);
  list_insert(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);

  return;
}

//*************************************************************************
// _lfs_inode_remove- Removes the given inode so it can't be looked up
//    NOTE:  Locking should be handled externally!
//*************************************************************************

void _lfs_inode_remove(lio_fuse_t *lfs, lio_inode_t *inode)
{
  list_iter_t it;
  Stack_t *stack;
  char aname[512];
  char *key;
  lio_attr_t *attr;
  int n;

  log_printf(15, "ino=" XIDT "\n", inode->ino);

  //** Remove it form the ino index
  list_remove(lfs->ino_index, (list_key_t *)&(inode->ino), (list_data_t *)inode);

  //** Also remove all the attributes
  stack = new_stack();
  n = snprintf(aname, sizeof(aname), XIDT ":", inode->ino);
  it = list_iter_search(lfs->attr_index, aname, 0);
  while (list_next(&it, (list_key_t **)&key, (list_data_t **)&attr) == 0) {
log_printf(15, "checking... aname=%s key=%s\n", aname, key);
     if (strncmp(aname, key, n) != 0) break;
     push(stack, attr);
     push(stack, key);
  }
  while ((key =  pop(stack)) != NULL) {
     attr = pop(stack);
log_printf(15, "Dropping key=%s\n", key);
     list_remove(lfs->attr_index, key, attr);
  }

  free_stack(stack, 1);

  return;
}

//*************************************************************************
// lfs_inode_destroy-  Destroys the given inode.  It doesn't remove it from the list
//*************************************************************************

void lfs_inode_destroy(lio_fuse_t *lfs, lio_inode_t *inode)
{
  if (inode->link != NULL) free(inode->link);
  free(inode);
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
     mode = S_IFLNK | 0777;
  } else if (ftype & OS_OBJECT_DIR) {
     mode = S_IFDIR | 0755;
  } else {
     mode = S_IFREG | 0444;
  }

  return(mode);
}

//*************************************************************************
// lfs_fill_stat - Fills a stat structure with the inode info
//*************************************************************************

void lfs_fill_stat(struct stat *stat, lio_inode_t *inode)
{

  memset(stat, 0, sizeof(struct stat));
  stat->st_ino = inode->ino;
  if (inode->fh != NULL) inode->size = segment_size(inode->fh->seg);
  stat->st_size = (inode->ftype & OS_OBJECT_SYMLINK) ? strlen(inode->link) : inode->size;
  stat->st_blksize = 4096;
  stat->st_blocks = stat->st_size / 512;
  stat->st_mode = ftype_lio2fuse(inode->ftype);
  stat->st_nlink = inode->nlinks;
  stat->st_mtime = inode->modify_data_ts;
  stat->st_ctime = inode->modify_attr_ts;
  stat->st_atime = stat->st_ctime;

//  if (inode->ftype & OS_OBJECT_SYMLINK) stat->st_blksize = 1024;
   if (stat->st_size < 1024) stat->st_blksize = 1024;
}


//*************************************************************************
// _lfs_parse_inode_vals - Parses the inode values received
//*************************************************************************

void _lfs_parse_inode_vals(lio_fuse_t *lfs, lio_inode_t *inode, char **val, int *v_size)
{
  int i;
  char *link;
  lio_attr_t *attr;
  char aname[512];

  if (val[0] != NULL) {
     inode->ino = 0; sscanf(val[0], XIDT, &(inode->ino));
  } else {
     if (inode->ino == 0) {
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

  inode->link = val[6];  //** Don't want to free this value below
  val[6] = NULL;
  if (inode->link != NULL) {
     if (inode->link[0] == '/') { //** IF an absolute link then we need to add the mount prefix back
        i = strlen(inode->link) + lfs->mount_point_len + 1;
        type_malloc(link, char, i);
        snprintf(link, i, "%s%s", lfs->mount_point, inode->link);
        free(inode->link);
        inode->link = link;
     }
  }

  //** Handle all the various security ACLs that Linux likes to check
  for (i=_inode_fuse_attr_start; i<_inode_key_size; i++) {
     snprintf(aname, sizeof(aname), XIDT ":%s", inode->ino, _inode_keys[i]);
     attr = list_search(lfs->attr_index, aname);
     if (attr != NULL) { //** Already exists so just update the info
log_printf(15, "UPDATING aname=%s\n", aname);
        if (attr->val != NULL) free (attr->val);
        if (v_size[i] > 0) {
           type_malloc(attr->val, char, v_size[i]);
           memcpy(attr->val, val[i], v_size[i]);
           attr->v_size = v_size[i];
        } else {
           attr->v_size = 0;
           attr->val = NULL;
        }
     } else {   //** New entry so insert it
        type_malloc_clear(attr, lio_attr_t, 1);
        if (v_size[i] > 0) {
           type_malloc(attr->val, char, v_size[i]);
           memcpy(attr->val, val[i], v_size[i]);
           attr->v_size = v_size[i];
        } else {
           attr->v_size = 0;
           attr->val = NULL;
        }

log_printf(15, "ADDING aname=%s p=%p v_size=%d\n", aname, attr, attr->v_size);
        list_insert(lfs->attr_index, strdup(aname), attr);
     }

     attr->recheck_time = apr_time_now() + lfs->xattr_to;
  }

  //** Clean up
  for (i=0; i<_inode_key_size; i++) {
     if (val[i] != NULL) free(val[i]);
  }

  //** Update the recheck time
  inode->recheck_time = apr_time_now() + lfs->attr_to;
}


//*************************************************************************
//  _lfs_load_inode_entry - Loads an Inode and dentry from the OS
//    NOTE: The curr_inode can be null and a new struct is created and
//          the new inode is returned.
//*************************************************************************

lio_inode_t *_lfs_load_inode_entry(lio_fuse_t *lfs, const char *fname, lio_inode_t *curr_inode)
{
  int v_size[_inode_key_size];
  char *val[_inode_key_size];
  char *myfname;
  lio_fuse_file_handle_t *fh;
  int i, err;
//  ex_id_t start_ino;
  lio_inode_t inode;
  lio_inode_t *tinode;

  //** If the file is open we don't do an update
  if (curr_inode != NULL) {
     if (curr_inode->fh != NULL) return(curr_inode);
  }

  err = OP_STATE_SUCCESS;

  for (i=0; i<_inode_key_size; i++) {
     v_size[i] = -lfs->lc->max_attr;
     val[i] = NULL;
  }

  lfs_unlock(lfs);  //** Do the OS query without the lock

  //** Get the attributes
  myfname = (strcmp(fname, "") == 0) ? "/" : (char *)fname;
  err = lioc_get_multiple_attrs(lfs->lc, lfs->lc->creds, myfname, NULL, _inode_keys, (void **)val, v_size, _inode_key_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     for (i=0; i<_inode_key_size; i++) if (val[i] != NULL) free(val[i]);
     lfs_lock(lfs);
     return(NULL);
  }

  log_printf(15, "Parsing info for fname=%s\n", myfname);
  memset(&inode, 0, sizeof(inode));

  //** Now try and insert it
  lfs_lock(lfs);  //** Reacquire the lock

  _lfs_parse_inode_vals(lfs, &inode, val, v_size);  //** Need the lock cause the attr table is updated

//  if (start_ino == 1) inode.ino = 1;


  tinode = _lfs_inode_lookup(lfs, inode.ino);
  if (tinode == NULL) { //** Doesn't exist so insert it
log_printf(1, "inserting info for fname=%s\n", fname);
     type_malloc_clear(tinode, lio_inode_t, 1);
     *tinode = inode;
     _lfs_inode_insert(lfs, tinode);
  } else {  //** Just update the contents
log_printf(1, "updating info for fname=%s\n", fname);
     fh = tinode->fh;
     err = tinode->flagged;
     if (tinode->link != NULL) free(tinode->link);
     *tinode = inode;
     tinode->fh = fh;
     tinode->flagged = err;
  }


  return(tinode);
}


//*************************************************************************
// _lfs_dentry_insert - Adds a dentry to the file table
//*************************************************************************

int _lfs_dentry_insert(lio_fuse_t *lfs, lio_dentry_t *entry)
{
  int i;
  char *fname;

  //** Find where the local file name starts
  fname = entry->fname;
  i = 0;
  while (fname[i] != 0) {
    if (fname[i] == '/') entry->name_start = i+1;
    i++;
  }

  entry->recheck_time = apr_time_now() + lfs->entry_to;

log_printf(1, "fname=%s name_start=%d name=%s\n", entry->fname, entry->name_start, dentry_name(entry));
  list_insert(lfs->fname_index, (list_key_t *)entry->fname, (list_data_t *)entry);

  return(0);
}

//*************************************************************************
// _lfs_dentry_remove - Removes the fname's dentry
//*************************************************************************

int _lfs_dentry_remove(lio_fuse_t *lfs, lio_dentry_t *entry)
{
log_printf(1, "fname=%s\n", entry->fname);
  list_remove(lfs->fname_index, (list_key_t *)entry->fname, (list_data_t *)entry);
  return(0);
}

//*************************************************************************
// lfs_dentry_destroy - Destroys dentry
//*************************************************************************

void lfs_dentry_destroy(lio_fuse_t *lfs, lio_dentry_t *entry)
{
  free(entry->fname);
  free(entry);
}

//*************************************************************************
// _lfs_dentry_get - Returns the dentry
//*************************************************************************

lio_dentry_t * _lfs_dentry_get(lio_fuse_t *lfs, const char *fname)
{
  lio_dentry_t *entry = NULL;

  entry = list_search(lfs->fname_index, (list_key_t *)fname);

  return(entry);
}

//*************************************************************************
// _lfs_dentry_lookup - Returns the files inode using both the dentry and inode caches
//*************************************************************************

lio_inode_t * _lfs_dentry_lookup(lio_fuse_t *lfs, const char *fname, int auto_insert)
{
  lio_dentry_t *entry = NULL;
  lio_inode_t *inode = NULL;

  entry = _lfs_dentry_get(lfs, fname);

  if (entry != NULL) {
     inode = _lfs_inode_lookup(lfs, entry->ino);
  }

  log_printf(1, "looking up fname=%s INITIAL entry=%p inode=%p\n", fname, entry, inode);

  if ((auto_insert == 1) && (inode == NULL)) {  //** Go ahead and insert it and the inode
     inode = _lfs_load_inode_entry(lfs, fname, NULL);
     if (inode == NULL) {
        log_printf(15, "FAILED looking up fname=%s\n", fname);
        return(NULL);
     }

     //** Check if someone else beat us to it during the loading phase
     entry = _lfs_dentry_get(lfs, fname);
     if (entry == NULL) {
        type_malloc_clear(entry, lio_dentry_t, 1);
        entry->fname = strdup(fname);
        entry->ino = inode->ino;
        _lfs_dentry_insert(lfs, entry);
     } else if (entry->ino != inode->ino) {
log_printf(1, "fname=%s inode changed old=" XOT " new=" XOT "\n", fname, entry->ino, inode->ino);
        entry->ino = inode->ino;
     }
  }

  log_printf(1, "looking up fname=%s FINAL entry=%p inode=%p\n", fname, entry, inode);
  return(inode);
}

//*************************************************************************
// lfs_stat - Does a stat on the file/dir
//            Note: lfs_stat_real does the lifting, lfs_stat just handles
//                  getting the lfs handle from the fuse contex. Splitting
//                  it up this way makes it so someone can use this without
//                  FUSE
//*************************************************************************

int lfs_stat_real(const char *fname,
                  struct stat *stat,
                  lio_fuse_t *lfs)
{
  lio_inode_t *inode;
  lio_dentry_t *entry;
  /*
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  */
  log_printf(1, "fname=%s\n", fname); flush_log();

  lfs_lock(lfs);

  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  entry = _lfs_dentry_get(lfs, fname);
  if (entry == NULL) log_printf(0, "ERROR_DENTRY:  fname=%s inode=%p\n", fname, inode);
  if (entry->flagged == LFS_INODE_DELETE) {
     log_printf(1, "fname=%s flagged for removal\n", fname);
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  if (apr_time_now() > entry->recheck_time) { //** Update the entry timeout as well
      entry->recheck_time = apr_time_now() + lfs->attr_to;
  }

  if (apr_time_now() > inode->recheck_time) {  //** Update the inode
     //** Update the recheck time to minimaze the GC from removing.  It *will* still happen though
     inode->recheck_time = apr_time_now() + 100*APR_USEC_PER_SEC;  //** This gets set properly in when loading
     inode = _lfs_load_inode_entry(lfs, fname, inode);

     if (inode == NULL) {  //** Remove the old dentry
        entry = _lfs_dentry_get(lfs, fname);
        if (entry != NULL) {
           _lfs_dentry_remove(lfs, entry);
           lfs_dentry_destroy(lfs, entry);
        }

        lfs_unlock(lfs);
        log_printf(1, "fname=%s missing from backend\n", fname);
        return(-ENOENT);
     }
  }

  lfs_fill_stat(stat, inode);
  lfs_unlock(lfs);
  return(0);
}
int lfs_stat(const char *fname,
                  struct stat *stat)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid\n");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_stat_real(fname, stat, lfs);
}

//*************************************************************************
// lfs_opendir - FUSE opendir call
//*************************************************************************

int lfs_opendir(const char *fname, struct fuse_file_info *fi) {
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_opendir_real(fname, fi, lfs);

}
int lfs_opendir_real(const char *fname, struct fuse_file_info *fi, lio_fuse_t *lfs)
{
  lfs_dir_iter_t *dit;
  char path[OS_PATH_MAX];
  char *dir, *file;
  lio_dentry_t *e2;
  lio_inode_t *inode;
  int i;
  log_printf(1, "fname=%s\n", fname); flush_log();

  type_malloc_clear(dit, lfs_dir_iter_t, 1);

  for (i=0; i<_inode_key_size; i++) {
    dit->v_size[i] = -lfs->lc->max_attr;
    dit->val[i] = NULL;
  }

  dit->lfs = lfs;
  snprintf(path, OS_PATH_MAX, "%s/*", fname);
  dit->path_regex = os_path_glob2regex(path);

  dit->it = os_create_object_iter_alist(dit->lfs->lc->os, dit->lfs->lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY, 0, _inode_keys, (void **)dit->val, dit->v_size, _inode_key_size);

  dit->stack = new_stack();

  dit->state = 0;

  lfs_lock(dit->lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     log_printf(0, "ERROR with lookup of fname=%s\n", fname);
     lfs_unlock(dit->lfs);
     return(-ENOENT);
  }

  //** Add "."
  dit->dot_path = strdup(fname);
  type_malloc_clear(e2, lio_dentry_t, 1);
  e2->ino = inode->ino;
  e2->fname = strdup(".");
  e2->name_start = 0;
  insert_below(dit->stack, e2);

  //** And ".."
  if (strcmp(fname, "/") != 0) {
     os_path_split((char *)fname, &dir, &file);
     inode = _lfs_dentry_lookup(lfs, dir, 1);
     dit->dotdot_path = dir;
     free(file);
  } else {
     dit->dotdot_path = strdup(fname);
  }

  log_printf(1, "dot=%s dotdot=%s\n", dit->dot_path, dit->dotdot_path);

  type_malloc_clear(e2, lio_dentry_t, 1);
  e2->ino = inode->ino;
  e2->fname = strdup("..");
  e2->name_start = 0;
  insert_below(dit->stack, e2);

  lfs_unlock(dit->lfs);

  //** Compose our reply
  fi->fh = (uint64_t)dit;
  return(0);
}

//*************************************************************************
// lfs_readdir - Returns the next file in the directory
//*************************************************************************

int lfs_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi) {

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_readdir_real(dname, buf, filler, off, fi, lfs);

}
int lfs_readdir_real(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, lio_fuse_t *lfs)
{
  lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
  lio_dentry_t *entry, *fentry;
  lio_inode_t *inode;
  int ftype, prefix_len, n, i;
  char *fname;
  struct stat stbuf;
  apr_time_t now;
  double dt;
  int off2 = off;
  log_printf(1, "dname=%s off=%d stack_size=%d\n", dname, off2, stack_size(dit->stack)); flush_log();
  now = apr_time_now();

  if (dit == NULL) {
     return(-EBADF);
  }

  off++;  //** This is the *next* slot to get where the stack top is off=1

  memset(&stbuf, 0, sizeof(stbuf));
  n = stack_size(dit->stack);
  if (n>=off) { //** Rewind
    move_to_bottom(dit->stack);  //** Go from the bottom up.
    for (i=n; i>off; i--) move_up(dit->stack);

    lfs_lock(dit->lfs);
    entry = get_ele_data(dit->stack);
    while (entry != NULL) {
       if (off > 2) {
          inode = _lfs_dentry_lookup(dit->lfs, entry->fname, 1);
       } else if (off == 1) {
          inode = _lfs_dentry_lookup(dit->lfs, dit->dot_path, 1);
       } else if (off == 2) {
          inode = _lfs_dentry_lookup(dit->lfs, dit->dotdot_path, 1);
       } else {
          inode= NULL;
       }

       if (inode == NULL) {
          log_printf(0, "Missing inode!  fname=%s ino=" XIDT " off=%d\n", entry->fname, entry->ino, off);
          lfs_unlock(dit->lfs);
          return(-ENOENT);
       }

off2=off;
log_printf(15, "inserting fname=%s off=%d\n", entry->fname, off2);
       if (entry->flagged != LFS_INODE_DELETE) {
          lfs_fill_stat(&stbuf, inode);
          if (filler(buf, dentry_name(entry), &stbuf, off) == 1) {
             dt = apr_time_now() - now;
             dt /= APR_USEC_PER_SEC;
             log_printf(1, "dt=%lf\n", dt);
             return(0);
          }
       } else {
          log_printf(1, "fname=%s flagged for removal\n", entry->fname);
       }

       off++;
       move_down(dit->stack);
       entry = get_ele_data(dit->stack);
    }
    lfs_unlock(dit->lfs);
  }

log_printf(15, "dname=%s switching to iter\n", dname);

  for (;;) {
     //** If we made it here then grab the next file and look it up.
     ftype = os_next_object(dit->lfs->lc->os, dit->it, &fname, &prefix_len);
     if (ftype <= 0) { //** No more files
        dt = apr_time_now() - now;
        dt /= APR_USEC_PER_SEC;
off2=off;
log_printf(15, "dname=%s NOTHING LEFT off=%d dt=%lf\n", dname,off2, dt);
        return(0);
     }

//     if (dit->val[0] != NULL) {
//        ino = 0; sscanf(dit->val[0], XIDT, &ino);
//     } else {
//        log_printf(0, "Missing inode!  fname=%s\n", fname);
//        for (i=0; i<_inode_key_size; i++) if (dit->val[i] != NULL) free(dit->val[i]);
//        return(-ENOENT);
//     }

     lfs_lock(dit->lfs);
     fentry = _lfs_dentry_get(dit->lfs, fname);
     type_malloc_clear(entry, lio_dentry_t, 1);
     if (fentry == NULL) { //** New entry
log_printf(15, "new entry fname=%s\n", fname);  flush_log();
        entry->fname = fname;
        entry->name_start = prefix_len+1;
        entry->ino = 0;  //** Reset after the parse

        //** This is for the lookup table
        type_malloc_clear(fentry, lio_dentry_t, 1);
        fentry->fname = strdup(fname);
        fentry->ino = 0;  //** REset it after the parse
        _lfs_dentry_insert(lfs, fentry);
     } else {
log_printf(15, "existing entry fname=%s\n", fname); flush_log();
        *entry = *fentry;
        entry->fname = fname;
        fentry->recheck_time = apr_time_now() + lfs->attr_to;
     }

     inode = _lfs_inode_lookup(dit->lfs, entry->ino);
     if (inode == NULL) {
        type_malloc_clear(inode, lio_inode_t, 1);
        inode->ftype = ftype;
        inode->ino = 0;
        _lfs_parse_inode_vals(dit->lfs, inode, dit->val, dit->v_size);
        _lfs_inode_insert(dit->lfs, inode);
     } else {
        _lfs_parse_inode_vals(dit->lfs, inode, dit->val, dit->v_size);
     }

     fentry->ino = entry->ino = inode->ino;  //** Make sure the ino is in sync

off2=off;
log_printf(1, "next fname=%s ftype=%d prefix_len=%d ino=" XIDT " off=%d\n", entry->fname, ftype, prefix_len, entry->ino, off);


     move_to_bottom(dit->stack);
     insert_below(dit->stack, entry);

     if (entry->flagged != LFS_INODE_DELETE) {
        lfs_fill_stat(&stbuf, inode);
        if (filler(buf, dentry_name(entry), &stbuf, off) == 1) {
           dt = apr_time_now() - now;
           dt /= APR_USEC_PER_SEC;
           log_printf(1, "dt=%lf\n", dt);
           lfs_unlock(dit->lfs);
           return(0);
        }
     } else {
        log_printf(1, "fname=%s flagged for removal\n", entry->fname);
     }

     off++;

     lfs_unlock(dit->lfs);
  }

  return(0);
}

//*************************************************************************
// lfs_closedir - Closes the opendir file handle
//*************************************************************************

int lfs_closedir(const char *fname, struct fuse_file_info *fi)
{
  lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
  lio_dentry_t *entry;

  log_printf(1, "fname=%s START\n", fname); flush_log();

  if (dit == NULL) {
     return(-EBADF);
  }

  free(dit->dot_path);
  free(dit->dotdot_path);

  //** Cyle through releasing all the entries
  lfs_lock(dit->lfs);
  while ((entry = (lio_dentry_t *)pop(dit->stack)) != NULL) {
log_printf(0, "fname=%s\n", entry->fname); flush_log();
     free(entry->fname);
     free(entry);
  }
  lfs_unlock(dit->lfs);

  free_stack(dit->stack, 0);

  os_destroy_object_iter(dit->lfs->lc->os, dit->it);
  os_regex_table_destroy(dit->path_regex);
  free(dit);

  log_printf(1, "fname=%s END\n", fname); flush_log();

  return(0);
}


//*************************************************************************
// lfs_object_create
//*************************************************************************

int lfs_object_create(lio_fuse_t *lfs, const char *fname, mode_t mode, int ftype)
{
  lio_inode_t *inode;
  char fullname[OS_PATH_MAX];
  int err, n;

  log_printf(1, "fname=%s\n", fname); flush_log();

  lfs_lock(lfs);

  //** Make sure it doesn't exists
  n = lioc_exists(lfs->lc, lfs->lc->creds, (char *)fname);
  if (n != 0) {  //** File already exists
     log_printf(15, "File already exist! fname=%s\n", fullname);
     lfs_unlock(lfs);
     return(-EEXIST);
   }

  //** If we made it here it's a new file or dir
  //** Create the new object
  err = gop_sync_exec(lio_create_object(lfs->lc, lfs->lc->creds, (char *)fname, ftype, NULL, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "Error creating object! fname=%s\n", fullname);
     lfs_unlock(lfs);
     if (strlen(fullname) > 3900) {  //** Probably a path length issue
        return(-ENAMETOOLONG);
     }
     return(-EREMOTEIO);
  }

  //** Load the inode
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) { //** File doesn't exist!
     log_printf(1, "File doesn't exist! fname=%s\n", fname);
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  lfs_unlock(lfs);

  return(0);
}

//*************************************************************************
// lfs_mknod - Makes a regular file
//*************************************************************************

int lfs_mknod_real(const char *fname, mode_t mode, dev_t rdev, lio_fuse_t *lfs)
{
  log_printf(1, "fname=%s\n", fname); flush_log();
  return(lfs_object_create(lfs, fname, mode, OS_OBJECT_FILE));
}
int lfs_mknod(const char *fname, mode_t mode, dev_t rdev)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  // we don't support making devices in FUSE, right?
  return lfs_mknod_real(fname, mode, rdev, lfs);
}




//*************************************************************************
// lfs_mkdir - Makes a directory
//*************************************************************************

int lfs_mkdir(const char *fname, mode_t mode)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "fname=%s mode=%d\n", fname, mode); flush_log();

  return(lfs_object_create(lfs, fname, mode, OS_OBJECT_DIR));
}

//*****************************************************************
// lfs_actual_remove - Does the actual removal
//*****************************************************************

int lfs_actual_remove(lio_fuse_t *lfs, const char *fname, int ftype)
{
  int err;
  err = gop_sync_exec(lio_remove_object(lfs->lc, lfs->lc->creds, (char *)fname, NULL, ftype));
log_printf(1, "remove err=%d\n", err);
  if (err == OP_STATE_SUCCESS) {
      return(0);
  } else if ((ftype & OS_OBJECT_DIR) > 0) { //** Most likey the dirs not empty
      return(-ENOTEMPTY);
  }

  return(-EREMOTEIO);  //** Probably an expired exnode but through an error anyway
}

//*****************************************************************
//  lfs_object_remove - Removes a file or directory
//*****************************************************************

int lfs_object_remove(lio_fuse_t *lfs, const char *fname)
{
  lio_inode_t *inode;
  lio_dentry_t *entry;
  int ftype, remove_now;

  log_printf(1, "fname=%s\n", fname); flush_log();

  lfs_lock(lfs);

  remove_now = 1;

  inode = _lfs_dentry_lookup(lfs, fname, 1);
  entry = _lfs_dentry_get(lfs, fname);
  if ((entry == NULL) || (inode == NULL)) { //** Oops it doesn't exist
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  ftype = inode->ftype;

  if (inode->fh == NULL) { //** No one has it open so safe to remove
     _lfs_dentry_remove(lfs, entry);
     lfs_dentry_destroy(lfs, entry);
     _lfs_inode_remove(lfs, inode);
     lfs_inode_destroy(lfs, inode);
  } else {   //** Somebody has it open
     if (entry->ref_count <= 0) { //** No one has this entry open
        if ((inode->nlinks>1) && (inode->ftype & OS_OBJECT_FILE)) { //** It's a hard link with extra links so ok to remove
           _lfs_dentry_remove(lfs, entry);
           lfs_dentry_destroy(lfs, entry);
           inode->nlinks--;
        } else {
          remove_now = 0;
          entry->flagged = LFS_INODE_DELETE;  //** Mark if for deletion when the file is closed
        }
     } else {
       remove_now = 0;
       entry->flagged = LFS_INODE_DELETE;  //** Mark if for deletion when the file is closed
     }
  }

  lfs_unlock(lfs);

  if (remove_now == 1) {
     return(lfs_actual_remove(lfs, fname, ftype));
  }

  return(0);  //** Deferred removal
}


//*****************************************************************
//  lfs_unlink - Remove a file
//*****************************************************************
int lfs_unlink(const char *fname) {
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_unlink_real(fname, lfs);
}
int lfs_unlink_real(const char *fname, lio_fuse_t *lfs)
{
  log_printf(1, "fname=%s\n", fname); flush_log();

  return(lfs_object_remove(lfs, fname));
}

//*****************************************************************
//  lfs_rmdir - Remove a directory
//*****************************************************************

int lfs_rmdir(const char *fname)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "fname=%s\n", fname); flush_log();

  return(lfs_object_remove(lfs, fname));
}

//*****************************************************************
// lfs_file_lock - Locks out the ability to opne/close files mapping
//    to the same slot.  The slot is returned
//*****************************************************************

int lfs_file_lock(lio_fuse_t *lfs, const char *fname)
{
  lio_inode_t *inode;
  int slot;

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     log_printf(0, "ERROR fname=%s doesn't exist!\n", fname);
     return(-ENOENT);
  }

  //** Got a symlink so need to load the link
  if ((inode->ftype & OS_OBJECT_SYMLINK) > 0) {
     inode = _lfs_dentry_lookup(lfs, fname, 1);
     if (inode == NULL) {
        lfs_unlock(lfs);
        log_printf(0, "ERROR fname=%s doesn't exist!\n", fname);
        return(-ENOENT);
     }
  }

  slot = inode->ino % lfs->file_count;
  lfs_unlock(lfs);

  apr_thread_mutex_lock(lfs->file_lock[slot]);

  return(slot);
}

//*****************************************************************
// lfs_file_unlock - Releases the file lock
//*****************************************************************

void lfs_file_unlock(lio_fuse_t *lfs, const char *fname, int slot)
{
  apr_thread_mutex_unlock(lfs->file_lock[slot]);
}


//*****************************************************************
// lfs_load_file_handle - Loads the shared file handle
//*****************************************************************

lio_fuse_file_handle_t *lfs_load_file_handle(lio_fuse_t *lfs, const char *fname)
{
  lio_fuse_file_handle_t *fh;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  lio_inode_t *inode;
  char *ex_data;
  int v_size, err;

  log_printf(15, "loading exnode fname=%s\n", fname);

  //** Get the exnode
  v_size = -lfs->lc->max_attr;
  err = lioc_get_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, "system.exnode", (void **)&ex_data, &v_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "Failed retrieving exnode! path=%s\n", fname);
     return(NULL);
  }

  lfs_lock(lfs);  //** Want to make sure I'm the only one doing the loading

  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode->fh != NULL) {  //** Someone beat me to it so use theirs
     inode->fh->ref_count++;
     lfs_unlock(lfs);
     return(inode->fh);
  }

  //** Load it
  exp = exnode_exchange_text_parse(ex_data);
  ex = exnode_create();
  if (exnode_deserialize(ex, exp, lfs->lc->ess) != 0) {
     log_printf(0, "Bad exnode! fname=%s\n", fname);
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     lfs_unlock(lfs);
     return(NULL);
  }

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     log_printf(0, "No default segment!  Aborting! fname=%s\n", fname);
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     lfs_unlock(lfs);
     return(NULL);
  }

  //** Make the shared handle
  type_malloc_clear(fh, lio_fuse_file_handle_t, 1);
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

int lfs_myopen(lio_fuse_t *lfs, const char *fname, int flags, lio_fuse_fd_t **myfd)
{
  lio_inode_t *inode;
  lio_dentry_t *entry;
  lio_fuse_fd_t *fd;
  lio_fuse_file_handle_t *fh;
  int slot;

  log_printf(15, "fname=%s\n", fname); flush_log();

  *myfd = NULL;

  slot = lfs_file_lock(lfs, fname);
  if (slot < 0) return(slot);

  lfs_lock(lfs);

  //** Lookup the inode
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     lfs_file_unlock(lfs, fname, slot);
     return(-ENOENT);
  }

  //** Get the entry and flag it as being used
  entry = _lfs_dentry_get(lfs, fname);
  entry->ref_count++;

  fh = inode->fh;
  if (fh != NULL) fh->ref_count++;

log_printf(1, "fname=%s inode=%p fh=%p entry=%p\n", fname, inode, entry);
  lfs_unlock(lfs);

  if (fh == NULL) { //** Not currently open so need to load it
     fh = lfs_load_file_handle(lfs, fname);
  }

  if (fh == NULL) { //** Failed getting the shared file handle so return an error
     lfs_unlock(lfs);
log_printf(1, "ERROR failed getting shared file handle fname=%s entry=%p ref_coun t=%d\n", fname, entry, entry->ref_count); flush_log();
     entry->ref_count--;
     lfs_unlock(lfs);
     lfs_file_unlock(lfs, fname, slot);
     return(-ENOENT);
  }

  //** Make the file handle
  type_malloc_clear(fd, lio_fuse_fd_t, 1);
  fd->fh = fh;
  fd->entry = entry;
  fd->mode = O_RDONLY;
  if (flags & O_RDONLY) {
     fd->mode = LFS_READ_MODE;
  } else if (flags & O_WRONLY) {
     fd->mode = LFS_WRITE_MODE;
  } else if (flags & O_RDWR) {
     fd->mode = LFS_READ_MODE | LFS_WRITE_MODE;
  }

  lfs_file_unlock(lfs, fname, slot);

  *myfd = fd;
  return(0);
}


//*****************************************************************
// lfs_open - Opens a file for I/O
//*****************************************************************

int lfs_open_real(const char *fname, struct fuse_file_info *fi, lio_fuse_t *lfs)
{
  lio_fuse_fd_t *fd;
  int err;
  // FIXME, need to populate fi upstream and undo this comment
  log_printf(1, "fname=%s dio=%d START \n", fname, fi->direct_io); flush_log();

//fi->direct_io = 1;

  err = lfs_myopen(lfs, fname, fi->flags, &fd);

  if (err == 0) {
     fi->fh = (uint64_t)fd;
  }
  //log_printf(1, "fname=%s dio=%d err=%d END\n", fname, fi->direct_io, err); flush_log();
  log_printf(1, "fname=%s dio=%d err=%d fh=%d END\n", fname, fi->direct_io, err, fi->fh); flush_log();
  return(err);
}

int lfs_open(const char *fname, struct fuse_file_info *fi)
{
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  lio_fuse_t *lfs;
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_open_real(fname, fi, lfs);
}


//*****************************************************************
// lfs_myclose - Closes a file
//*****************************************************************

int lfs_myclose_real(char *fname, lio_fuse_fd_t *fd, lio_fuse_t *lfs)
{
  //// lio_fuse_t *lfs;
  lio_fuse_file_handle_t *fh;
  lio_dentry_t *fentry;
  lio_inode_t *inode;
  int flags, slot, n;
  char *key[6] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data", NULL, NULL, NULL };
  char *val[6];
  int err, v_size[6];
  char ebuf[128];
  segment_errors_t serr;
  apr_time_t now;
  double dt;

    log_printf(1, "fname=%s modified=%d count=%d\n", fname, fd->fh->modified, fd->fh->ref_count); flush_log();

  slot = lfs_file_lock(lfs, fname);
  if (slot < 0) return(slot);

  //** Get the handles
  fh = fd->fh;
  //// lfs = fh->lfs;
  fentry = fd->entry;

  free(fd);

  lfs_lock(lfs);

  inode = _lfs_dentry_lookup(lfs, fname, 0);
  if (inode == NULL) {
     log_printf(0, "DEBUG ERROR  missing inode on open file! fname=%s\n", fname);
  } else {
     inode->size = segment_size(fh->seg);  //** Update the size on a subsequent call
  }

  if (fh->ref_count > 1) {  //** Somebody else has it open as well
     fh->ref_count--;
     fentry->ref_count--;
     flags = fentry->flagged;
     lfs_unlock(lfs);
     lfs_file_unlock(lfs, fname, slot);
     if (flags == LFS_INODE_DELETE) return(lfs_object_remove(lfs, fname));
     return(0);
  }

  lfs_unlock(lfs);

log_printf(1, "FLUSH/TRUNCATE fname=%s\n", fname);
  //** Flush and truncate everything which could take some time
  now = apr_time_now();
  err = gop_sync_exec(segment_truncate(fh->seg, lfs->lc->da, segment_size(fh->seg), lfs->lc->timeout));
  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "TRUNCATE fname=%s dt=%lf\n", fname, dt);
  now = apr_time_now();
  err = gop_sync_exec(segment_flush(fh->seg, lfs->lc->da, 0, segment_size(fh->seg)+1, lfs->lc->timeout));
  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "FLUSH fname=%s dt=%lf\n", fname, dt);

  //** Now acquire the global lock.  No need to check again since I have the file lock
  lfs_lock(lfs);

  inode = _lfs_dentry_lookup(lfs, fname, 0);
  if (inode == NULL) {
     log_printf(0, "ERROR  missing inode on open file! fname=%s\n", fname);
     lfs_unlock(lfs);
     lfs_file_unlock(lfs, fname, slot);
     return(-EIO);
  }

  //** Now we can release the lock while we update the object cause we still have the file lock
  lfs_unlock(lfs);


  log_printf(5, "starting update process fname=%s\n", fname); flush_log();

  //** Ok no one has the file opened so teardown the segment/exnode
  //** IF not modified just tear down and clean up
  n = 0;
  if (fh->modified == 0) {
     //*** See if we need to update the error counts
     lioc_get_error_counts(lfs->lc, fh->seg, &serr);
     n = lioc_encode_error_counts(&serr, key, val, ebuf, v_size, 0);
     if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
        log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fname, serr.hard, serr.soft, serr.write);
     }
     if (n > 0) {
        err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, fname, NULL, key, (void **)val, v_size, n);
        if (err != OP_STATE_SUCCESS) {
           log_printf(0, "ERROR updating exnode! fname=%s\n", fname);
        }
     }

     //** Tear everything down
     exnode_destroy(fh->ex);
     fh->ref_count--;
     lfs_lock(lfs);
     fentry->ref_count--;
     flags = fentry->flagged;
     inode->fh = NULL;
     lfs_unlock(lfs);
     lfs_file_unlock(lfs, fname, slot);
     free(fh);
     if (flags == LFS_INODE_DELETE) return(lfs_object_remove(lfs, fname));
     return(0);
  }

  //** Get any errors that may have occured
  lioc_get_error_counts(lfs->lc, fh->seg, &serr);

  now = apr_time_now();

  //** Update the exnode and misc attributes
  err = lioc_update_exnode_attrs(lfs->lc, lfs->lc->creds, fh->ex, fh->seg, fname, &serr);
  if (err > 1) {
     log_printf(0, "ERROR updating exnode! fname=%s\n", fname);
  }

  n += ((err > 1) || (serr.hard > 0)) ? 1 : 0;

  if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
     log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fname, serr.hard, serr.soft, serr.write);
  }

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "ATTR_UPDATE fname=%s dt=%lf\n", fname, dt);

  lfs_lock(lfs);  //** Do the final release
  fh->ref_count--;
  fentry->ref_count--;
  flags = fentry->flagged;
  inode->fh = NULL;
  lfs_unlock(lfs);

  //** Clean up
  now = apr_time_now();
  exnode_destroy(fh->ex);
  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "exnode_destroy fname=%s dt=%lf\n", fname, dt);

  free(fh);

  lfs_file_unlock(lfs, fname, slot);

  if (flags == LFS_INODE_DELETE) return(lfs_object_remove(lfs, fname));

  return((serr.hard==0) ? 0 : -EIO);
}

int lfs_myclose(char *fname, lio_fuse_fd_t *fd)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_myclose_real(fname, fd, lfs);
}

//*****************************************************************
// lfs_release - Closes a file
//*****************************************************************
int lfs_release_real(const char *fname,
                     struct fuse_file_info *fi,
                     lio_fuse_t *lfs)
{
  lio_fuse_fd_t *fd;
  int err;
  apr_time_t now;
  double dt;

  log_printf(1, "SART fname=%s\n", fname); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     return(-EBADF);
  }

  now = apr_time_now();
  err = lfs_myclose_real((char *)fname, fd, lfs);

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "END fname=%s dt=%lf\n", fname, dt); flush_log();

  return(err);
}

int lfs_release(const char *fname, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }
  return lfs_release_real(fname, fi, lfs);
}

//*****************************************************************
// lfs_read_ex - Reads data from a file using a more native interface
//    NOTE: Does NO readahead
//*****************************************************************

int lfs_read_ex(const char *fname, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  int i, err, size;
  apr_time_t now;
  double dt;
  ex_off_t t1, t2;

  if (n_iov <=0) return(0);

  t1 = iov[0].len; t2 = iov[0].offset;
  log_printf(1, "fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fname, n_iov, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     log_printf(0, "ERROR: Got a null file desriptor\n");
     return(-EBADF);
  }

  lfs = NULL;
  lfs = fd->fh->lfs;
  if (lfs == NULL)
  {
    log_printf(0, "ERROR: Got a null LFS handle\n");
    return(-EBADF);
  }
  now = apr_time_now();

  //** Do the read op
  err = gop_sync_exec(segment_read(fd->fh->seg, lfs->lc->da, n_iov, iov, buffer, boff, lfs->lc->timeout));

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "END fname=%s seg=" XIDT " dt=%lf\n", fname, segment_id(fd->fh->seg), dt); flush_log();

  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR with read! fname=%s\n", fname);
     printf("got value %d\n", err);
     return(-EIO);
  }

  size = iov[0].len;
  for (i=1; i<n_iov; i++) size += iov[i].len;  
  return(size);
}

//*****************************************************************
// lfs_readv - Reads data from a file using a struct iovec
//    NOTE: Does NO readahead
//*****************************************************************

int lfs_readv(const char *fname, iovec_t *iov, int n_iov, size_t size, off_t off, struct fuse_file_info *fi)
{
  tbuffer_t tbuf;
  ex_iovec_t exv;

  tbuffer_vec(&tbuf, size, n_iov, iov);
  ex_iovec_single(&exv, off, size);
  return(lfs_read_ex(fname, 1, &exv, &tbuf, 0, fi));
}

//*****************************************************************
// lfs_read_test_ex - Reads data from a file using a lfs_read_ex routine
//    This routine is designed for simply testing the lfs_read_ex routine
//    and it should NOT be used for FUSE cause it does not check for a
//    read beyond EOF which FUSE allows.
//    NOTE: Does NO readahead
//*****************************************************************

int lfs_read_test_ex(const char *fname, char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
  tbuffer_t tbuf;
  ex_iovec_t exv;

  tbuffer_single(&tbuf, size, buf);
  ex_iovec_single(&exv, off, size);
  return(lfs_read_ex(fname, 1, &exv, &tbuf, 0, fi));
}


//*****************************************************************
// lfs_read - Reads data from a file
//    NOTE: Uses the LFS readahead hints
//*****************************************************************

int lfs_read(const char *fname, char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  tbuffer_t tbuf;
  ex_iovec_t exv;
  int err;
  ex_off_t ssize, pend, rsize, rend, dr;
  apr_time_t now;
  double dt;

  ex_off_t t1, t2;
  t1 = size; t2 = off;
  log_printf(1, "fname=%s size=" XOT " off=" XOT "\n", fname, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     log_printf(0, "ERROR: Got a null file desriptor\n");
     return(-EBADF);
  }

  lfs = NULL;
  lfs = fd->fh->lfs;
  if (lfs == NULL)
  {
    log_printf(0, "ERROR: Got a null LFS handle\n");
    return(-EBADF);
  }
  now = apr_time_now();

  //** Do the read op
  ssize = segment_size(fd->fh->seg);
  pend = off + size;
  log_printf(0, "ssize=" XOT " off=" XOT " len=" XOT " pend=" XOT " readahead=" XOT " trigger=" XOT "\n", ssize, off, size, pend, lfs->readahead, lfs->readahead_trigger);
  if (pend > ssize)
  {
    if (off > ssize) {
        // offset is past the end of the segment
        return(0);
    } else {
        size = ssize - off;  //** Tweak the size based on how much data there is
    }
  }
  log_printf(0, "tweaked len=" XOT "\n", size);
  if (size <= 0) { log_printf(0, "Clipped tweaked len\n"); return(0); }

  rend = pend + lfs->readahead;  //** Tweak based on readahead
  rsize = size;
  segment_lock(fd->fh->seg);
  dr = pend - fd->fh->readahead_end;
  if ((dr > 0) || ((-dr) > lfs->readahead_trigger)) {
     rsize = rend - off;
     if (rend > ssize)
     {
       if (off <= ssize) {
          rsize = ssize - off;  //** Tweak the size based on how much data there is
       }
     }

     fd->fh->readahead_end = rend;  //** Update the readahead end
  } else {
     rend = fd->fh->readahead_end;
  }
  segment_unlock(fd->fh->seg);

  tbuffer_single(&tbuf, size, buf);  //** This is the buffer size
  ex_iovec_single(&exv, off, rsize);  //** This is the buffer+readahead.  The extra doesn't get stored in the buffer.  Just in page cache.
  err = gop_sync_exec(segment_read(fd->fh->seg, lfs->lc->da, 1, &exv, &tbuf, 0, lfs->lc->timeout));

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "END fname=%s seg=" XIDT " size=" XOT " off=" XOT " rsize=" XOT " rend=" XOT " dt=%lf\n", fname, segment_id(fd->fh->seg), t1, t2, rsize, rend, dt); flush_log();

  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR with read! fname=%s\n", fname);
     printf("got value %d\n", err);
     return(-EIO);
  }

  return(size);
}

//*****************************************************************
// lfs_write_ex - Writes data from a file using a more native interface
//*****************************************************************

int lfs_write_ex(const char *fname, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  int i, err, size;
  apr_time_t now;
  double dt;
  ex_off_t t1, t2;

  if (n_iov <=0) return(0);

  t1 = iov[0].len; t2 = iov[0].offset;
  log_printf(1, "START fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fname, n_iov, t1, t2); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     log_printf(0, "ERROR: Got a null file desriptor\n");
     return(-EBADF);
  }

  lfs = NULL;
  lfs = fd->fh->lfs;
  if (lfs == NULL)
  {
    log_printf(0, "ERROR: Got a null LFS handle\n");
    return(-EBADF);
  }
  now = apr_time_now();

  atomic_set(fd->fh->modified, 1);

  //** Do the read op
  err = gop_sync_exec(segment_write(fd->fh->seg, lfs->lc->da, n_iov, iov, buffer, boff, lfs->lc->timeout));

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "END fname=%s seg=" XIDT " dt=%lf\n", fname, segment_id(fd->fh->seg), dt); flush_log();

  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR with write! fname=%s\n", fname);
     printf("got value %d\n", err);
     return(-EIO);
  }

  size = iov[0].len;
  for (i=1; i<n_iov; i++) size += iov[i].len;  
  return(size);
}

//*****************************************************************
// lfs_write - Writes data to a file
//*****************************************************************

int lfs_write(const char *fname, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
  tbuffer_t tbuf;
  ex_iovec_t exv;

  //** Do the write op
  tbuffer_single(&tbuf, size, (char *)buf);
  ex_iovec_single(&exv, off, size);
  return(lfs_write_ex(fname, 1, &exv, &tbuf, 0, fi));
}

//*****************************************************************
// lfs_writev - Writes data to a file using a struct iovec
//*****************************************************************

int lfs_writev(const char *fname, iovec_t *iov, int n_iov, size_t size, off_t off, struct fuse_file_info *fi)
{
  tbuffer_t tbuf;
  ex_iovec_t exv;

  //** Do the write op
  tbuffer_vec(&tbuf, size, n_iov, iov);
  ex_iovec_single(&exv, off, size);
  return(lfs_write_ex(fname, 1, &exv, &tbuf, 0, fi));
}

//*****************************************************************
// lfs_flush - Flushes any data to backing store
//*****************************************************************

int lfs_flush(const char *fname, struct fuse_file_info *fi)
{
  lio_fuse_t *lfs;
  lio_fuse_fd_t *fd;
  int err;
  apr_time_t now;
  double dt;

  now = apr_time_now();

  log_printf(1, "START fname=%s\n", fname); flush_log();

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     return(-EBADF);
  }

  lfs = fd->fh->lfs;

  err = gop_sync_exec(segment_flush(fd->fh->seg, lfs->lc->da, 0, segment_size(fd->fh->seg)+1, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     return(-EIO);
  }

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;
  log_printf(1, "END fname=%s dt=%lf\n", fname, dt); flush_log();

  return(0);
}

//*************************************************************************
// lfs_rename - Renames a file
//*************************************************************************

int lfs_rename(const char *oldname, const char *newname)
{
  lio_dentry_t *entry;
  int err;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "oldname=%s newname=%s\n", oldname, newname); flush_log();

  //** Do the move
  err = gop_sync_exec(lio_move_object(lfs->lc, lfs->lc->creds, (char *)oldname, (char *)newname));
  if (err != OP_STATE_SUCCESS) {
     return(-EIO);
  }

  //** Update the dentry names
  lfs_lock(lfs);
  entry = _lfs_dentry_get(lfs, oldname);
  if (entry != NULL) {
    _lfs_dentry_remove(lfs, entry);
    free(entry->fname);
    entry->fname = strdup(newname);
    _lfs_dentry_insert(lfs, entry);
  }
  lfs_unlock(lfs);

  return(0);
}



//*****************************************************************
// lfs_truncate - Truncate the file
//*****************************************************************

int lfs_truncate(const char *fname, off_t new_size)
{
  lio_fuse_fd_t *fd;
  lio_inode_t *inode;
  ex_off_t ts;
  int err;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "fname=%s\n", fname); flush_log();

  ts = new_size;
  log_printf(15, "adjusting size=" XOT "\n", ts);
  err = lfs_myopen(lfs, fname, O_WRONLY, &fd);
  if (fd == NULL) {
     return(-EBADF);
  }

  log_printf(15, "calling truncate\n");
  err = gop_sync_exec(segment_truncate(fd->fh->seg, lfs->lc->da, ts, lfs->lc->timeout));
  log_printf(15, "segment_truncate=%d\n", err);

  lfs_lock(lfs);
  inode= _lfs_dentry_lookup(lfs, fname, 0);
  if (inode != NULL) {
     inode->size = new_size;
     atomic_set(fd->fh->modified, 1);
  }
  lfs_unlock(lfs);

  lfs_myclose((char *)fname, fd);

  return(0);
}

// FIXME: This will get refactored "soon" - AMM Oct 8, 2014
int lfs_truncate_fd_temp_melo(lio_fuse_t *lfs, struct fuse_file_info *fi, off_t new_size)
{  
  lio_inode_t *inode;
  lio_fuse_fd_t *fd;

  ex_off_t ts;
  int err;

  ts = new_size;
  log_printf(15, "adjusting size=" XOT "\n", ts);
  log_printf(15, "calling truncate\n");

  fd = (lio_fuse_fd_t *)fi->fh;
  if (fd == NULL) {
     return(-EBADF);
  }

  err = gop_sync_exec(segment_truncate(fd->fh->seg, lfs->lc->da, ts, lfs->lc->timeout));
  log_printf(15, "segment_truncate=%d\n", err);

  lfs_lock(lfs);
  inode= _lfs_dentry_lookup(lfs, fd->entry->fname, 0);
  if (inode != NULL) {
     inode->size = new_size;
     atomic_set(fd->fh->modified, 1);
  }
  lfs_unlock(lfs);

  return(0);
}


//*****************************************************************
// lfs_utimens - Sets the access and mod times in ns
//*****************************************************************

int lfs_utimens(const char *fname, const struct timespec tv[2])
{
  lio_inode_t *inode;
  char buf[1024];
  char *key;
  char *val;
  int v_size;
  ex_off_t ts;
  int err;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "fname=%s\n", fname); flush_log();


  key = "system.modify_attr";
  ts = tv[1].tv_sec;
  snprintf(buf, 1024, XOT "|%s", ts, lfs->id);
  val = buf;
  v_size = strlen(buf);

//  key = "os.timestamp.system.modify_attr";
//  val = lfs->id;
//  v_size = strlen(val);

  err = lioc_set_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, key, (void *)val, v_size);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating stat! fname=%s\n", fname);
     return(-EBADE);
  }

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 0);
  if (inode != NULL) inode->modify_data_ts = tv[1].tv_sec;
if (inode == NULL) log_printf(1, "ERROR missing inode fname=%s\n", fname);
  lfs_unlock(lfs);

  return(0);
}

//*****************************************************************
// lfs_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

int lfs_listxattr(const char *fname, char *list, size_t size)
{
  char *buf, *key, *val;
  int bpos, bufsize, v_size, n, i, err;
  os_regex_table_t *attr_regex;
  os_attr_iter_t *it;
  os_fd_t *fd;
  lio_inode_t *inode;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  bpos= size;
  log_printf(1, "fname=%s size=%d\n", fname, bpos); flush_log();

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     return(-ENOENT);
  }
  lfs_unlock(lfs);


  //** Make an iterator
  attr_regex = os_path_glob2regex("user.*");
  err = gop_sync_exec(os_open_object(lfs->lc->os, lfs->lc->creds, (char *)fname, OS_MODE_READ_IMMEDIATE, lfs->id, &fd, lfs->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR: opening file: %s err=%d\n", fname, err);
     return(-ENOENT);
  }
  it = os_create_attr_iter(lfs->lc->os, lfs->lc->creds, fd, attr_regex, 0);
  if (it == NULL) {
     log_printf(15, "ERROR creating iterator for fname=%s\n", fname);
     return(-ENOENT);
  }

  //** Cycle through the keys
  bufsize = 10*1024;
  type_malloc_clear(buf, char, bufsize);
  val = NULL;
  bpos = 0;

//  if ((lfs->enable_tape == 1) && ((ftype & OS_OBJECT_SYMLINK) == 0)) { //** Add the tape attribute
  if (lfs->enable_tape == 1)  { //** Add the tape attribute
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

log_printf(15, "bpos=%d size=%d buf=%s\n", bpos, size, buf);

  if (size == 0) {
log_printf(15, "SIZE bpos=%d buf=%s\n", bpos, buf);
  } else if (size > bpos) {
log_printf(15, "FULL bpos=%d buf=%s\n", bpos, buf);
    memcpy(list, buf, bpos);
  } else {
log_printf(15, "ERANGE bpos=%d buf=%s\n", bpos, buf);
  }
  free(buf);

  return(bpos);
}

//*****************************************************************
// lfs_set_tape_attr - Disburse the tape attribute
//*****************************************************************

void lfs_set_tape_attr(lio_fuse_t *lfs, char *fname, char *mytape_val, int tape_size)
{
  char *val[_tape_key_size], *tape_val, *bstate, *tmp;
  int v_size[_tape_key_size];
  int n, i, fin, ex_key, err, ftype, nkeys;;
  lio_inode_t *inode;
  exnode_exchange_t *exp;
  exnode_t *ex, *cex;

  type_malloc(tape_val, char, tape_size+1);
  memcpy(tape_val, mytape_val, tape_size);
  tape_val[tape_size] = 0;  //** Just to be safe with the string/prints routines

log_printf(15, "fname=%s tape_size=%d\n", fname, tape_size);
log_printf(15, "Tape attribute follows:\n%s\n", tape_val);

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     return;
  }
  ftype = inode->ftype;
  lfs_unlock(lfs);

  nkeys = (ftype & OS_OBJECT_SYMLINK) ? 1 : _tape_key_size;

  //** The 1st key should be n_keys
  tmp = string_token(tape_val, "=\n", &bstate, &fin);
  if (strcmp(tmp, "n_keys") != 0) { //*
     log_printf(0, "ERROR parsing tape attribute! Missing n_keys! fname=%s\n", fname);
     log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
     free(tape_val);
     return;
  }

  n = -1;
  sscanf(string_token(NULL, "=\n", &bstate, &fin), "%d", &n);
log_printf(15, "fname=%s n=%d nkeys=%d ftype=%d\n", fname, n, nkeys, ftype);
  if (n != nkeys) {
     log_printf(0, "ERROR parsing n_keys size fname=%s\n", fname);
     log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
     free(tape_val);
     return;
  }

log_printf(15, "nkeys=%d fname=%s ftype=%d\n", nkeys, fname, ftype);

  //** Set all of them to 0 cause the size is used to see if the key was loaded
  for (i=0; i<_tape_key_size; i++) { v_size[i] = 0; }

  //** Parse the sizes
  for (i=0; i<nkeys; i++) {
    tmp = string_token(NULL, "=\n", &bstate, &fin);
    if (strcmp(tmp, _tape_keys[i]) == 0) {
       sscanf(string_token(NULL, "=\n", &bstate, &fin), "%d", &(v_size[i]));
       if (v_size[i] < 0) {
          log_printf(0, "ERROR parsing key=%s size=%d fname=%s\n", tmp, v_size[i], fname);
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
  for (i=0; i<nkeys; i++) {
      val[i] = NULL;
      if (v_size[i] > 0) {
         type_malloc(val[i], char, v_size[i]+1);
         memcpy(val[i], &(bstate[n]), v_size[i]);
         val[i][v_size[i]] = 0;
         n = n + v_size[i];
         log_printf(15, "fname=%s key=%s val=%s\n", fname, _tape_keys[i], val[i]);
      }
  }

  //** Just need to process the exnode
  ex_key = 1;  //** tape_key index for exnode
  if (v_size[ex_key] > 0) {
     //** If this has a caching segment we need to disable it from being adding
     //** to the global cache table cause there could be multiple copies of the
     //** same segment being serialized/deserialized.
     //** Deserialize it
     exp = exnode_exchange_text_parse(val[ex_key]);
     ex = exnode_create();
     err = exnode_deserialize(ex, exp, lfs->lc->ess_nocache);
     exnode_exchange_free(exp);
     val[ex_key] = NULL;

     if (err != 0) {
        log_printf(1, "ERROR parsing parent exnode fname=%s\n", fname);
        exnode_exchange_destroy(exp);
        exnode_destroy(ex);
        exnode_destroy(cex);
     }

     //** Execute the clone operation
     err = gop_sync_exec(exnode_clone(lfs->lc->tpc_unlimited, ex, lfs->lc->da, &cex, NULL, CLONE_STRUCTURE, lfs->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR cloning parent fname=%s\n", fname);
     }

     //** Serialize it for storage
     exnode_serialize(cex, exp);
     val[ex_key] = exp->text.text;
     v_size[ex_key] = strlen(val[ex_key]);
     exp->text.text = NULL;
     exnode_exchange_destroy(exp);
     exnode_destroy(ex);
     exnode_destroy(cex);
  }

  //** Store them
  err = lioc_set_multiple_attrs(lfs->lc, lfs->lc->creds, (char *)fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR updating exnode! fname=%s\n", fname);
  }

  //** Clean up
  free(tape_val);
  for (i=0; i<nkeys; i++) { if (val[i] != NULL) free(val[i]); }

  return;
}


//*****************************************************************
// lfs_get_tape_attr - Retreives the tape attribute
//*****************************************************************

void lfs_get_tape_attr(lio_fuse_t *lfs, char *fname, char **tape_val, int *tape_size)
{
  char *val[_tape_key_size];
  int v_size[_tape_key_size];
  int n, i, j, used, ftype, nkeys;
  int hmax= 1024;
  char *buffer, header[hmax];
  lio_inode_t *inode;

  *tape_val = NULL;
  *tape_size = 0;

log_printf(15, "START fname=%s\n", fname);

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
     return;
  }
  ftype = inode->ftype;
  lfs_unlock(lfs);

  for (i=0; i<_tape_key_size; i++) {
    val[i] = NULL;
    v_size[i] = -lfs->lc->max_attr;
  }

log_printf(15, "fname=%s ftype=%d\n", fname, ftype);
  nkeys = (ftype & OS_OBJECT_SYMLINK) ? 1 : _tape_key_size;
  i = lioc_get_multiple_attrs(lfs->lc, lfs->lc->creds, fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
  if (i != OP_STATE_SUCCESS) {
     log_printf(15, "Failed retrieving file info!  path=%s\n", fname);
     return;
  }

  //** Figure out how much space we need
  n = 0;
  used = 0;
  append_printf(header, &used, hmax, "n_keys=%d\n", nkeys);
  for (i=0; i<nkeys; i++) {
     j = (v_size[i] > 0) ? v_size[i] : 0;
     n = n + 1 + j;
     append_printf(header, &used, hmax, "%s=%d\n", _tape_keys[i], j);
  }

  //** Copy all the data into the buffer;
  n = n + used;
  type_malloc_clear(buffer, char, n);
  n = used;
  memcpy(buffer, header, used);
  for (i=0; i<nkeys; i++) {
     if (v_size[i] > 0) {
        memcpy(&(buffer[n]), val[i], v_size[i]);
        n = n + v_size[i];
        free(val[i]);
     }
  }

log_printf(15, "END fname=%s\n", fname);

  *tape_val = buffer;
  *tape_size = n;
  return;
}

//*****************************************************************
//  lfs_attr_free - Destroys an attribute
//*****************************************************************

void lfs_attr_free(list_data_t *obj)
{
  lio_attr_t *a = (lio_attr_t *)obj;

  log_printf(15, "a=%p\n", a);
  if (a == NULL) return;

  if (a->v_size > 0) free(a->val);
  free(a);
}

//*****************************************************************
// lfs_getxattr - Gets a extended attributes
//*****************************************************************
int lfs_getxattr(const char *fname, const char *name, char *buf, size_t size) {
    lio_fuse_t *lfs;
    struct fuse_context *ctx;
    ctx = fuse_get_context();
    if (NULL == ctx || NULL == ctx->private_data)
    {
        log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
        return(-EINVAL);
    } else {
        lfs = (lio_fuse_t*)ctx->private_data;
    }
    return lfs_getxattr_real(fname, name, buf, size, lfs);
}
int lfs_getxattr_real(const char *fname, const char *name, char *buf, size_t size, lio_fuse_t *lfs)
{
  char *val;
  int v_size, err, got_tape;
  char aname[512];
  lio_inode_t *inode;
  lio_attr_t *attr;
  apr_time_t now, now2;
  double dt, dt2;
  now = apr_time_now();

  v_size= size;
  log_printf(1, "fname=%s size=%d attr_name=%s\n", fname, size, name); flush_log();

  //** See if it's cached
  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  snprintf(aname, sizeof(aname), XIDT ":%s", inode->ino, name);
  attr = list_search(lfs->attr_index, aname);
  if (attr != NULL) { //** In cache so check if it's expired
     if (apr_time_now() <= attr->recheck_time) {  //** It's good
        v_size = attr->v_size;
        val = attr->val;

        dt = apr_time_now() - now;
        dt /= APR_USEC_PER_SEC;

        if (size == 0) {
          log_printf(1, "CACHED SIZE bpos=%d buf=%s dt=%lf\n", v_size, val, dt);
        } else if (size >= v_size) {
          log_printf(1, "CACHED FULL bpos=%d buf=%s dt=%lf\n", v_size, val, dt);
          memcpy(buf, val, v_size);
        } else {
          log_printf(1, "CACHED ERANGE bpos=%d buf=%s dt=%lf\n", v_size, val, dt);
        }
        lfs_unlock(lfs);
        return(v_size);
     }
  }
  lfs_unlock(lfs);

  now2 = apr_time_now();

  //** IF we made it here we either don't have it cached or it's expired
  v_size = (size == 0) ? -lfs->lc->max_attr : -size;
  val = NULL;
  got_tape = 0;
  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {  //** Want the tape backup attr
     lfs_get_tape_attr(lfs, (char *)fname, &val, &v_size);
     got_tape = 1;
  } else {
     err = lioc_get_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void **)&val, &v_size);
     if (err != OP_STATE_SUCCESS) {
        return(-ENOENT);
     }
  }

  dt2 = apr_time_now() - now2;
  dt2 /= APR_USEC_PER_SEC;

  if (v_size < 0) v_size = 0;  //** No attribute

  if (size == 0) {
log_printf(1, "SIZE bpos=%d buf=%s\n", v_size, val);
  } else if (size >= v_size) {
log_printf(1, "FULL bpos=%d buf=%s\n",v_size, val);
    memcpy(buf, val, v_size);
  } else {
log_printf(1, "ERANGE bpos=%d buf=%s\n", v_size, val);
  }

  //** Update the table... but not if it's the tape attr requested
  if (got_tape) {
     if (val != NULL) free(val);
     return(v_size);
  }

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  snprintf(aname, sizeof(aname), XIDT ":%s", inode->ino, name);
log_printf(1, "fname=%s aname=%s\n", fname, aname);
  attr = list_search(lfs->attr_index, aname);

  dt = apr_time_now() - now;
  dt /= APR_USEC_PER_SEC;

  if (attr != NULL) { //** Already exists so just update the info
log_printf(1, "UPDATING fname=%s aname=%s, dt=%lf dt_query=%lf\n", fname, aname, dt, dt2);

     if (attr->val != NULL) free (attr->val);
     attr->val = val;
     attr->v_size = v_size;
  } else {   //** New entry so insert it

     type_malloc_clear(attr, lio_attr_t, 1);
     attr->val = val;
     attr->v_size = v_size;
log_printf(1, "ADDING fname=%s aname=%s p=%p v_size=%d df=%lf dt_query=%lf\n", fname, aname, attr, attr->v_size, dt, dt2);
     list_insert(lfs->attr_index, strdup(aname), attr);
  }

  attr->recheck_time = apr_time_now() + lfs->xattr_to;
  lfs_unlock(lfs);

  return(v_size);
}

//*****************************************************************
// lfs_setxattr - Sets a extended attribute
//*****************************************************************
int lfs_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags)
{

    lio_fuse_t *lfs;
    struct fuse_context *ctx;
    ctx = fuse_get_context();
    if (NULL == ctx || NULL == ctx->private_data)
    {
        log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
        return(-EINVAL);
    } else {
        lfs = (lio_fuse_t*)ctx->private_data;
    }

    return lfs_setxattr_real(fname, name, fval, size, flags, lfs);
}
int lfs_setxattr_real(const char *fname, const char *name, const char *fval, size_t size, int flags, lio_fuse_t *lfs)
{
  char *val;
  int v_size, err;
  lio_inode_t *inode;
  lio_attr_t *attr;
  char aname[512];

  v_size= size;
  log_printf(1, "fname=%s size=%d attr_name=%s\n", fname, size, name); flush_log();

  if (flags != 0) { //** Got an XATTR_CREATE/XATTR_REPLACE
     v_size = 0;
     val = NULL;
     err = lioc_get_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void **)&val, &v_size);
     if (flags == XATTR_CREATE) {
        if (err == OP_STATE_SUCCESS) {
           return(-EEXIST);
        }
     } else if (flags == XATTR_REPLACE) {
       if (err != OP_STATE_SUCCESS) {
          return(-ENOATTR);
       }
     }
  }

  v_size = size;
  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {  //** Got the tape attribute
     lfs_set_tape_attr(lfs, (char *)fname, (char *)fval, v_size);
     return(0);
  } else {
     err = lioc_set_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void *)fval, v_size);
     if (err != OP_STATE_SUCCESS) {
        return(-ENOENT);
     }
  }

  //** Update the cache copy as well... except the tape attribute

  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(-ENOENT);
  }

  snprintf(aname, sizeof(aname), XIDT ":%s", inode->ino, name);
  attr = list_search(lfs->attr_index, aname);
  if (attr != NULL) { //** Already exists so just update the info
log_printf(15, "UPDATING fname=%s aname=%s\n", fname, aname);
     if (attr->val != NULL) free (attr->val);
     type_malloc(attr->val, char, v_size);
     memcpy(attr->val, fval, v_size);
     attr->v_size = v_size;
  } else {   //** New entry so insert it
     type_malloc_clear(attr, lio_attr_t, 1);
     type_malloc(attr->val, char, v_size);
     memcpy(attr->val, fval, v_size);
     attr->v_size = v_size;
log_printf(15, "ADDING fname=%s aname=%s p=%p v_size=%d\n", fname, aname, attr, attr->v_size);
     list_insert(lfs->attr_index, strdup(aname), attr);
  }

  attr->recheck_time = apr_time_now() + lfs->xattr_to;
  lfs_unlock(lfs);

  return(0);
}

//*****************************************************************
// lfs_removexattr - Removes an extended attribute
//*****************************************************************

int lfs_removexattr(const char *fname, const char *name)
{
  int v_size, err;
  lio_inode_t *inode;
  lio_attr_t *attr;
  char aname[512];

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "fname=%s attr_name=%s\n", fname, name); flush_log();

  if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) { return(0); }

  v_size = -1;
  err = lioc_set_attr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, NULL, v_size);
  if (err != OP_STATE_SUCCESS) {
     return(-ENOENT);
  }

  //** Also remove it from the table
  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     lfs_unlock(lfs);
     return(0);
  }

  snprintf(aname, sizeof(aname), XIDT ":%s", inode->ino, name);
log_printf(15, "Attempting to remove fname=%s aname=%s\n", fname, aname);

  attr = list_search(lfs->attr_index, aname);
  if (attr != NULL) {
log_printf(15, "REMOVING fname=%s aname=%s\n", fname, aname);
     if (attr->val != NULL) free(attr->val);
     attr->val = NULL;
     attr->v_size = 0;
  }
  lfs_unlock(lfs);

  return(0);
}

//*************************************************************************
// lfs_hardlink - Creates a hardlink to an existing file
//*************************************************************************

int lfs_hardlink(const char *oldname, const char *newname)
{
  lio_inode_t *inode;
  int err;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "oldname=%s newname=%s\n", oldname, newname); flush_log();

  //** Now do the hard link
  err = gop_sync_exec(lio_link_object(lfs->lc, lfs->lc->creds, 0, (char *)oldname, (char *)newname, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     lfs_unlock(lfs);
     return(-EIO);
  }

  //** Update the ref count
  lfs_lock(lfs);
  inode = _lfs_dentry_lookup(lfs, oldname, 0);
  if (inode != NULL) inode->nlinks++;
  lfs_unlock(lfs);

  return(0);
}

//*****************************************************************
//  lfs_readlink - Reads the object symlink
//*****************************************************************

int lfs_readlink(const char *fname, char *buf, size_t bsize)
{
  lio_inode_t *inode;
  int v_size;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(15, "fname=%s\n", fname); flush_log();


  lfs_lock(lfs);

  inode = _lfs_dentry_lookup(lfs, fname, 1);
  if (inode == NULL) {
     return(-ENOENT);
  } else if ((inode->ftype & OS_OBJECT_SYMLINK) == 0) {
     return(-EINVAL);
  }

  v_size = strlen(inode->link);
  if (bsize <= v_size) v_size = bsize-1;
  memcpy(buf, inode->link, v_size);
  buf[v_size] = 0;

  lfs_unlock(lfs);

  return(0);
}

//*****************************************************************
//  lfs_symlink - Makes a symbolic link
//*****************************************************************

int lfs_symlink(const char *link, const char *newname)
{
  const char *link2;
  int err;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  log_printf(1, "link=%s newname=%s\n", link, newname); flush_log();

  //** If the link is an absolute path we need to peel off the mount point to the get attribs to link correctly
  //** We only support symlinks within LFS
  link2 = link;
  if (link[0] == '/') { //** Got an abs symlink
     if (strncmp(link, lfs->mount_point, lfs->mount_point_len) == 0) { //** abs symlink w/in LFS
        link2 = &(link[lfs->mount_point_len]);
     } else {
       log_printf(1, "Oops!  symlink outside LFS mount not supported!  link=%s newname=%s\n", link, newname);
       return(-EFAULT);
     }
  }

  //** Now do the sym link
  err = gop_sync_exec(lio_link_object(lfs->lc, lfs->lc->creds, 1, (char *)link2, (char *)newname, lfs->id));
  if (err != OP_STATE_SUCCESS) {
     return(-EIO);
  }

  return(0);
}

//****************************************************
// lfs_gc_thread - Does inode/dentry cleanup
//****************************************************

void *lfs_gc_thread(apr_thread_t *th, void *data)
{
  lio_fuse_t *lfs = (lio_fuse_t *)data;
  list_iter_t it;
  lio_inode_t *inode;
  lio_dentry_t *entry;
  ex_id_t *ino;
  char date[128];
  char *fname;
  int n_inode, n_dentry, err;
  apr_time_t now, dt, stale, next_run;
  Stack_t *stack;
  useconds_t  usecs;

//return(NULL);

  usecs = 1 * 1000000;

  log_printf(1, "Starting inode/dentry gc thread\n");

  stale = lfs->stale_dt;
  stack = new_stack();

  next_run = apr_time_now();

  lfs_lock(lfs);
  while (lfs->shutdown == 0) {
     now = apr_time_now();
     if (now > next_run) {
        next_run = now + lfs->gc_interval;
        apr_ctime(date, now);
        log_printf(15, "Cleanup iteration starting %s\n", date);

        //** Cycle through and destroy all the stale inodes
        now = apr_time_now();
        it = list_iter_search(lfs->ino_index, NULL, 0);
        while ((err=list_next(&it, (list_key_t **)&ino, (list_data_t **)&inode)) == 0) {
           if ((now > inode->recheck_time) && (inode->fh == NULL)) {
              dt = now - inode->recheck_time;
              if (dt > stale) {
                 push(stack, inode);
              }
           }
        }

        n_inode = stack_size(stack);
        while ((inode = pop(stack)) != NULL) {
           log_printf(1, "Dropping inode=" XIDT "\n", inode->ino);
           _lfs_inode_remove(lfs, inode);
           lfs_inode_destroy(lfs, inode);
        }

        //** Do the same for the dentries
        now = apr_time_now();
        it = list_iter_search(lfs->fname_index, NULL, 0);
        while ((err=list_next(&it, (list_key_t **)&fname, (list_data_t **)&entry)) == 0) {
//log_printf(1, "dentry->fname=%s ref_count=%d recheck=" TT " now=" TT "\n", entry->fname, entry->ref_count, entry->recheck_time, now);
           if ((now > entry->recheck_time) && (entry->ref_count <= 0)) {
              dt = now - entry->recheck_time;
//log_printf(15, "DT=" TT " stale= " TT "\n", dt, stale);
              if (dt > stale) {
//log_printf(1, "STALE dentry->fname=%s ref_count=%d\n", entry->fname, entry->ref_count);
                 push(stack, entry);
              }
           }
        }

        n_dentry = stack_size(stack);
        while ((entry = pop(stack)) != NULL) {
           log_printf(1, "Dropping dentry fname=%s inode=" XIDT " ref_count=%d\n", entry->fname, entry->ino, entry->ref_count);
           _lfs_dentry_remove(lfs, entry);
           lfs_dentry_destroy(lfs, entry);
        }

        log_printf(1, "Destroyed inodes=%d dentry=%d\n", n_inode, n_dentry);
        log_printf(1, "Active inodes=%d dentry=%d\n", list_key_count(lfs->ino_index), list_key_count(lfs->fname_index));
     }

     lfs_unlock(lfs);
     usleep(usecs);
     lfs_lock(lfs);
  }

  lfs_unlock(lfs);

  free_stack(stack, 0);

  log_printf(1, "Exiting inode/dentry gc thread\n");

  return(NULL);
}

//*************************************************************************
// lfs_statfs - Returns the files system size
//*************************************************************************

int lfs_statfs(const char *fname, struct statvfs *fs)
{
  rs_space_t space;
  char *config;

  lio_fuse_t *lfs;
  struct fuse_context *ctx;
  ctx = fuse_get_context();
  if (NULL == ctx || NULL == ctx->private_data)
  {
    log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid");
    return(-EINVAL);
  }else
  {
    lfs = (lio_fuse_t*)ctx->private_data;
  }

  memset(fs, 0, sizeof(struct statvfs));

  log_printf(1, "fname=%s\n", fname);

  //** Get the config
  config = rs_get_rid_config(lfs->lc->rs);

  //** And parse it
  space = rs_space(config);
  free(config);

  fs->f_bsize = 4096;
  fs->f_blocks = space.total_up / 4096;
  fs->f_bfree = space.free_up / 4096;
  fs->f_bavail = fs->f_bfree;
  fs->f_files = 1;
  fs->f_ffree = (ex_off_t)1024*1024*1024*1024*1024;
//  fs->f_favail =
//  fs->f_fsid =
//  fs->f_flag =
  fs->f_namemax = 4096 - 100;

  return(0);
}


//*************************************************************************
//  lio_fuse_init - Creates a lowlevel fuse handle for use
//     Note that this function should be called by FUSE and the return value of this function
//     overwrites the .private_data field of the fuse context. This function returns the
//     lio fuse handle (lio_fuse_t *lfs) on success and NULL on failure.
//
//     This function calls lio_init(...) itself, no need to call it beforehand.
//
//*************************************************************************

void *lfs_init_real(struct fuse_conn_info *conn,
                    const int argc,
                    const char **argv,
                    const char *mount_point)
{
  lio_fuse_t *lfs;
  char *section =  "lfs";
  double n;
  int p;

//#ifdef HAVE_XATTR
//  printf("XATTR found!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
//#endif
  lio_fuse_init_args_t *init_args;
  lio_fuse_init_args_t real_args;
  // Retrieve the fuse_context, the last argument of fuse_main(...) is passed in the private_data field for use as a generic user arg. We pass the mount point in it.
  struct fuse_context *ctx;
  if ((argc == 0) && (argv == NULL) && (mount_point == NULL))
  {
    // I really don't get what indentation scheme's being followed here
    // --AMM 9/20/13
    ctx = fuse_get_context();
    if (NULL == ctx || NULL == ctx->private_data)
    {
        log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid. (Hint: last arg of fuse_main(...) must be lio_fuse_init_args_t* and have the mount point set)");
        return(NULL); //TODO: what is the best way to signal failure in the init function? Note that the return value of this function overwrites the .private_data field of the fuse context
    }else
    {
      init_args = (lio_fuse_init_args_t*)ctx->private_data;
    }
  }
  else
  {
    // We weren't called by fuse, so the args are function arguments
    // AMM - 9/23/13
    init_args = &real_args;
    init_args->lio_argc = argc;
    init_args->lio_argv = argv;
    init_args->mount_point = mount_point;
  }
  lio_init(&init_args->lio_argc, &init_args->lio_argv); //This sets the global lio_gc, it also uses a reference count to safely handle extra calls to init
  init_args->lc = lio_gc;

  log_printf(15, "START mount=%s\n", init_args->mount_point);

  //** See if we need to change the CWD
  if (init_args->lio_argc > 1) {
     if (strcmp(init_args->lio_argv[1], "-C") == 0) {
        if (chdir(init_args->lio_argv[2]) != 0) {
           fprintf(stderr, "ERROR setting CWD=%s.  errno=%d\n", init_args->lio_argv[2], errno);
           log_printf(0, "ERROR setting CWD=%s.  errno=%d\n", init_args->lio_argv[2], errno);
        } else {
           log_printf(0, "Setting CWD=%s\n", init_args->lio_argv[2]);
        }
     }
  }

  type_malloc_clear(lfs, lio_fuse_t, 1);

  lfs->lc = init_args->lc;
  lfs->mount_point = strdup(init_args->mount_point);
  lfs->mount_point_len = strlen(init_args->mount_point);

  lfs->entry_to = APR_USEC_PER_SEC * inip_get_double(lfs->lc->ifd, section, "entry_timout", 10.0);
  lfs->attr_to = APR_USEC_PER_SEC * inip_get_double(lfs->lc->ifd, section, "stat_timeout", 10.0);
  lfs->inode_cache_size = inip_get_integer(lfs->lc->ifd, section, "inode_cache_size", 1000000);
  lfs->xattr_to = APR_USEC_PER_SEC * inip_get_integer(lfs->lc->ifd, section, "xattr_timeout", 60);
  lfs->stale_dt = APR_USEC_PER_SEC * inip_get_integer(lfs->lc->ifd, section, "stale_timeout", 60);
  lfs->gc_interval = APR_USEC_PER_SEC * inip_get_integer(lfs->lc->ifd, section, "gc_interval", 60);
  lfs->file_count = inip_get_integer(lfs->lc->ifd, section, "file_lock_size", 1000);
  lfs->enable_tape = inip_get_integer(lfs->lc->ifd, section, "enable_tape", 0);
  lfs->readahead = inip_get_integer(lfs->lc->ifd, section, "readahead", 0);
  lfs->readahead_trigger = lfs->readahead * inip_get_double(lfs->lc->ifd, section, "readahead_trigger", 1.0);

  n = lfs->inode_cache_size;
  p = log2(n) + 3;
  lfs->ino_index = create_skiplist_full(p, 0.5, 0, &ino_compare, NULL, NULL, NULL);
  lfs->fname_index = create_skiplist_full(p, 0.5, 0, &list_string_compare, NULL, NULL, NULL);
  lfs->attr_index = create_skiplist_full(p, 0.5, 0, &list_string_compare, NULL, list_simple_free, lfs_attr_free);

  apr_pool_create(&(lfs->mpool), NULL);
  apr_thread_mutex_create(&(lfs->lock), APR_THREAD_MUTEX_DEFAULT, lfs->mpool);
  thread_create_assert(&(lfs->gc_thread), NULL, lfs_gc_thread, (void *)lfs, lfs->mpool);

  //** Make the cond table
  type_malloc_clear(lfs->file_lock, apr_thread_mutex_t *, lfs->file_count);
  for (p=0; p<lfs->file_count; p++) {
     apr_thread_mutex_create(&(lfs->file_lock[p]), APR_THREAD_MUTEX_DEFAULT, lfs->mpool);
  }

  //** Get the default host ID for opens
  char hostname[1024];
  apr_gethostname(hostname, sizeof(hostname), lfs->mpool);
  lfs->id = strdup(hostname);

  // TODO: find a cleaner way to get fops here
  //lfs->fops = ctx->fuse->fuse_fs->op;
  lfs->fops = lfs_fops;

  log_printf(15, "END\n");

  return(lfs); //
}
void *lfs_init(struct fuse_conn_info *conn)
{
  return lfs_init_real(conn,0,NULL,NULL);
}
//*************************************************************************
// lio_fuse_destroy - Destroy a fuse object
//
//    (handles shuting down lio as appropriate, no need to call lio_shutdown() externally)
//
//*************************************************************************

void lfs_destroy(void *private_data)
{
  list_iter_t it;
  ex_id_t *ino;
  lio_inode_t *inode;
  lio_dentry_t *entry;
  char *fname;
  int err, i;
  apr_status_t value;
  lio_fuse_t *lfs;

  log_printf(0, "shutting down\n"); flush_log();

  lfs = (lio_fuse_t*)private_data;
  if (lfs == NULL){
    log_printf(0,"lio_fuse_destroy: Error, the lfs handle is null, unable to shutdown cleanly. Perhaps lfs creation failed?");
    return;
  }

  //** Shut down the RW thread
  apr_thread_mutex_lock(lfs->lock);
  lfs->shutdown=1;
  apr_thread_mutex_unlock(lfs->lock);

  apr_thread_join(&value, lfs->gc_thread);  //** Wait for it to complete

  //** Cycle through and destroy all the inodes
  it = list_iter_search(lfs->ino_index, NULL, 0);
  while ((err=list_next(&it, (list_key_t **)&ino, (list_data_t **)&inode)) == 0) {
log_printf(0, "destroying ino=" XIDT "\n", *ino); flush_log();
     lfs_inode_destroy(lfs, inode);
  }
  list_destroy(lfs->ino_index);

  //** Do the same for the dentries
  it = list_iter_search(lfs->fname_index, NULL, 0);
  while ((err=list_next(&it, (list_key_t **)&fname, (list_data_t **)&entry)) == 0) {
log_printf(0, "destroying entry=%s\n", fname); flush_log();
     lfs_dentry_destroy(lfs, entry);
  }
  list_destroy(lfs->fname_index);

  //** ..and the attributes
  list_destroy(lfs->attr_index);

  //** Clean up the file cond table
  for (i=0; i<lfs->file_count; i++) {
     apr_thread_mutex_destroy(lfs->file_lock[i]);
  }
  free(lfs->file_lock);


  //** Clean up everything else
  if (lfs->id != NULL) free (lfs->id);
  free(lfs->mount_point);
  apr_thread_mutex_destroy(lfs->lock);
  apr_pool_destroy(lfs->mpool);

  free(lfs);

  lio_shutdown(); // Reference counting in this function protects against shutdown if lio is still in use elsewhere
}

struct fuse_operations lfs_fops = { //All lfs instances should use the same functions so statically initialize
  .init = lfs_init,
  .destroy = lfs_destroy,

  .opendir = lfs_opendir,
  .releasedir = lfs_closedir,
  .readdir = lfs_readdir,
  .getattr = lfs_stat,
  .utimens = lfs_utimens,
  .truncate = lfs_truncate,
  .rename = lfs_rename,
  .mknod = lfs_mknod,
  .mkdir = lfs_mkdir,
  .unlink = lfs_unlink,
  .rmdir = lfs_rmdir,
  .open = lfs_open,
  .release = lfs_release,
  .read = lfs_read,
//  .read = lfs_read_test_ex,
  .write = lfs_write,
  .flush = lfs_flush,
  .link = lfs_hardlink,
  .readlink = lfs_readlink,
  .symlink = lfs_symlink,
  .statfs = lfs_statfs,

#ifdef HAVE_XATTR
  .listxattr = lfs_listxattr,
  .getxattr = lfs_getxattr,
  .setxattr = lfs_setxattr,
  .removexattr = lfs_removexattr,
#endif
};

