/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#define _log_module_index 212

#include "config.h"

#include <apr_hash.h>
#include <apr_network_io.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <lio/segment.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/types.h"
#include "lio.h"
#include "lio_fuse.h"
#include "os.h"
#include "rs.h"

#if defined(HAVE_SYS_XATTR_H)
#include <sys/xattr.h>
#elif defined(HAVE_ATTR_XATTR_H)
#include <attr/xattr.h>
#endif


//#define lfs_lock(lfs)  log_printf(0, "lfs_lock\n"); tbx_log_flush(); apr_thread_mutex_lock((lfs)->lock)
//#define lfs_unlock(lfs) log_printf(0, "lfs_unlock\n");  tbx_log_flush(); apr_thread_mutex_unlock((lfs)->lock)
#define lfs_lock(lfs)    apr_thread_mutex_lock((lfs)->lock)
#define lfs_unlock(lfs)  apr_thread_mutex_unlock((lfs)->lock)

#define _inode_key_size 11
#define _inode_fuse_attr_start 7
static char *_inode_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count", "os.link",
                               "security.selinux",  "system.posix_acl_access", "system.posix_acl_default", "security.capability"
                             };

#define _tape_key_size  2
static char *_tape_keys[] = { "system.owner", "system.exnode" };

typedef struct {
    char *dentry;
    struct stat stat;
} lfs_dir_entry_t;

typedef struct {
    char *fname;
    ex_id_t sid;
    int ref_count;
    int remove_on_close;
}  lio_fuse_open_file_t;

typedef struct {
    lio_fuse_t *lfs;
    os_object_iter_t *it;
    lio_os_regex_table_t *path_regex;
    char *val[_inode_key_size];
    int v_size[_inode_key_size];
    char *dot_path;
    char *dotdot_path;
    tbx_stack_t *stack;
    int state;
} lfs_dir_iter_t;

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t vid);


//*************************************************************************
// ftype_lio2fuse - Converts a LIO filetype to fuse
//*************************************************************************

mode_t ftype_lio2fuse(int ftype)
{
    mode_t mode;

    if (ftype & OS_OBJECT_SYMLINK_FLAG) {
        mode = S_IFLNK | 0777;
    } else if (ftype & OS_OBJECT_DIR_FLAG) {
        mode = S_IFDIR | 0755;
    } else {
//     mode = S_IFREG | 0444;
        mode = S_IFREG | 0666;  //** Make it so that everything has RW access
    }

    return(mode);
}

//*************************************************************************
// _lfs_parse_inode_vals - Parses the inode values received
//   NOTE: All the val[*] strings are free'ed!
//*************************************************************************

void _lfs_parse_stat_vals(lio_fuse_t *lfs, char *fname, struct stat *stat, char **val, int *v_size)
{
    int i, n, readlink;
    lio_fuse_open_file_t *fop;
    ex_id_t ino;
    char *link;
    lio_file_handle_t *fh;
    ex_off_t len;
    int ts;

    ino = 0;
    if (val[0] != NULL) {
        sscanf(val[0], XIDT, &ino);
    } else {
        generate_ex_id(&ino);
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT "\n", ino);
    }
    stat->st_ino = ino;

    //** Modify TS's
    ts = 0;
    if (val[1] != NULL) lio_get_timestamp(val[1], &ts, NULL);
    stat->st_mtime = ts;
    ts = 0;
    if (val[2] != NULL) lio_get_timestamp(val[2], &ts, NULL);
    stat->st_ctime = ts;
    stat->st_atime = stat->st_ctime;

    //** Get the symlink if it exists
    readlink = 0;
    if (val[6] != NULL) {
        link = val[6];
        readlink = strlen(link);
        log_printf(15, "inode->link=%s mount_point=%s moun_point_len=%d\n", link, lfs->mount_point, lfs->mount_point_len);
        if (link[0] == '/') { //** IF an absolute link then we need to add the mount prefix back
            readlink += lfs->mount_point_len + 1;
        }
    }

    //** File types
    n = 0;
    if (val[4] != NULL) sscanf(val[4], "%d", &n);
    stat->st_mode = ftype_lio2fuse(n);

    //** Size
    ino = 0;
    lfs_lock(lfs);
    fop = apr_hash_get(lfs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop != NULL) ino = fop->sid;
    lfs_unlock(lfs);

    len = 0;
    if (val[3] != NULL) sscanf(val[3], XOT, &len);
    if (ino != 0) { //** Got an open file
        lio_lock(lfs->lc);
        fh = _lio_get_file_handle(lfs->lc, ino);
        if (fh) len = segment_size(fh->seg);
        lio_unlock(lfs->lc);
    }

    stat->st_size = (n & OS_OBJECT_SYMLINK_FLAG) ? readlink : len;
    stat->st_blksize = 4096;
    stat->st_blocks = stat->st_size / 512;
    if (stat->st_size < 1024) stat->st_blksize = 1024;

    //** N-links
    n = 0;
    if (val[5] != NULL) sscanf(val[5], "%d", &n);
    stat->st_nlink = n;

    //** All the various security ACLs that Linux likes to check we just ignore
    //** By fetching them with everything else we've preloaded the OS cache
    //** so the subsequent call by FUSE will pull from cache instead of remote.

    //** Clean up
    for (i=0; i<_inode_key_size; i++) {
        if (val[i] != NULL) free(val[i]);
    }
}

//*************************************************************************
// lfs_get_context - Returns the LFS context.  If none is available it aborts
//*************************************************************************

lio_fuse_t *lfs_get_context()
{
    lio_fuse_t *lfs;
    struct fuse_context *ctx;
    ctx = fuse_get_context();

   FATAL_UNLESS(NULL != ctx);

    lfs = (lio_fuse_t*)ctx->private_data;
   FATAL_UNLESS(NULL != lfs);

    return(lfs);
}

//*************************************************************************
// lfs_stat - Does a stat on the file/dir
//*************************************************************************

int lfs_stat(const char *fname, struct stat *stat, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    char *val[_inode_key_size];
    int v_size[_inode_key_size], i, err;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    for (i=0; i<_inode_key_size; i++) v_size[i] = -lfs->lc->max_attr;
    err = lio_get_multiple_attrs(lfs->lc, lfs->lc->creds, fname, NULL, _inode_keys, (void **)val, v_size, _inode_key_size);

    if (err != OP_STATE_SUCCESS) {
        return(-ENOENT);
    }
    _lfs_parse_stat_vals(lfs, (char *)fname, stat, val, v_size);

    log_printf(1, "END fname=%s err=%d\n", fname, err);
    tbx_log_flush();

    return(0);
}

//*************************************************************************
// lfs_closedir - Closes the opendir file handle
//*************************************************************************

int lfs_closedir_real(lfs_dir_iter_t *dit)
{
    lfs_dir_entry_t *de;

    if (dit == NULL) return(-EBADF);

    if (dit->dot_path) free(dit->dot_path);
    if (dit->dotdot_path) free(dit->dotdot_path);

    if (dit->stack) {
        //** Cyle through releasing all the entries
        while ((de = (lfs_dir_entry_t *)tbx_stack_pop(dit->stack)) != NULL) {
            log_printf(0, "fname=%s\n", de->dentry);
            tbx_log_flush();
            free(de->dentry);
            free(de);
        }

        tbx_stack_free(dit->stack, 0);
    }

    if (dit->it) lio_destroy_object_iter(dit->lfs->lc, dit->it);
    lio_os_regex_table_destroy(dit->path_regex);
    free(dit);

    return(0);
}

int lfs_closedir(const char *fname, struct fuse_file_info *fi)
{
    lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;

    return(lfs_closedir_real(dit));
}

//*************************************************************************
// lfs_opendir - FUSE opendir call
//*************************************************************************

int lfs_opendir(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lfs_dir_iter_t *dit;
    char path[OS_PATH_MAX];
    char *dir, *file;
    lfs_dir_entry_t *de, *de2;
    int i;
    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    tbx_type_malloc_clear(dit, lfs_dir_iter_t, 1);

    for (i=0; i<_inode_key_size; i++) {
        dit->v_size[i] = -lfs->lc->max_attr;
        dit->val[i] = NULL;
    }

    dit->lfs = lfs;
    snprintf(path, OS_PATH_MAX, "%s/*", fname);
    dit->path_regex = lio_os_path_glob2regex(path);

    dit->it = lio_create_object_iter_alist(dit->lfs->lc, dit->lfs->lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY_FLAG, 0, _inode_keys, (void **)dit->val, dit->v_size, _inode_key_size);
    if (dit->it == NULL) {
        lfs_closedir_real(dit);
        return(-ENOENT);
    }

    dit->stack = tbx_stack_new();

    dit->state = 0;

    //** Add "."
    dit->dot_path = strdup(fname);
    tbx_type_malloc(de, lfs_dir_entry_t, 1);
    if (lfs_stat(fname, &(de->stat), NULL) != 0) {
        lfs_closedir_real(dit);
        free(de);
        return(-ENOENT);
    }
    de->dentry = strdup(".");
    tbx_stack_insert_below(dit->stack, de);

    //** And ".."
    if (strcmp(fname, "/") != 0) {
        lio_os_path_split((char *)fname, &dir, &file);
        dit->dotdot_path = dir;
        free(file);
    } else {
        dit->dotdot_path = strdup(fname);
    }

    log_printf(1, "dot=%s dotdot=%s\n", dit->dot_path, dit->dotdot_path);

    tbx_type_malloc(de2, lfs_dir_entry_t, 1);
    if (lfs_stat(dit->dotdot_path, &(de2->stat), NULL) != 0) {
        lfs_closedir_real(dit);
        free(de2);
        return(-ENOENT);
    }
    de2->dentry = strdup("..");
    tbx_stack_insert_below(dit->stack, de2);

    //** Compose our reply
    fi->fh = (uint64_t)dit;
    return(0);
}

//*************************************************************************
// lfs_readdir - Returns the next file in the directory
//*************************************************************************

int lfs_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    lfs_dir_iter_t *dit= (lfs_dir_iter_t *)fi->fh;
    lfs_dir_entry_t *de;
    int ftype, prefix_len, n, i;
    char *fname;
    struct stat stbuf;
    apr_time_t now;
    double dt;
    int off2 = off;

    log_printf(1, "dname=%s off=%d stack_size=%d\n", dname, off2, tbx_stack_count(dit->stack));
    tbx_log_flush();
    now = apr_time_now();

    if (dit == NULL) {
        return(-EBADF);
    }

    off++;  //** This is the *next* slot to get where the stack top is off=1

    memset(&stbuf, 0, sizeof(stbuf));
    n = tbx_stack_count(dit->stack);
    if (n>=off) { //** Rewind
        tbx_stack_move_to_bottom(dit->stack);  //** Go from the bottom up.
        for (i=n; i>off; i--) tbx_stack_move_up(dit->stack);

        de = tbx_stack_get_current_data(dit->stack);
        while (de != NULL) {
            if (filler(buf, de->dentry, &(de->stat), off, FUSE_FILL_DIR_PLUS) == 1) {
                dt = apr_time_now() - now;
                dt /= APR_USEC_PER_SEC;
                log_printf(1, "dt=%lf\n", dt);
                return(0);
            }

            off++;
            tbx_stack_move_down(dit->stack);
            de = tbx_stack_get_current_data(dit->stack);
        }
    }

    log_printf(15, "dname=%s switching to iter\n", dname);

    for (;;) {
        //** If we made it here then grab the next file and look it up.
        ftype = lio_next_object(dit->lfs->lc, dit->it, &fname, &prefix_len);
        if (ftype <= 0) { //** No more files
            dt = apr_time_now() - now;
            dt /= APR_USEC_PER_SEC;
            off2=off;
            if (ftype == 0) {
                log_printf(15, "dname=%s NOTHING LEFT off=%d dt=%lf\n", dname,off2, dt);
            } else {
                log_printf(15, "dname=%s ERROR getting next object off=%d dt=%lf\n", dname,off2, dt);
            }
            return((ftype == 0) ? 0 : -EIO);
        }

        tbx_type_malloc(de, lfs_dir_entry_t, 1);
        de->dentry = strdup(fname+prefix_len+1);
        _lfs_parse_stat_vals(dit->lfs, fname, &(de->stat), dit->val, dit->v_size);
        free(fname);
        log_printf(1, "next fname=%s ftype=%d prefix_len=%d ino=" XIDT " off=" XOT "\n", de->dentry, ftype, prefix_len, de->stat.st_ino, off);

        tbx_stack_move_to_bottom(dit->stack);
        tbx_stack_insert_below(dit->stack, de);

        if (filler(buf, de->dentry, &(de->stat), off, FUSE_FILL_DIR_PLUS) == 1) {
            dt = apr_time_now() - now;
            dt /= APR_USEC_PER_SEC;
            log_printf(15, "BUFFER FULL dt=%lf\n", dt);
            return(0);
        }

        off++;
    }

    return(0);
}


//*************************************************************************
// lfs_object_create
//*************************************************************************

int lfs_object_create(lio_fuse_t *lfs, const char *fname, mode_t mode, int ftype)
{
    char fullname[OS_PATH_MAX];
    int err, n;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    //** Make sure it doesn't exists
    n = lio_exists(lfs->lc, lfs->lc->creds, (char *)fname);
    if (n != 0) {  //** File already exists
        log_printf(15, "File already exist! fname=%s\n", fullname);
        return(-EEXIST);
    }

    //** If we made it here it's a new file or dir
    //** Create the new object
    err = gop_sync_exec(lio_create_gop(lfs->lc, lfs->lc->creds, (char *)fname, ftype, NULL, lfs->id));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error creating object! fname=%s\n", fullname);
        if (strlen(fullname) > 3900) {  //** Probably a path length issue
            return(-ENAMETOOLONG);
        }
        return(-EREMOTEIO);
    }

    return(0);
}

//*************************************************************************
// lfs_mknod - Makes a regular file
//*************************************************************************

int lfs_mknod(const char *fname, mode_t mode, dev_t rdev)
{
    lio_fuse_t *lfs = lfs_get_context();
    return(lfs_object_create(lfs, fname, mode, OS_OBJECT_FILE_FLAG));
}


//*************************************************************************
// lfs_mkdir - Makes a directory
//*************************************************************************

int lfs_mkdir(const char *fname, mode_t mode)
{
    lio_fuse_t *lfs = lfs_get_context();
    return(lfs_object_create(lfs, fname, mode, OS_OBJECT_DIR_FLAG));
}

//*****************************************************************
// lfs_actual_remove - Does the actual removal
//*****************************************************************

int lfs_actual_remove(lio_fuse_t *lfs, const char *fname, int ftype)
{
    int err;
    err = gop_sync_exec(lio_remove_gop(lfs->lc, lfs->lc->creds, (char *)fname, NULL, 0));

    log_printf(1, "remove err=%d\n", err);
    if (err == OP_STATE_SUCCESS) {
        return(0);
    } else if ((ftype & OS_OBJECT_DIR_FLAG) > 0) { //** Most likey the dirs not empty
        return(-ENOTEMPTY);
    }

    return(-EREMOTEIO);  //** Probably an expired exnode but through an error anyway
}

//*****************************************************************
//  lfs_object_remove - Removes a file or directory
//*****************************************************************

int lfs_object_remove(lio_fuse_t *lfs, const char *fname)
{
    lio_fuse_open_file_t *fop;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    //** Check if it's open.  If so do a delayed removal
    lfs_lock(lfs);
    fop = apr_hash_get(lfs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop != NULL) {
        fop->remove_on_close = 1;
        return(0);
    }
    lfs_unlock(lfs);

    return(lfs_actual_remove(lfs, fname, 0));
}


//*****************************************************************
//  lfs_unlink - Remove a file
//*****************************************************************

int lfs_unlink(const char *fname)
{
    lio_fuse_t *lfs = lfs_get_context();
    return(lfs_object_remove(lfs, fname));
}

//*****************************************************************
//  lfs_rmdir - Remove a directory
//*****************************************************************

int lfs_rmdir(const char *fname)
{
    lio_fuse_t *lfs = lfs_get_context();
    return(lfs_object_remove(lfs, fname));
}

//*****************************************************************
// lfs_open - Opens a file for I/O
//*****************************************************************

int lfs_open(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd;
    lio_fuse_open_file_t *fop;
    int mode;

    mode = 0;
    if (fi->flags & O_RDONLY) {
        mode = LIO_READ_MODE;
    } else if (fi->flags & O_WRONLY) {
        mode = LIO_WRITE_MODE;
    } else if (fi->flags & O_RDWR) {
        mode = LIO_READ_MODE | LIO_WRITE_MODE;
    }

    if (fi->flags & O_APPEND) mode |= LIO_APPEND_MODE;
    if (fi->flags & O_CREAT) mode |= LIO_CREATE_MODE;
    if (fi->flags & O_TRUNC) mode |= LIO_TRUNCATE_MODE;

    fi->fh = 0;
    gop_sync_exec(lio_open_gop(lfs->lc, lfs->lc->creds, (char *)fname, mode, NULL, &fd, 60));
    log_printf(2, "fname=%s fd=%p\n", fname, fd);
    if (fd == NULL) {
        log_printf(0, "Failed opening file!  path=%s\n", fname);
        return(-EREMOTEIO);
    }

    fi->fh = (uint64_t)fd;

    lfs_lock(lfs);
    fop = apr_hash_get(lfs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop == NULL) {
        tbx_type_malloc_clear(fop, lio_fuse_open_file_t, 1);
        fop->fname = strdup(fd->path);
        fop->sid = segment_id(fd->fh->seg);
        apr_hash_set(lfs->open_files, fop->fname, APR_HASH_KEY_STRING, fop);
    }
    fop->ref_count++;
    lfs_unlock(lfs);

    //** See if we have WQ enabled
    if (lfs->n_merge > 0) lio_wq_enable(fd, lfs->n_merge);

    return(0);
}

//*****************************************************************
// lfs_release - Closes a file
//*****************************************************************

int lfs_release(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    lio_fuse_open_file_t *fop;
    int err, remove_on_close;

    log_printf(2, "fname=%s fd->path=%s fd=%p\n", fname, fd->path, fd);

    remove_on_close = 0;

    lfs_lock(lfs);
    fop = apr_hash_get(lfs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop) {
        remove_on_close = fop->remove_on_close;
        fop->ref_count--;
        if (fop->ref_count <= 0) {  //** LAst one so remove it.
            apr_hash_set(lfs->open_files, fname, APR_HASH_KEY_STRING, NULL);
            free(fop->fname);
            free(fop);
        }
    }
    lfs_unlock(lfs);

    //** See if we need to remove it
    if (remove_on_close == 1) {
        segment_lock(fd->fh->seg);
        fd->fh->remove_on_close = 1;
        segment_unlock(fd->fh->seg);
    }

    //** Check if a rename occurred during file open
    if (strcmp(fname, fd->path) != 0) {
        free(fd->path);
        fd->path = strdup(fname);
    }

    err = gop_sync_exec(lio_close_gop(fd)); // ** Close it but keep track of the error

    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "Failed closing file!  path=%s\n", fname);
        return(-EREMOTEIO);
    }

    return(0);
}

//*****************************************************************
// lfs_read - Reads data from a file
//    NOTE: Uses the LFS readahead hints
//*****************************************************************

int lfs_read(const char *fname, char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd;
    ex_off_t nbytes;
    apr_time_t now;
    double dt;

    ex_off_t t1, t2;
    t1 = size;
    t2 = off;

    fd = (lio_fd_t *)fi->fh;
    log_printf(1, "fname=%s size=" XOT " off=" XOT " fd=%p\n", fname, t1, t2, fd);
    tbx_log_flush();
    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null file desriptor\n");
        return(-EBADF);
    }

    now = apr_time_now();

    //** Do the read op
    nbytes = lio_read(fd, buf, size, off, lfs->rw_hints);

    if (tbx_log_level() > 0) {
        t2 = size+off-1;
        log_printf(1, "LFS_READ:START " XOT " %zu\n", off, size);
        log_printf(1, "LFS_READ:END " XOT "\n", t2);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s seg=" XIDT " size=" XOT " off=%zu nbytes=" XOT " dt=%lf\n", fname, segment_id(fd->fh->seg), t1, t2, nbytes, dt);
    tbx_log_flush();

    return(nbytes);
}

//*****************************************************************
// lfs_write - Writes data to a file
//*****************************************************************

int lfs_write(const char *fname, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    ex_off_t nbytes;
    lio_fd_t *fd;

    fd = (lio_fd_t *)fi->fh;

    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null LFS handle\n");
        return(-EBADF);
    }

    //** Do the write op
    nbytes = lio_write(fd, (char *)buf, size, off, lfs->rw_hints);
    return(nbytes);
}

//*****************************************************************
// lfs_flush - Flushes any data to backing store
//*****************************************************************

int lfs_flush(const char *fname, struct fuse_file_info *fi)
{
    lio_fd_t *fd;
    int err;
    apr_time_t now;
    double dt;

    now = apr_time_now();

    log_printf(1, "START fname=%s\n", fname);
    tbx_log_flush();

    fd = (lio_fd_t *)fi->fh;
    if (fd == NULL) {
        return(-EBADF);
    }

    err = gop_sync_exec(segment_flush(fd->fh->seg, fd->fh->lc->da, 0, segment_size(fd->fh->seg)+1, fd->fh->lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        return(-EIO);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s dt=%lf\n", fname, dt);
    tbx_log_flush();

    return(0);
}

//*****************************************************************
// lfs_fsync - Flushes any data to backing store
//*****************************************************************

int lfs_fsync(const char *fname, int datasync, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd;
    int err;
    apr_time_t now;
    double dt;

    now = apr_time_now();

    fd = (lio_fd_t *)fi->fh;
    log_printf(1, "START fname=%s fd=%p\n", fname, fd);
    tbx_log_flush();
    if (fd == NULL) {
        return(-EBADF);
    }

    err = gop_sync_exec(segment_flush(fd->fh->seg, fd->fh->lc->da, 0, segment_size(fd->fh->seg)+1, lfs->lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        return(-EIO);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s dt=%lf\n", fname, dt);
    tbx_log_flush();

    return(0);
}

//*************************************************************************
// lfs_rename - Renames a file
//*************************************************************************

int lfs_rename(const char *oldname, const char *newname, unsigned int flags)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fuse_open_file_t *fop;
    gop_op_status_t status;

    log_printf(1, "oldname=%s newname=%s flags=%ud\n", oldname, newname, flags);
    tbx_log_flush();

    if (flags) return(-EINVAL);  //** We don't currently do anything with the flags
    
    lfs_lock(lfs);
    fop = apr_hash_get(lfs->open_files, oldname, APR_HASH_KEY_STRING);
    if (fop) {  //** Got an open file so need to mve the entry there as well.
        apr_hash_set(lfs->open_files, oldname, APR_HASH_KEY_STRING, NULL);
        free(fop->fname);
        fop->fname = strdup(newname);
        apr_hash_set(lfs->open_files, fop->fname, APR_HASH_KEY_STRING, fop);
    }
    lfs_unlock(lfs);

    //** Do the move
    status = gop_sync_exec_status(lio_move_object_gop(lfs->lc, lfs->lc->creds, (char *)oldname, (char *)newname));
    if (status.op_status != OP_STATE_SUCCESS) {
        return((status.error_code != 0) ? -status.error_code : -EREMOTEIO);
    }

    return(0);
}


//*****************************************************************
// lfs_truncate - Truncate the file
//*****************************************************************

int lfs_truncate(const char *fname, off_t new_size)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd;
    ex_off_t ts;
    int result;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    ts = new_size;
    log_printf(15, "adjusting size=" XOT "\n", ts);

    gop_sync_exec(lio_open_gop(lfs->lc, lfs->lc->creds, (char *)fname, LIO_RW_MODE, NULL, &fd, 60));
    if (fd == NULL) {
        log_printf(0, "Failed opening file!  path=%s\n", fname);
        return(-EIO);
    }

    result = 0;
    if (gop_sync_exec(lio_truncate_gop(fd, new_size)) != OP_STATE_SUCCESS) {
        log_printf(0, "Failed truncating file!  path=%s\n", fname);
        result = -EIO;
    }

    if (gop_sync_exec(lio_close_gop(fd)) != OP_STATE_SUCCESS) {
        log_printf(0, "Failed closing file!  path=%s\n", fname);
        result = -EIO;
    }

    return(result);
}

//*****************************************************************
// lfs_ftruncate - Truncate the file associated with the FD
//*****************************************************************

int lfs_ftruncate(const char *fname, off_t new_size, struct fuse_file_info *fi)
{
    lio_fd_t *fd;
    int err;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    if (fi == NULL) return(lfs_truncate(fname, new_size));
    
    fd = (lio_fd_t *)fi->fh;
    if (fd == NULL) {
        return(-EBADF);
    }

    err = gop_sync_exec(lio_truncate_gop(fd, new_size));

    return((err == OP_STATE_SUCCESS) ? 0 : -EIO);
}


//*****************************************************************
// lfs_utimens - Sets the access and mod times in ns
//*****************************************************************

int lfs_utimens(const char *fname, const struct timespec tv[2], struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    char buf[1024];
    char *key;
    char *val;
    int v_size;
    ex_off_t ts;
    int err;


    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    key = "system.modify_attr";
    ts = tv[1].tv_sec;
    snprintf(buf, 1024, XOT "|%s", ts, lfs->id);
    val = buf;
    v_size = strlen(buf);

//  key = "os.timestamp.system.modify_attr";
//  val = lfs->id;
//  v_size = strlen(val);

    err = lio_setattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, key, (void *)val, v_size);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating stat! fname=%s\n", fname);
        return(-EBADE);
    }

    return(0);
}

//*****************************************************************
// lfs_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

int lfs_listxattr(const char *fname, char *list, size_t size)
{
    lio_fuse_t *lfs = lfs_get_context();
    char *buf, *key, *val;
    int bpos, bufsize, v_size, n, i, err;
    lio_os_regex_table_t *attr_regex;
    os_attr_iter_t *it;
    os_fd_t *fd;

    bpos= size;
    log_printf(1, "fname=%s size=%d\n", fname, bpos);
    tbx_log_flush();

    //** Make an iterator
    attr_regex = lio_os_path_glob2regex("user.*");
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
    tbx_type_malloc_clear(buf, char, bufsize);
    val = NULL;
    bpos = 0;

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
    lio_os_regex_table_destroy(attr_regex);

    log_printf(15, "bpos=%d size=%zu buf=%s\n", bpos, size, buf);

    if (size == 0) {
        log_printf(15, "SIZE bpos=%d buf=%s\n", bpos, buf);
    } else if ((int)size > bpos) {
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
    lio_exnode_exchange_t *exp;
    lio_exnode_t *ex, *cex;

    tbx_type_malloc(tape_val, char, tape_size+1);
    memcpy(tape_val, mytape_val, tape_size);
    tape_val[tape_size] = 0;  //** Just to be safe with the string/prints routines

    log_printf(15, "fname=%s tape_size=%d\n", fname, tape_size);
    log_printf(15, "Tape attribute follows:\n%s\n", tape_val);

    ftype = lio_exists(lfs->lc, lfs->lc->creds, fname);
    if (ftype <= 0) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        return;
    }

    nkeys = (ftype & OS_OBJECT_SYMLINK_FLAG) ? 1 : _tape_key_size;

    //** The 1st key should be n_keys
    tmp = tbx_stk_string_token(tape_val, "=\n", &bstate, &fin);
    if (strcmp(tmp, "n_keys") != 0) { //*
        log_printf(0, "ERROR parsing tape attribute! Missing n_keys! fname=%s\n", fname);
        log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
        free(tape_val);
        return;
    }

    n = -1;
    sscanf(tbx_stk_string_token(NULL, "=\n", &bstate, &fin), "%d", &n);
    log_printf(15, "fname=%s n=%d nkeys=%d ftype=%d\n", fname, n, nkeys, ftype);
    if (n != nkeys) {
        log_printf(0, "ERROR parsing n_keys size fname=%s\n", fname);
        log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
        free(tape_val);
        return;
    }

    log_printf(15, "nkeys=%d fname=%s ftype=%d\n", nkeys, fname, ftype);

    //** Set all of them to 0 cause the size is used to see if the key was loaded
    for (i=0; i<_tape_key_size; i++) {
        v_size[i] = 0;
    }

    //** Parse the sizes
    for (i=0; i<nkeys; i++) {
        tmp = tbx_stk_string_token(NULL, "=\n", &bstate, &fin);
        if (strcmp(tmp, _tape_keys[i]) == 0) {
            sscanf(tbx_stk_string_token(NULL, "=\n", &bstate, &fin), "%d", &(v_size[i]));
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
            tbx_type_malloc(val[i], char, v_size[i]+1);
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
        exp = lio_exnode_exchange_text_parse(val[ex_key]);
        ex = lio_exnode_create();
        err = lio_exnode_deserialize(ex, exp, lfs->lc->ess_nocache);
        exnode_exchange_free(exp);
        val[ex_key] = NULL;

        if (err != 0) {
            log_printf(1, "ERROR parsing parent exnode fname=%s\n", fname);
            lio_exnode_exchange_destroy(exp);
            lio_exnode_destroy(ex);
        }

        //** Execute the clone operation
        err = gop_sync_exec(lio_exnode_clone_gop(lfs->lc->tpc_unlimited, ex, lfs->lc->da, &cex, NULL, CLONE_STRUCTURE, lfs->lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR cloning parent fname=%s\n", fname);
        }

        //** Serialize it for storage
        lio_exnode_serialize(cex, exp);
        val[ex_key] = exp->text.text;
        v_size[ex_key] = strlen(val[ex_key]);
        exp->text.text = NULL;
        lio_exnode_exchange_destroy(exp);
        lio_exnode_destroy(ex);
        lio_exnode_destroy(cex);
    }

    //** Store them
    err = lio_multiple_setattr_op(lfs->lc, lfs->lc->creds, (char *)fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating exnode! fname=%s\n", fname);
    }

    //** Clean up
    free(tape_val);
    for (i=0; i<nkeys; i++) {
        if (val[i] != NULL) free(val[i]);
    }

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

    *tape_val = NULL;
    *tape_size = 0;

    log_printf(15, "START fname=%s\n", fname);

    ftype = lio_exists(lfs->lc, lfs->lc->creds, fname);
    if (ftype <= 0) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        return;
    }

    for (i=0; i<_tape_key_size; i++) {
        val[i] = NULL;
        v_size[i] = -lfs->lc->max_attr;
    }

    log_printf(15, "fname=%s ftype=%d\n", fname, ftype);
    nkeys = (ftype & OS_OBJECT_SYMLINK_FLAG) ? 1 : _tape_key_size;
    i = lio_get_multiple_attrs(lfs->lc, lfs->lc->creds, fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
    if (i != OP_STATE_SUCCESS) {
        log_printf(15, "Failed retrieving file info!  path=%s\n", fname);
        return;
    }

    //** Figure out how much space we need
    n = 0;
    used = 0;
    tbx_append_printf(header, &used, hmax, "n_keys=%d\n", nkeys);
    for (i=0; i<nkeys; i++) {
        j = (v_size[i] > 0) ? v_size[i] : 0;
        n = n + 1 + j;
        tbx_append_printf(header, &used, hmax, "%s=%d\n", _tape_keys[i], j);
    }

    //** Copy all the data into the buffer;
    n = n + used;
    tbx_type_malloc_clear(buffer, char, n);
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
// lfs_getxattr - Gets an extended attribute
//*****************************************************************

#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int lfs_getxattr(const char *fname, const char *name, char *buf, size_t size)
#  else
int lfs_getxattr(const char *fname, const char *name, char *buf, size_t size, uint32_t dummy)
#  endif
{
    lio_fuse_t *lfs = lfs_get_context();
    char *val;
    int v_size, err;

    v_size= size;
    log_printf(1, "fname=%s size=%zu attr_name=%s\n", fname, size, name);
    tbx_log_flush();

    v_size = (size == 0) ? -lfs->lc->max_attr : -(int)size;
    val = NULL;
    if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {  //** Want the tape backup attr
        lfs_get_tape_attr(lfs, (char *)fname, &val, &v_size);
    } else {
        err = lio_getattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void **)&val, &v_size);
        if (err != OP_STATE_SUCCESS) {
            return(-ENOENT);
        }
    }

    if (v_size < 0) v_size = 0;  //** No attribute

    if (size == 0) {
        log_printf(1, "SIZE bpos=%d buf=%s\n", v_size, val);
    } else if ((int)size >= v_size) {
        log_printf(1, "FULL bpos=%d buf=%s\n",v_size, val);
        memcpy(buf, val, v_size);
    } else {
        log_printf(1, "ERANGE bpos=%d buf=%s\n", v_size, val);
    }

    if (val != NULL) free(val);
    return(v_size);
}
#endif //HAVE_XATTR
//*****************************************************************
// lfs_setxattr - Sets a extended attribute
//*****************************************************************
#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int lfs_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags)
#  else
int lfs_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags, uint32_t dummy)
#  endif
{
    lio_fuse_t *lfs = lfs_get_context();
    char *val;
    int v_size, err;

    v_size= size;
    log_printf(1, "fname=%s size=%zu attr_name=%s\n", fname, size, name);
    tbx_log_flush();

    if (flags != 0) { //** Got an XATTR_CREATE/XATTR_REPLACE
        v_size = 0;
        val = NULL;
        err = lio_getattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void **)&val, &v_size);
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
        err = lio_setattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, (void *)fval, v_size);
        if (err != OP_STATE_SUCCESS) {
            return(-ENOENT);
        }
    }

    return(0);
}

//*****************************************************************
// lfs_removexattr - Removes an extended attribute
//*****************************************************************

int lfs_removexattr(const char *fname, const char *name)
{
    lio_fuse_t *lfs = lfs_get_context();
    int v_size, err;

    log_printf(1, "fname=%s attr_name=%s\n", fname, name);
    tbx_log_flush();

    if ((lfs->enable_tape == 1) && (strcmp(name, LFS_TAPE_ATTR) == 0)) {
        return(0);
    }

    v_size = -1;
    err = lio_setattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, (char *)name, NULL, v_size);
    if (err != OP_STATE_SUCCESS) {
        return(-ENOENT);
    }

    return(0);
}
#endif //HAVE_XATTR
//*************************************************************************
// lfs_hardlink - Creates a hardlink to an existing file
//*************************************************************************

int lfs_hardlink(const char *oldname, const char *newname)
{
    lio_fuse_t *lfs = lfs_get_context();
    int err;

    log_printf(1, "oldname=%s newname=%s\n", oldname, newname);
    tbx_log_flush();

    //** Now do the hard link
    err = gop_sync_exec(lio_link_gop(lfs->lc, lfs->lc->creds, 0, (char *)oldname, (char *)newname, lfs->id));
    if (err != OP_STATE_SUCCESS) {
        return(-EIO);
    }

    return(0);
}

//*****************************************************************
//  lfs_readlink - Reads the object symlink
//*****************************************************************

int lfs_readlink(const char *fname, char *buf, size_t bsize)
{
    lio_fuse_t *lfs = lfs_get_context();
    int v_size, err, i;
    char *val;

    log_printf(15, "fname=%s\n", fname);
    tbx_log_flush();

    v_size = -lfs->lc->max_attr;
    val = NULL;
    err = lio_getattr(lfs->lc, lfs->lc->creds, (char *)fname, NULL, "os.link", (void **)&val, &v_size);
    if (err != OP_STATE_SUCCESS) {
        buf[0] = 0;
        return(-EIO);
    } else if (v_size < 0) {
        buf[0] = 0;
        return(-EINVAL);
    }

    if (val[0] == '/') {
        snprintf(buf, bsize, "%s%s", lfs->mount_point, (char *)val);
    } else {
        snprintf(buf, bsize, "%s", (char *)val);
    }
    if (val != NULL) free(val);
    buf[bsize] = 0;

    i=bsize;
    log_printf(15, "fname=%s bsize=%d link=%s mountpoint=%s\n", fname, i, buf, lfs->mount_point);
    tbx_log_flush();

    return(0);
}

//*****************************************************************
//  lfs_symlink - Makes a symbolic link
//*****************************************************************

int lfs_symlink(const char *link, const char *newname)
{
    lio_fuse_t *lfs = lfs_get_context();
    const char *link2;
    int err;

    log_printf(1, "link=%s newname=%s\n", link, newname);
    tbx_log_flush();

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
    err = gop_sync_exec(lio_link_gop(lfs->lc, lfs->lc->creds, 1, (char *)link2, (char *)newname, lfs->id));
    if (err != OP_STATE_SUCCESS) {
        return(-EIO);
    }

    return(0);
}

//*************************************************************************
// lfs_statfs - Returns the files system size
//*************************************************************************

int lfs_statfs(const char *fname, struct statvfs *fs)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_rs_space_t space;
    char *config;

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
    fs->f_ffree = 10*1024*1024;
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
                    int argc,
                    char **argv,
                    const char *mount_point)
{
    lio_fuse_t *lfs;
    char *section =  "lfs";

    lio_fuse_init_args_t *init_args;
    lio_fuse_init_args_t real_args;

    // Retrieve the fuse_context, the last argument of fuse_main(...) is passed in the private_data field for use as a generic user arg. We pass the mount point in it.
    struct fuse_context *ctx;
    if ((argc == 0) && (argv == NULL) && (mount_point == NULL)) {
        ctx = fuse_get_context();
        if (NULL == ctx || NULL == ctx->private_data) {
            log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid. (Hint: last arg of fuse_main(...) must be lio_fuse_init_args_t* and have the mount point set)");
            return(NULL); //TODO: what is the best way to signal failure in the init function? Note that the return value of this function overwrites the .private_data field of the fuse context
        } else {
            init_args = (lio_fuse_init_args_t*)ctx->private_data;
        }
    } else {
        // We weren't called by fuse, so the args are function arguments
        // AMM - 9/23/13
        init_args = &real_args;
        init_args->lio_argc = argc;
        init_args->lio_argv = argv;
        init_args->mount_point = (char *)mount_point;
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

    tbx_type_malloc_clear(lfs, lio_fuse_t, 1);

    lfs->lc = init_args->lc;
    lfs->mount_point = strdup(init_args->mount_point);
    lfs->mount_point_len = strlen(init_args->mount_point);

    lfs->enable_tape = tbx_inip_get_integer(lfs->lc->ifd, section, "enable_tape", 0);
    lfs->n_merge = tbx_inip_get_integer(lfs->lc->ifd, section, "n_merge", 128);

    apr_pool_create(&(lfs->mpool), NULL);
    apr_thread_mutex_create(&(lfs->lock), APR_THREAD_MUTEX_DEFAULT, lfs->mpool);
    lfs->open_files = apr_hash_make(lfs->mpool);

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

void *lfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    cfg->use_ino = 0;
    
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
    lio_fuse_t *lfs;

    log_printf(0, "shutting down\n");
    tbx_log_flush();

    lfs = (lio_fuse_t*)private_data;
    if (lfs == NULL) {
        log_printf(0,"lio_fuse_destroy: Error, the lfs handle is null, unable to shutdown cleanly. Perhaps lfs creation failed?");
        return;
    }

    //** We're ignoring cleaning up the open files table since were only using this on FUSE and FUSE should have closed all files

    //** Clean up everything else
    if (lfs->id != NULL) free (lfs->id);
    free(lfs->mount_point);
    apr_thread_mutex_destroy(lfs->lock);
    apr_pool_destroy(lfs->mpool);

    if (lfs->rw_hints) free(lfs->rw_hints);
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
    .truncate = lfs_ftruncate,
    .rename = lfs_rename,
    .mknod = lfs_mknod,
    .mkdir = lfs_mkdir,
    .unlink = lfs_unlink,
    .rmdir = lfs_rmdir,
    .open = lfs_open,
    .release = lfs_release,
    .read = lfs_read,
    .write = lfs_write,
    .flush = lfs_flush,
    .fsync = lfs_fsync,
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

