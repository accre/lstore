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

//***********************************************************************
//  hdfs_lstore - HDFS module supporting the use of LStore files
//***********************************************************************

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <gop/gop.h>
#include <lio/lio.h>
#include <tbx/constructor_wrapper.h>
#include <tbx/fmttypes.h>
#include <tbx/type_malloc.h>
#include "visibility.h"
#include "hdfs_lstore.h"

struct hdfs_lstore_s {
    lio_config_t *lc;
};

struct hdfsl_fd_s {
   lio_fd_t *fd;
   hdfs_lstore_t *ctx;
   lio_path_tuple_t tuple;
};

struct hdfsl_fstat_iter_s {
    hdfs_lstore_t *ctx;
    char *val[6];
    int v_size[6];
    os_object_iter_t *it;
    lio_os_regex_table_t *rp;
    lio_path_tuple_t tuple;
};

//***********************************************************************
//  lstore_activate - Start LIO subsystem when loading the shared library
//***********************************************************************

hdfs_lstore_t *lstore_activate(int *argc, char ***argv)
{
    hdfs_lstore_t *ctx;

    lio_init(argc, argv);
    if (!lio_gc) {
        log_printf(-1,"Failed to load LStore\n");
        exit(1);
    }

    tbx_type_malloc_clear(ctx, hdfs_lstore_t, 1);
    ctx->lc = lio_gc;

    tbx_log_flush();

    return(ctx);
}

//***********************************************************************
// lstore_deactivate - Shutdown LStore
//***********************************************************************

void lstore_deactivate(hdfs_lstore_t *ctx)
{
    tbx_log_flush();
    lio_shutdown();

    free(ctx);
}

//***********************************************************************
// lstore_delete - Remove a file and optionally recurse the path
//***********************************************************************

int lstore_delete(hdfs_lstore_t *ctx, char *path, int recurse_depth)
{
    int obj_types = OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG|OS_OBJECT_SYMLINK_FLAG;
    int err;
    lio_path_tuple_t tuple;

    lio_os_regex_table_t *rpath;

    if ((strcmp(path, "/") == 0) || (strcmp(path, "/*") == 0)) {
        fprintf(stderr, "Recursive delete from / not supported!\n");
        return(1);
    }

    tuple = lio_path_resolve(ctx->lc->auto_translate, path);
    rpath = lio_os_path_glob2regex(tuple.path);
    err = gop_sync_exec(lio_remove_regex_gop(ctx->lc, tuple.creds, rpath, NULL, obj_types, recurse_depth, 100));

    lio_path_release(&tuple);
    lio_os_regex_table_destroy(rpath);

    return(err == OP_STATE_SUCCESS ? 0 : 1);
}

//***********************************************************************
// lstore_fstat_parse - Parse the returned attibutes and stores them in
//   the HDFS fstat struct
//***********************************************************************

int lstore_fstat_parse(hdfsl_fstat_t *fstat, char **val, int *v_size)
{
    int ts;
    ex_off_t len;

    if (v_size[0] > 0) { fstat->user = val[0];  val[0] = NULL; }
    if (v_size[1] > 0) { fstat->group = val[1]; val[1] = NULL; }

    len = 0;
    if (v_size[2] > 0) { sscanf(val[2], XOT, &len); }
    fstat->len = len;

    ts = 0;
    if (v_size[3] > 0) lio_get_timestamp(val[3], &ts, NULL);
    fstat->modify_time_ms = ts * 1000;


    if (v_size[4] > 0) { fstat->symlink = val[4]; val[4] = NULL; }
    fstat->objtype = 0;
    if (v_size[5] > 0) sscanf(val[5], "%d", &fstat->objtype);

    return(0);
}

//***********************************************************************
// lstore_fstat - File stat for HDFS
//***********************************************************************

int lstore_fstat(hdfs_lstore_t *ctx, char *path, hdfsl_fstat_t *fstat)
{
    char *keys[] = { "system.owner", "system.group", "system.exnode.size", "system.modify_data", "os.link", "os.type" };
    char *vals[6];
    int v_size[6];
    lio_path_tuple_t tuple;
    int i, err;


    memset(fstat, 0, sizeof(hdfsl_fstat_t));
    fstat->path = path;
    tuple = lio_path_resolve(ctx->lc->auto_translate, path);

    for (i=0; i<6; i++) v_size[i] = -ctx->lc->max_attr;
    err = lio_get_multiple_attrs(tuple.lc, tuple.creds, tuple.path, NULL, keys, (void **)vals, v_size, 6);

    lio_path_release(&tuple);

    if (err != OP_STATE_SUCCESS) {
        return(1);
    }

    err = lstore_fstat_parse(fstat, vals, v_size);

    for (i=0; i<6; i++) if (vals[i]) free(vals[i]);
    return(err);
}

//***********************************************************************
// lstore_fstat_iter - Returns an fstat iterator
//***********************************************************************

hdfsl_fstat_iter_t *lstore_fstat_iter(hdfs_lstore_t *ctx, char *path, int recurse_depth)
{
    hdfsl_fstat_iter_t *it;
    char fpath[4096];
    char *keys[] = { "system.owner", "system.group", "system.exnode.size", "system.modify_data", "os.link", "os.type" };
    int i;
    int obj_types = OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG|OS_OBJECT_SYMLINK_FLAG;

    tbx_type_malloc_clear(it, hdfsl_fstat_iter_t, 1);

    snprintf(fpath, sizeof(fpath), "%s/*", path);
    it->tuple = lio_path_resolve(ctx->lc->auto_translate, fpath);
    it->rp = lio_os_path_glob2regex(it->tuple.path);
    it->ctx = ctx;
    for (i=0; i<6; i++) it->v_size[i] = -ctx->lc->max_attr;
    it->it = lio_create_object_iter_alist(it->tuple.lc, it->tuple.creds, it->rp, NULL, obj_types, recurse_depth, keys, (void **)it->val, it->v_size, 6);

    return(it);
}

//***********************************************************************
// lstore_fstat_iter_next - Returns the next iterator object
//***********************************************************************

int lstore_fstat_iter_next(hdfsl_fstat_iter_t *it, hdfsl_fstat_t *fstat)
{
    int ftype, prefix_len, err, i;

    memset(fstat, 0, sizeof(hdfsl_fstat_t));
    err = 0;
    ftype = lio_next_object(it->tuple.lc, it->it, &fstat->path, &prefix_len);
    if (ftype == 0) return(-1);

    err = lstore_fstat_parse(fstat, it->val, it->v_size);

    for (i=0; i<6; i++) it->v_size[i] = -it->ctx->lc->max_attr;

    for (i=0; i<6; i++) if (it->val[i]) free(it->val[i]);
    return(err);
}

//***********************************************************************
// lstore_fstat_iter - Destroys an fstat iterator
//***********************************************************************

void lstore_fstat_iter_destroy(hdfsl_fstat_iter_t *it)
{
    lio_destroy_object_iter(it->ctx->lc, it->it);
    lio_os_regex_table_destroy(it->rp);
    lio_path_release(&it->tuple);
    free(it);
}

//***********************************************************************
// lstore_mkdir - Make a directory
//***********************************************************************

int lstore_mkdir(hdfs_lstore_t *ctx, char *path)
{
    int ftype, err;
    char *dpath, *fpath;
    lio_path_tuple_t tuple;

    tuple = lio_path_resolve(ctx->lc->auto_translate, path);

    //** Make sure it doesn't exist
    ftype = lio_exists(tuple.lc, tuple.creds, tuple.path);

    if (ftype > 0) { //** The file exists
        err = 1;
    } else if (ftype < 0) {
        err = 2;
    } else {
        //** Now create the object
        err = gop_sync_exec(lio_create_gop(tuple.lc, tuple.creds, tuple.path, OS_OBJECT_DIR_FLAG, NULL, NULL));
        if (err != OP_STATE_SUCCESS) {  //** Failed so try and recurse back up/down to make the intermediate dirs
            lio_os_path_split(tuple.path, &dpath, &fpath);
            err = lstore_mkdir(ctx, dpath);
            if (dpath) free(dpath);
            if (fpath) free(fpath);
            if (err == 0) {  //** Successfully made the parent directory
                err = gop_sync_exec(lio_create_gop(tuple.lc, tuple.creds, tuple.path, OS_OBJECT_DIR_FLAG, NULL, NULL));
                err = (err == OP_STATE_SUCCESS) ? 0 : 1;
            }
        } else {
            err = 0;
        }
    }

    lio_path_release(&tuple);

    return(err);
}

//***********************************************************************
// lstore_rename - Rename an object
//***********************************************************************

int lstore_rename(hdfs_lstore_t *ctx, char *src_path, char *dest_path)
{
    int err;
    lio_path_tuple_t stuple, dtuple;

    stuple = lio_path_resolve(ctx->lc->auto_translate, src_path);
    dtuple = lio_path_resolve(ctx->lc->auto_translate, dest_path);

    err = gop_sync_exec(lio_move_object_gop(stuple.lc, stuple.creds, stuple.path, dtuple.path));

    lio_path_release(&stuple);
    lio_path_release(&dtuple);

    return(err == OP_STATE_SUCCESS ? 0 : 1);

}

//***********************************************************************
//  lstore_open - Opens an existing object
//***********************************************************************

hdfsl_fd_t *lstore_open(hdfs_lstore_t *ctx, char *path, int mode)
{
    hdfsl_fd_t *fd;
    int lmode;

    tbx_type_malloc_clear(fd, hdfsl_fd_t, 1);
    fd->ctx = ctx;
    fd->tuple = lio_path_resolve(lio_gc->auto_translate, path);

    switch(mode) {
        case HDFSL_OPEN_READ:
            lmode = LIO_READ_MODE;
            break;
        case HDFSL_OPEN_WRITE:
            lmode = LIO_WRITE_MODE | LIO_TRUNCATE_MODE | LIO_CREATE_MODE;
            break;
        case HDFSL_OPEN_APPEND:
            lmode = LIO_WRITE_MODE | LIO_CREATE_MODE;
            break;
        default:
            lio_path_release(&fd->tuple);
            free(fd);
            return(NULL);
    }

    gop_sync_exec(lio_open_gop(ctx->lc, ctx->lc->creds, fd->tuple.path, lmode, NULL, &fd->fd, 60));
    if (fd->fd == NULL) {
        lio_path_release(&fd->tuple);
        free(fd);
        return(NULL);
    }

    if (mode == HDFSL_OPEN_APPEND) {
        lio_seek(fd->fd, 0, SEEK_END);
    } else {
        lio_seek(fd->fd, 0, SEEK_SET);
    }

    return(fd);
}

//***********************************************************************
// lstore_close - Close the target
//***********************************************************************

void lstore_close(hdfsl_fd_t *fd)
{
    gop_sync_exec(lio_close_gop(fd->fd));

    lio_path_release(&fd->tuple);
    free(fd);
}

//***********************************************************************
//  lstore_read - Read data from the target
//***********************************************************************

int lstore_read(hdfsl_fd_t *fd, char *buf, int64_t length)
{
    int err;

    err = lio_read(fd->fd, buf, length, -1, NULL);

    return((err == length) ? 0 : 1);
}

//***********************************************************************
//  lstore_write - Read data from the target
//***********************************************************************

int lstore_write(hdfsl_fd_t *fd, char *buf, int64_t length)
{

    int err;

    err = lio_write(fd->fd, buf, length, -1, NULL);

    return((err == length) ? 0 : 1);
}


//***********************************************************************
// lstore_seek - Set the current file position
//***********************************************************************

void lstore_seek(hdfsl_fd_t *fd, int64_t off)
{
    lio_seek(fd->fd, off, SEEK_SET);
    return;
}

//***********************************************************************
// lstore_getpos - Get the current position
//***********************************************************************

int64_t lstore_getpos(hdfsl_fd_t *fd)
{
    return(lio_tell(fd->fd));
}
