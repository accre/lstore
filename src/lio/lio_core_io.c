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

#define _log_module_index 189

#include <apr_time.h>
#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <zlib.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/types.h"
#include "lio.h"
#include "os.h"

//***********************************************************************
// Core LIO I/O functionality
//***********************************************************************

#define LFH_KEY_INODE  0
#define LFH_KEY_EXNODE 1
#define LFH_NKEYS      2

static char *_lio_fh_keys[] = { "system.inode", "system.exnode" };

typedef struct {
    ex_off_t offset;
    ex_off_t len;
    uLong adler32;
} lfs_adler32_t;

//***********************************************************************
// Core LIO R/W functionality
//***********************************************************************

//***********************************************************************
// lio_fopen_flags - Handles fopen type string flags and converts them
//   to an integer which can be passed to lio_open calls.
//   On error -1 is returned
//***********************************************************************

int lio_fopen_flags(char *sflags)
{
    int mode = -1;

    if (strcmp(sflags, "r") == 0) {
        mode = LIO_READ_MODE;
    } else if (strcmp(sflags, "r+") == 0) {
        mode = LIO_RW_MODE;
    } else if (strcmp(sflags, "w") == 0) {
        mode = LIO_WRITE_MODE | LIO_TRUNCATE_MODE | LIO_CREATE_MODE;
    } else if (strcmp(sflags, "w+") == 0 ) {
        mode = LIO_RW_MODE | LIO_TRUNCATE_MODE | LIO_CREATE_MODE;
    } else if (strcmp(sflags, "a") == 0) {
        mode = LIO_WRITE_MODE | LIO_CREATE_MODE | LIO_APPEND_MODE;
    } else if (strcmp(sflags, "w+") == 0) {
        mode = LIO_RW_MODE | LIO_CREATE_MODE | LIO_APPEND_MODE;
    }

    return(mode);
}

//***********************************************************************
// lio_encode_error_counts - Encodes the error counts for a setattr call
//
//  The keys, val, and v_size arrays should have 3 elements. Buf is used
//  to store the error numbers.  It's assumed to have at least 3*32 bytes.
//  mode is used to determine how to handle 0 error values
//  (-1=remove attr, 0=no update, 1=store 0 value).
//  On return the number of attributes stored is returned.
//***********************************************************************

int lio_encode_error_counts(lio_segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode)
{
    char *ekeys[] = { "system.hard_errors", "system.soft_errors",  "system.write_errors" };
    int err[3];
    int i, n, k;

    k = n = 0;

    //** So I can do this in a loop
    err[0] = serr->hard;
    err[1] = serr->soft;
    err[2] = serr->write;

    for (i=0; i<3; i++) {
        if ((err[i] != 0) || (mode == 1)) {  //** Always store
            val[n] = &(buf[k]);
            k += snprintf(val[n], 32, "%d", err[i]) + 1;
            v_size[n] = strlen(val[n]);
            key[n] = ekeys[i];
            n++;
        } else if (mode == -1) { //** Remove the attribute
            val[n] = NULL;
            v_size[n] = -1;
            key[n] = ekeys[i];
            n++;
        }
    }

    return(n);
}

//***********************************************************************
// lio_get_error_counts - Gets the error counts
//***********************************************************************

void lio_get_error_counts(lio_config_t *lc, lio_segment_t *seg, lio_segment_errors_t *serr)
{
    gop_op_generic_t *gop;
    gop_op_status_t status;

    gop = segment_inspect(seg, lc->da, lio_ifd, INSPECT_HARD_ERRORS, 0, NULL, 1);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
    serr->hard = status.error_code;

    gop = segment_inspect(seg, lc->da, lio_ifd, INSPECT_SOFT_ERRORS, 0, NULL, 1);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
    serr->soft = status.error_code;

    gop = segment_inspect(seg, lc->da, lio_ifd, INSPECT_WRITE_ERRORS, 0, NULL, 1);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
    serr->write = status.error_code;

    return;
}

//***********************************************************************
// lio_update_error_count - Updates the error count attributes if needed
//***********************************************************************

int lio_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode)
{
    char *keys[3];
    char *val[3];
    char buf[128];
    int v_size[3];
    int n;
    lio_segment_errors_t serr;

    lio_get_error_counts(lc, seg, &serr);
    n = lio_encode_error_counts(&serr, keys, val, buf, v_size, mode);
    if (n > 0) {
        lio_multiple_setattr_op(lc, creds, path, NULL, keys, (void **)val, v_size, n);
    }

    return(serr.hard);
}

//***********************************************************************
// lio_update_exnode_attrs - Updates the exnode and system.error_* attributes
//***********************************************************************

int lio_update_exnode_attrs(lio_config_t *lc, lio_creds_t *creds, lio_exnode_t *ex, lio_segment_t *seg, char *fname, lio_segment_errors_t *serr)
{
    ex_off_t ssize;
    char buffer[32];
    char *key[6] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data", NULL, NULL, NULL };
    char *val[6];
    lio_exnode_exchange_t *exp;
    int n, err, ret, v_size[6];
    lio_segment_errors_t my_serr;
    char ebuf[128];

    ret = 0;
    if (serr == NULL) serr = &my_serr; //** If caller doesn't care about errors use my own space

    //** Serialize the exnode
    exp = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(ex, exp);
    ssize = segment_size(seg);

    //** Get any errors that may have occured
    lio_get_error_counts(lc, seg, serr);

    //** Update the exnode
    n = 3;
    val[0] = exp->text.text;
    v_size[0] = strlen(val[0]);
    log_printf(0, "fname=%s exnode_size=%d exnode=%s\n", fname, v_size[0], exp->text.text);
    sprintf(buffer, XOT, ssize);
    val[1] = buffer;
    v_size[1] = strlen(val[1]);
    val[2] = NULL;
    v_size[2] = 0;

    n += lio_encode_error_counts(serr, &(key[3]), &(val[3]), ebuf, &(v_size[3]), 0);
    if ((serr->hard>0) || (serr->soft>0) || (serr->write>0)) {
        log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fname, serr->hard, serr->soft, serr->write);
        ret += 1;
    }

    err = lio_multiple_setattr_op(lc, creds, fname, NULL, key, (void **)val, v_size, n);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating exnode+attrs! fname=%s\n", fname);
        ret += 2;
    }

    lio_exnode_exchange_destroy(exp);

    return(ret);
}

//*****************************************************************
// lio_store_and_release_adler32 - Takes all the adler32 structures
//    and coalesces them into a single adler32 and stores it in the
//    user.lfs_adler32 file attribute.
//    It also detroys the write_table
//*****************************************************************

void lio_store_and_release_adler32(lio_config_t *lc, lio_creds_t *creds, tbx_list_t *write_table, char *fname)
{
    tbx_list_iter_t it;
    ex_off_t next, missing, overlap, dn, nbytes, pend;
    uLong cksum;
    unsigned int aval;
    lfs_adler32_t *a32;
    tbx_stack_t *stack;
    ex_off_t *aoff;
    char value[256];
    stack = tbx_stack_new();
    it = tbx_list_iter_search(write_table, 0, 0);
    cksum = adler32(0L, Z_NULL, 0);
    missing = next = overlap = nbytes = 0;
    while (tbx_list_next(&it, (tbx_list_key_t **)&aoff, (tbx_list_data_t **)&a32) == 0) {
        aval = a32->adler32;
        pend = a32->offset + a32->len - 1;
        tbx_stack_push(stack, a32);

        if (a32->offset != next) {
            dn = a32->offset - next;
            log_printf(1, "fname=%s a32=%08x off=" XOT " end=" XOT " nbytes=" XOT " OOPS dn=" XOT "\n", fname, aval, a32->offset, pend, a32->len, dn);
            if (dn < 0) {
                overlap -= dn;
            } else {
                missing += dn;
            }
        } else {
            log_printf(1, "fname=%s a32=%08x off=" XOT " end=" XOT " nbytes=" XOT "\n", fname, aval, a32->offset, pend, a32->len);
        }

        nbytes += a32->len;
        cksum = adler32_combine(cksum, a32->adler32, a32->len);

        next = a32->offset + a32->len;
    }

    tbx_list_destroy(write_table);
    tbx_stack_free(stack, 1);

    //** Store the attribute
    aval = cksum;
    dn = snprintf(value, sizeof(value), "%08x:" XOT ":" XOT ":" XOT, aval, missing, overlap, nbytes);
    lio_setattr(lc, creds, fname, NULL, "user.lfs_write", value, dn);
}

//***********************************************************************
//  lio_load_file_handle_attrs - Loads the attributes for a file handle
//***********************************************************************

int lio_load_file_handle_attrs(lio_config_t *lc, lio_creds_t *creds, char *fname, ex_id_t *inode, char **exnode)
{
    char *myfname;
    char vino[256];
    int err, v_size[2];
    char *val[2];

    //** Get the attributes
    v_size[0] = sizeof(vino);
    val[0] = vino;
    v_size[1] = -lc->max_attr;
    val[1] = NULL;

    myfname = (strcmp(fname, "") == 0) ? "/" : (char *)fname;
    err = lio_get_multiple_attrs(lc, creds, myfname, NULL, _lio_fh_keys, (void **)val, v_size, LFH_NKEYS);
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        if (val[1] != NULL) free(val[1]);
        return(-1);
    }

    *exnode = val[1];

    if (v_size[LFH_KEY_INODE] > 0) {
        *inode = 0;
        sscanf(vino, XIDT, inode);
    } else {
        generate_ex_id(inode);
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT "\n", *inode);
    }

    return(0);
}


//***********************************************************************
//  _lio_get_file_handle - Returns the file handle associated with the view ID
//     number if the file is already open.  Otherwise NULL is returned
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t vid)
{
    return(tbx_list_search(lc->open_index, (tbx_list_key_t *)&vid));

}

//***********************************************************************
// _lio_add_file_handle - Adds the file handle to the table
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

void _lio_add_file_handle(lio_config_t *lc, lio_file_handle_t *fh)
{
    tbx_list_insert(lc->open_index, (tbx_list_key_t *)&(fh->vid), (tbx_list_data_t *)fh);
}


//***********************************************************************
// _lio_remove_file_handle - Removes the file handle from the open table
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

void _lio_remove_file_handle(lio_config_t *lc, lio_file_handle_t *fh)
{
    tbx_list_remove(lc->open_index, (tbx_list_key_t *)&(fh->vid), (tbx_list_data_t *)fh);
}

//*************************************************************************
// lio_open_gop - Attempt to open the object for R/W
//*************************************************************************

typedef struct {
    char *id;
    lio_config_t *lc;
    lio_creds_t *creds;
    char *path;
    lio_fd_t **fd;
    int max_wait;
    int mode;
} lio_fd_op_t;

//*************************************************************************

gop_op_status_t lio_myopen_fn(void *arg, int id)
{
    lio_fd_op_t *op = (lio_fd_op_t *)arg;
    lio_config_t *lc = op->lc;
    lio_file_handle_t *fh;
    lio_fd_t *fd;
    char *exnode;
    ex_id_t ino, vid;
    lio_exnode_exchange_t *exp;
    gop_op_status_t status;
    int dtype, err;

    status = gop_success_status;

    //** Check if it exists
    dtype = lio_exists(lc, op->creds, op->path);

    if ((op->mode & (LIO_WRITE_MODE|LIO_CREATE_MODE)) != 0) {  //** Writing and they want to create it if it doesn't exist
        if (dtype == 0) { //** Need to create it
            err = gop_sync_exec(lio_create_gop(lc, op->creds, op->path, OS_OBJECT_FILE_FLAG, NULL, NULL));
            if (err != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", op->path);
                log_printf(1, "ERROR creating file(%s)!\n", op->path);
                free(op->path);
                *op->fd = NULL;
                return(gop_failure_status);
            }
        } else if ((dtype & OS_OBJECT_DIR_FLAG) > 0) { //** It's a dir so fail
            info_printf(lio_ifd, 1, "Destination(%s) is a dir!\n", op->path);
            log_printf(1, "ERROR: Destination(%s) is a dir!\n", op->path);
            free(op->path);
            *op->fd = NULL;
            return(gop_failure_status);
        } else if (op->mode & LIO_EXCL_MODE) { //** This file shouldn't exist with this flag so kick out
            info_printf(lio_ifd, 1, "ERROR file(%s) already exists and EXCL is set!\n", op->path);
            log_printf(1, "ERROR file(%s) already exists and EXCL is set!\n", op->path);
            free(op->path);
            *op->fd = NULL;
            return(gop_failure_status);
        }
    } else if (dtype == 0) { //** No file so return an error
        info_printf(lio_ifd, 20, "Destination(%s) doesn't exist!\n", op->path);
        log_printf(1, "ERROR: Destination(%s) doesn't exist!\n", op->path);
        free(op->path);
        *op->fd = NULL;
        return(gop_failure_status);
    }

    //** Make the space for the FD
    tbx_type_malloc_clear(fd, lio_fd_t, 1);
    fd->path = op->path;
    fd->mode = op->mode;
    fd->creds = op->creds;
    fd->lc = lc;

    exnode = NULL;
    if (lio_load_file_handle_attrs(lc, op->creds, op->path, &ino, &exnode) != 0) {
        log_printf(1, "ERROR loading attributes! fname=%s\n", op->path);
        free(fd);
        *op->fd = NULL;
        free(op->path);
        return(gop_failure_status);
    }

    //** Load the exnode and get the default view ID
    exp = lio_lio_exnode_exchange_text_parse(exnode);
    vid = exnode_exchange_get_default_view_id(exp);
    if (vid == 0) {  //** Make sure the vid is valid.
        log_printf(1, "ERROR loading exnode! fname=%s\n", op->path);
        free(fd);
        *op->fd = NULL;
        free(op->path);
        lio_exnode_exchange_destroy(exp);
        return(gop_failure_status);
    }

    lio_lock(lc);
    fh = _lio_get_file_handle(lc, vid);
    log_printf(2, "fname=%s fh=%p\n", op->path, fh);

    if (fh != NULL) { //** Already open so just increment the ref count and return a new fd
        fh->ref_count++;
        fd->fh = fh;
        lio_unlock(lc);
        *op->fd = fd;
        lio_exnode_exchange_destroy(exp);
        return(gop_success_status);
    }

    //** New file to open
    tbx_type_malloc_clear(fh, lio_file_handle_t, 1);
    fh->vid = vid;
    fh->ref_count++;
    fh->lc = lc;

    //** Load it
    fh->ex = lio_exnode_create();
    if (lio_exnode_deserialize(fh->ex, exp, lc->ess) != 0) {
        log_printf(0, "ERROR: Bad exnode! fname=%s\n", fd->path);
        status.op_status = OP_STATE_FAILURE;
        status.error_code = -EFAULT;
        goto cleanup;
    }

    //** Get the default view to use
    fh->seg = lio_exnode_default_get(fh->ex);
    if (fh->seg == NULL) {
        log_printf(0, "ERROR: No default segment!  Aborting! fname=%s\n", fd->path);
        status.op_status = OP_STATE_FAILURE;
        status.error_code = -EFAULT;
        goto cleanup;
    }

    if (lc->calc_adler32) fh->write_table = tbx_list_create(0, &skiplist_compare_ex_off, NULL, NULL, NULL);

    //Add it to the file open table
    _lio_add_file_handle(lc, fh);
    lio_unlock(lc);  //** Now we can release the lock

    fd->fh = fh;
    *op->fd = fd;

    if ((op->mode & LIO_WRITE_MODE) > 0) {  //** For write mode we check for a few more flags
        if ((op->mode & LIO_TRUNCATE_MODE) > 0) { //** See if they want the file truncated also
            status = gop_sync_exec_status(lio_truncate_gop(fd, 0));
            if (status.op_status != OP_STATE_SUCCESS) goto cleanup;
        }

        if ((op->mode & LIO_APPEND_MODE) > 0) { //** Append to the end of the file
            segment_lock(fh->seg);
            fd->curr_offset = 0;
            segment_unlock(fh->seg);
        }
    }

    lio_exnode_exchange_destroy(exp);  //** Clean up

    return(status);

cleanup:  //** We only make it here on a failure
    log_printf(1, "ERROR in cleanup! fname=%s\n", op->path);

    lio_exnode_destroy(fh->ex);
    lio_exnode_exchange_destroy(exp);
    free(fd->path);
    free(fh);
    free(fd);
    *op->fd = NULL;

    return(status);
}


//*************************************************************************

gop_op_generic_t *lio_open_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int mode, char *id, lio_fd_t **fd, int max_wait)
{
    lio_fd_op_t *op;

    tbx_type_malloc_clear(op, lio_fd_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->mode = mode;
    op->id = id;
    op->path = strdup(path);
    op->fd = fd;
    op->max_wait = max_wait;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_myopen_fn, (void *)op, free, 1));
}


//*************************************************************************
// lio_close_gop - Rotuines for closing a previously opened file
//*************************************************************************

//*************************************************************************

gop_op_status_t lio_myclose_fn(void *arg, int id)
{
    lio_fd_t *fd = (lio_fd_t *)arg;
    lio_config_t *lc = fd->lc;
    lio_file_handle_t *fh;
    gop_op_status_t status;
    char *key[6] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data", NULL, NULL, NULL };
    char *val[6];
    int err, v_size[6];
    char ebuf[128];
    lio_segment_errors_t serr;
    ex_off_t final_size;
    apr_time_t now;
    int n;
    double dt;

    log_printf(1, "fname=%s modified=%d count=%d\n", fd->path, fd->fh->modified, fd->fh->ref_count);
    tbx_log_flush();

    status = gop_success_status;

    //** Get the handles
    fh = fd->fh;

    lio_lock(lc);
    fh->ref_count--;
    if (fh->ref_count > 0) {  //** Somebody else has it open as well
        lio_unlock(lc);
        return(status);
    }
    lio_unlock(lc);

    final_size = segment_size(fh->seg);

    log_printf(1, "FLUSH/TRUNCATE fname=%s final_size=" XOT "\n", fd->path, final_size);
    //** Flush and truncate everything which could take some time
    now = apr_time_now();
    gop_sync_exec(lio_segment_truncate(fh->seg, lc->da, final_size, lc->timeout));
    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "TRUNCATE fname=%s dt=%lf\n", fd->path, dt);
    now = apr_time_now();
    gop_sync_exec(segment_flush(fh->seg, lc->da, 0, segment_size(fh->seg)+1, lc->timeout));
    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "FLUSH fname=%s dt=%lf\n", fd->path, dt);

    log_printf(5, "starting update process fname=%s modified=%d\n", fd->path, fh->modified);
    tbx_log_flush();

    //** Ok no one has the file opened so teardown the segment/exnode
    //** IF not modified just tear down and clean up
    if (fh->modified == 0) {
        //*** See if we need to update the error counts
        lio_get_error_counts(lc, fh->seg, &serr);
        n = lio_encode_error_counts(&serr, key, val, ebuf, v_size, 0);
        if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
            log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fd->path, serr.hard, serr.soft, serr.write);
        }
        if (n > 0) {
            err = lio_multiple_setattr_op(lc, fd->creds, fd->path, NULL, key, (void **)val, v_size, n);
            if (err != OP_STATE_SUCCESS) {
                log_printf(0, "ERROR updating exnode! fname=%s\n", fd->path);
            }
        }

        //** Check again that no one else has opened the file
        lio_lock(lc);
        if (fh->ref_count > 0) {  //** Somebody else opened it while we were flushing buffers
            if (fd->path != NULL) free(fd->path);
            free(fd);
            lio_unlock(lc);
            return(status);
        }

        //** Tear everything down
        lio_exnode_destroy(fh->ex);
        _lio_remove_file_handle(lc, fh);
        lio_unlock(lc);

        if (fh->write_table != NULL) lio_store_and_release_adler32(lc, fd->creds, fh->write_table, fd->path);
        if (fh->remove_on_close == 1) status = gop_sync_exec_status(lio_remove_gop(lc, fd->creds, fd->path, NULL, lio_exists(lc, fd->creds, fd->path)));

        free(fh);
        if (fd->path != NULL) free(fd->path);
        free(fd);
        return(status);
    }

    //** Get any errors that may have occured
    lio_get_error_counts(lc, fh->seg, &serr);

    now = apr_time_now();

    //** Update the exnode and misc attributes
    err = lio_update_exnode_attrs(lc, fd->creds, fh->ex, fh->seg, fd->path, &serr);
    if (err > 1) {
        log_printf(0, "ERROR updating exnode! fname=%s\n", fd->path);
    }

    if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
        log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fd->path, serr.hard, serr.soft, serr.write);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "ATTR_UPDATE fname=%s dt=%lf\n", fd->path, dt);

    lio_lock(lc);  //** MAke sure no one else has opened the file while we were trying to close
    log_printf(1, "fname=%s ref_count=%d\n", fd->path, fh->ref_count);

    if (fh->ref_count > 0) {  //** Somebody else opened it while we were flushing buffers
        if (fd->path != NULL) free(fd->path);
        free(fd);
        lio_unlock(lc);
        return(status);
    }

    //** Clean up
    now = apr_time_now();
    lio_exnode_destroy(fh->ex); //** This is done in the lock to make sure the exnode isn't loaded twice
    _lio_remove_file_handle(lc, fh);

    lio_unlock(lc);

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "lio_exnode_destroy fname=%s dt=%lf\n", fd->path, dt);
    if (fh->write_table != NULL) lio_store_and_release_adler32(lc, fd->creds, fh->write_table, fd->path);


    if (fh->remove_on_close) status = gop_sync_exec_status(lio_remove_gop(lc, fd->creds, fd->path, NULL, lio_exists(lc, fd->creds, fd->path)));

    free(fh);
    if (fd->path != NULL) free(fd->path);
    free(fd);

    if (serr.hard != 0) status = gop_failure_status;
    log_printf(1, "hard=%d soft=%d status=%d\n", serr.hard, serr.soft, status.op_status);
    return(status);
}

//*************************************************************************

gop_op_generic_t *lio_close_gop(lio_fd_t *fd)
{
    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_myclose_fn, (void *)fd, NULL, 1));
}


//*************************************************************************
// lio_read_gop_XXXX - The various read routines
//*************************************************************************

typedef struct {
    lio_fd_t *fd;
    int n_iov;
    ex_tbx_iovec_t *iov;
    tbx_tbuf_t *buffer;
    lio_segment_rw_hints_t *rw_hints;
    ex_tbx_iovec_t iov_dummy;
    tbx_tbuf_t buffer_dummy;
    ex_off_t boff;
} lio_rw_op_t;

//*************************************************************************

gop_op_status_t lio_read_ex_fn(void *arg, int id)
{
    lio_rw_op_t *op = (lio_rw_op_t *)arg;
    lio_fd_t *fd = op->fd;
    lio_config_t *lc = fd->lc;
    ex_tbx_iovec_t *iov = op->iov;
    tbx_tbuf_t *buffer = op->buffer;
    gop_op_status_t status;
    int i, err, size;
    apr_time_t now;
    double dt;
    ex_off_t t1, t2;

    status = gop_success_status;
    if (op->n_iov <=0) return(status);

    t1 = iov[0].len;
    t2 = iov[0].offset;
    log_printf(1, "fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fd->path, op->n_iov, t1, t2);
    tbx_log_flush();

    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null file desriptor\n");
        _op_set_status(status, OP_STATE_FAILURE, -EBADF);
        return(status);
    }

    if (tbx_log_level() > 0) {
        for (i=0; i < op->n_iov; i++) {
            t2 = iov[i].offset+iov[i].len-1;
            log_printf(1, "LFS_READ:START " XOT " " XOT "\n", iov[i].offset, t2);
            log_printf(1, "LFS_READ:END " XOT "\n", t2);
        }
    }

    now = apr_time_now();

    //** Do the read op
    err = gop_sync_exec(segment_read(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, buffer, op->boff, lc->timeout));

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s seg=" XIDT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), dt);
    tbx_log_flush();

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR with read! fname=%s\n", fd->path);
        printf("got value %d\n", err);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        return(status);
    }

    //** Update the file position to thelast write
    segment_lock(fd->fh->seg);
    fd->curr_offset = iov[op->n_iov-1].offset+iov[op->n_iov-1].len;
    segment_unlock(fd->fh->seg);

    size = iov[0].len;
    for (i=1; i < op->n_iov; i++) size += iov[i].len;
    status.error_code = size;
    return(status);
}

//*************************************************************************

gop_op_generic_t *lio_read_ex_gop(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = n_iov;
    op->iov = ex_iov;
    op->buffer = buffer;
    op->boff = boff;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_read_ex_fn, (void *)op, free, 1));
}

//*************************************************************************

int lio_read_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t op;
    gop_op_status_t status;

    op.fd = fd;
    op.n_iov = n_iov;
    op.iov = ex_iov;
    op.buffer = buffer;
    op.boff = boff;
    op.rw_hints = rw_hints;

    status = lio_read_ex_fn((void *)&op, -1);
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_readv_gop(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    ex_off_t offset;
    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_vec(&(op->buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_read_ex_fn, (void *)op, free, 1));
}

//*************************************************************************

int lio_readv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t op;
    ex_off_t offset;
    gop_op_status_t status;

    op.fd = fd;
    op.n_iov = 1;
    op.iov = &(op.iov_dummy);
    op.buffer = &(op.buffer_dummy);
    op.boff = 0;
    op.rw_hints = rw_hints;

    tbx_tbuf_vec(&(op.buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op.iov, offset, size);
    status = lio_read_ex_fn((void *)&op, -1);
    return(status.error_code);
}

//*****************************************************************
// _lio_read_gop - Generates a read op.
//    NOTE: Uses the LIO readahead hints
// Return values 1 = Read beyond EOF so client should return gop_dummy(op_success_Status)
//               0 = Normal status. should call gop_read_ex
//               < 0 Bad command and the value is the error status to return
//*****************************************************************

int _lio_read_gop(lio_rw_op_t *op, lio_fd_t *fd, char *buf, ex_off_t size, off_t user_off, lio_segment_rw_hints_t *rw_hints)
{
    ex_off_t ssize, pend, rsize, rend, dr, off;

    //** Determine the offset
    off = (user_off < 0) ? fd->curr_offset : user_off;

    //** Do the read op
    ssize = segment_size(fd->fh->seg);
    pend = off + size;
    log_printf(0, "ssize=" XOT " off=" XOT " len=" XOT " pend=" XOT " readahead=" XOT " trigger=" XOT "\n", ssize, off, size, pend, fd->fh->lc->readahead, fd->fh->lc->readahead_trigger);
    if (pend > ssize) {
        if (off > ssize) {
            // offset is past the end of the segment
            return(1);
        } else {
            size = ssize - off;  //** Tweak the size based on how much data there is
        }
    }
    log_printf(0, "tweaked len=" XOT "\n", size);
    if (size <= 0) {
        log_printf(0, "Clipped tweaked len\n");
        return(1);
    }

    rend = pend + fd->fh->lc->readahead;  //** Tweak based on readahead
    rsize = size;
    segment_lock(fd->fh->seg);
    dr = pend - fd->fh->readahead_end;
    if ((dr > 0) || ((-dr) > fd->fh->lc->readahead_trigger)) {
        rsize = rend - off;
        if (rend > ssize) {
            if (off <= ssize) {
                rsize = ssize - off;  //** Tweak the size based on how much data there is
            }
        }

        fd->fh->readahead_end = rend;  //** Update the readahead end
    }
    segment_unlock(fd->fh->seg);

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_single(op->buffer, size, buf);  //** This is the buffer size
    ex_iovec_single(op->iov, off, rsize); //** This is the buffer+readahead.  The extra doesn't get stored in the buffer.  Just in page cache.
    return(0);
}

//*****************************************************************

gop_op_generic_t *lio_read_gop(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    gop_op_status_t status;
    int err;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    err = _lio_read_gop(op, fd, buf, size, off, rw_hints);
    if (err == 0) {
        return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_read_ex_fn, (void *)op, free, 1));
    } else if (err == 1) {
        free(op);
        return(gop_dummy(gop_success_status));
    } else {
        free(op);
        _op_set_status(status, OP_STATE_FAILURE, err);
        return(gop_dummy(status));
    }

    return(NULL);  //** Never make it here
}

//*****************************************************************

int lio_read(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t op;
    gop_op_status_t status;
    int err;

    err = _lio_read_gop(&op, fd, buf, size, off, rw_hints);
    if (err == 0) {
        status = lio_read_ex_fn((void *)&op, -1);
    } else if (err == 1) {
        status = gop_success_status;
    } else {
        _op_set_status(status, OP_STATE_FAILURE, err);
    }

    return(status.error_code);
}


//*****************************************************************
// lio_write_gop_XXXX - The various write routines
//*****************************************************************

//*****************************************************************
// lio_write_ex_fn - Does the actual writing of data to the file using a more native interface
//*****************************************************************

gop_op_status_t lio_write_ex_fn(void *arg, int id)
{
    lio_rw_op_t *op = (lio_rw_op_t *)arg;
    lio_fd_t *fd = op->fd;
    lio_config_t *lc = op->fd->fh->lc;
    ex_tbx_iovec_t *iov = op->iov;
    tbx_tbuf_t *buffer = op->buffer;
    gop_op_status_t status;
    int i, err, size;
    apr_time_t now;
    double dt;
    ex_off_t t1, t2;

    if ((fd->mode & LIO_WRITE_MODE) == 0) return(gop_failure_status);
    if (op->n_iov <=0) return(gop_success_status);

    t1 = iov[0].len;
    t2 = iov[0].offset;
    log_printf(1, "START fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fd->path, op->n_iov, t1, t2);
    tbx_log_flush();
    if (tbx_log_level() > 0) {
        for (i=0; i < op->n_iov; i++) {
            t2 = iov[i].offset+iov[i].len-1;
            log_printf(1, "LFS_WRITE:START " XOT " " XOT "\n", iov[i].offset, t2);
            log_printf(1, "LFS_WRITE:END " XOT "\n", t2);
        }
    }

    now = apr_time_now();

    tbx_atomic_set(fd->fh->modified, 1);  //** Flag it as modified

    //** Do the write op
    err = gop_sync_exec(segment_write(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, op->buffer, op->boff, lc->timeout));

    //** Update the file position to thelast write
    segment_lock(fd->fh->seg);
    fd->curr_offset = iov[op->n_iov-1].offset+iov[op->n_iov-1].len;
    segment_unlock(fd->fh->seg);

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s seg=" XIDT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), dt);
    tbx_log_flush();

    if (fd->fh->write_table != NULL) {
        tbx_tbuf_t tb;
        lfs_adler32_t *a32;
        unsigned char *buf = NULL;
        ex_off_t blen = 0;
        ex_off_t bpos = op->boff;
        for (i=0; i < op->n_iov; i++) {
            tbx_type_malloc(a32, lfs_adler32_t, 1);
            a32->offset = iov[i].offset;
            a32->len = iov[i].len;
            a32->adler32 = adler32(0L, Z_NULL, 0);

            //** This is sloppy should use tbuffer_next to do this but this is all going ot be thrown away once we track
            //** down the gridftp plugin issue
            if (blen < a32->len) {
                if (buf != NULL) free(buf);
                blen = a32->len;
                tbx_type_malloc(buf, unsigned char, blen);
            }
            tbx_tbuf_single(&tb, a32->len, (char *)buf);
            tbx_tbuf_copy(buffer, bpos, &tb, 0, a32->len, 1);
            a32->adler32 = adler32(a32->adler32, buf, a32->len);
            segment_lock(fd->fh->seg);
            tbx_list_insert(fd->fh->write_table, &(a32->offset), a32);
            segment_unlock(fd->fh->seg);

            bpos += a32->len;
        }

        if (buf != NULL) free(buf);
    }

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR with write! fname=%s\n", fd->path);
        printf("got value %d\n", err);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        return(status);
    }

    size = iov[0].len;
    for (i=1; i< op->n_iov; i++) size += iov[i].len;
    _op_set_status(status, OP_STATE_SUCCESS, size);
    return(status);
}

//*************************************************************************

gop_op_generic_t *lio_write_ex_gop_fn(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = n_iov;
    op->iov = iov;
    op->buffer = buffer;
    op->boff = boff;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_write_ex_fn, (void *)op, free, 1));
}

//*************************************************************************

int lio_write_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t op;
    gop_op_status_t status;

    op.fd = fd;
    op.n_iov = n_iov;
    op.iov = ex_iov;
    op.buffer = buffer;
    op.boff = boff;
    op.rw_hints = rw_hints;

    status = lio_write_ex_fn((void *)&op, -1);
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_write_gopv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    tbx_type_malloc_clear(op, lio_rw_op_t, 1);
    ex_off_t offset;

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_vec(&(op->buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_write_ex_fn, (void *)op, free, 1));
}

//*************************************************************************

int lio_writev(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t op;
    ex_off_t offset;
    gop_op_status_t status;

    op.fd = fd;
    op.n_iov = 1;
    op.iov = &(op.iov_dummy);
    op.buffer = &(op.buffer_dummy);
    op.boff = 0;
    op.rw_hints = rw_hints;

    tbx_tbuf_vec(&(op.buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op.iov, offset, size);
    status = lio_write_ex_fn((void *)&op, -1);
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_write_gop(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    ex_off_t offset;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_single(op->buffer, size, buf);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_write_ex_fn, (void *)op, free, 1));
}

//*************************************************************************

int lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    ex_off_t offset;
    lio_rw_op_t op;
    gop_op_status_t status;

    op.fd = fd;
    op.n_iov = 1;
    op.iov = &(op.iov_dummy);
    op.buffer = &(op.buffer_dummy);
    op.boff = 0;
    op.rw_hints = rw_hints;

    tbx_tbuf_single(op.buffer, size, buf);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op.iov, offset, size);
    status = lio_write_ex_fn((void *)&op, -1);
    return(status.error_code);
}

//***********************************************************************
//  All the various LIO copy routines and truncate:)
//***********************************************************************

#define LIO_COPY_BUFSIZE (20*1024*1024)

typedef struct {
    FILE *sffd, *dffd;
    lio_fd_t *slfd, *dlfd;
    ex_off_t bufsize;
    char *buffer;
    lio_copy_hint_t hints;
    lio_segment_rw_hints_t *rw_hints;
} lio_cp_fn_t;

//***********************************************************************
// lio_cp_local2lio - Copies a local file to LIO
//***********************************************************************

gop_op_status_t lio_cp_local2lio_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    char *buffer;
    ex_off_t bufsize;
    lio_file_handle_t *lfh = op->dlfd->fh;
    FILE *ffd = op->sffd;

    buffer = op->buffer;
    bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;

    if (buffer == NULL) { //** Need to make it ourself
        tbx_type_malloc(buffer, char, bufsize+1);
    }

    status = gop_sync_exec_status(segment_put_gop(lfh->lc->tpc_unlimited, lfh->lc->da, op->rw_hints, ffd, lfh->seg, 0, -1, bufsize, buffer, 1, 3600));
    lfh->modified = 1; //** Flag it as modified so the new exnode gets stored

    //** Clean up
    if (op->buffer == NULL) free(buffer);

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_local2lio_gop(FILE *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->sffd = sfd;
    op->dlfd = dfd;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(dfd->lc->tpc_unlimited, NULL, lio_cp_local2lio_fn, (void *)op, free, 1));

}

//***********************************************************************
// lio_cp_lio2local - Copies a LIO file to a local file
//***********************************************************************

gop_op_status_t lio_cp_lio2local_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    char *buffer;
    ex_off_t bufsize;
    lio_file_handle_t *lfh = op->slfd->fh;
    FILE *ffd = op->dffd;

    buffer = op->buffer;
    bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;

    if (buffer == NULL) { //** Need to make it ourself
        tbx_type_malloc(buffer, char, bufsize+1);
    }

    status = gop_sync_exec_status(segment_get_gop(lfh->lc->tpc_unlimited, lfh->lc->da, op->rw_hints, lfh->seg, ffd, 0, -1, bufsize, buffer, 3600));

    //** Clean up
    if (op->buffer == NULL) free(buffer);

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_lio2local_gop(lio_fd_t *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->slfd = sfd;
    op->dffd = dfd;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(sfd->lc->tpc_unlimited, NULL, lio_cp_lio2local_fn, (void *)op, free, 1));
}


//***********************************************************************
// lio_cp_lio2lio - Copies a LIO file to another LIO file
//***********************************************************************

gop_op_status_t lio_cp_lio2lio_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    lio_file_handle_t *sfh = op->slfd->fh;
    lio_file_handle_t *dfh = op->dlfd->fh;
    char *buffer;
    ex_off_t bufsize;
    int used;
    const int sigsize = 10*1024;
    char sig1[sigsize], sig2[sigsize];


    //** Check if we can do a depot->depot direct copy
    used = 0;
    segment_signature(sfh->seg, sig1, &used, sigsize);
    used = 0;
    segment_signature(dfh->seg, sig2, &used, sigsize);

    if ((strcmp(sig1, sig2) == 0) && ((op->hints & LIO_COPY_INDIRECT) == 0)) {
        status = gop_sync_exec_status(segment_clone(sfh->seg, dfh->lc->da, &(dfh->seg), CLONE_STRUCT_AND_DATA, NULL, dfh->lc->timeout));
    } else {
        buffer = op->buffer;
        bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;

        if (buffer == NULL) { //** Need to make it ourself
            tbx_type_malloc(buffer, char, bufsize+1);
        }
        status = gop_sync_exec_status(lio_segment_copy_gop(dfh->lc->tpc_unlimited, dfh->lc->da, op->rw_hints, sfh->seg, dfh->seg, 0, 0, -1, bufsize, buffer, 1, dfh->lc->timeout));

        //** Clean up
        if (op->buffer == NULL) free(buffer);
    }

    dfh->modified = 1; //** Flag it as modified so the new exnode gets stored

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_lio2lio_gop(lio_fd_t *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, int hints, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->slfd = sfd;
    op->dlfd = dfd;
    op->hints = hints;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(dfd->lc->tpc_unlimited, NULL, lio_cp_lio2lio_fn, (void *)op, free, 1));
}

//*************************************************************************
// lio_file_copy_op - Actual cp function.  Copies a regex to a dest *dir*
//*************************************************************************

gop_op_status_t lio_file_copy_op(void *arg, int id)
{
    lio_cp_file_t *cp = (lio_cp_file_t *)arg;
    gop_op_status_t status, close_status;
    FILE *sffd, *dffd;
    lio_fd_t *slfd, *dlfd;
    char *buffer;

    buffer = NULL;

    if ((cp->src_tuple.is_lio == 0) && (cp->dest_tuple.is_lio == 0)) {  //** Not allowed to both go to disk
        info_printf(lio_ifd, 0, "Both source(%s) and destination(%s) are local files!\n", cp->src_tuple.path, cp->dest_tuple.path);
        return(gop_failure_status);
    }


    if (cp->src_tuple.is_lio == 0) {  //** Source is a local file and dest is lio

        sffd = fopen(cp->src_tuple.path, "r");
        info_printf(lio_ifd, 0, "copy %s %s@%s:%s\n", cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);

        gop_sync_exec(lio_open_gop(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, lio_fopen_flags("w"), NULL, &dlfd, 60));

        if ((sffd == NULL) || (dlfd == NULL)) { //** Got an error
            if (sffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
            if (dlfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
            status = gop_failure_status;
        } else {
            tbx_type_malloc(buffer, char, cp->bufsize+1);
            status = gop_sync_exec_status(lio_cp_local2lio_gop(sffd, dlfd, cp->bufsize, buffer, cp->rw_hints));
        }
        if (dlfd != NULL) {
            close_status = gop_sync_exec_status(lio_close_gop(dlfd));
            if (close_status.op_status != OP_STATE_SUCCESS) status = close_status;
        }
        if (sffd != NULL) fclose(sffd);
    } else if (cp->dest_tuple.is_lio == 0) {  //** Source is lio and dest is local
        info_printf(lio_ifd, 0, "copy %s@%s:%s %s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, cp->dest_tuple.path);
        gop_sync_exec(lio_open_gop(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, lio_fopen_flags("r"), NULL, &slfd, 60));
        dffd = fopen(cp->dest_tuple.path, "w");

        if ((dffd == NULL) || (slfd == NULL)) { //** Got an error
            if (slfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
            if (dffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
            status = gop_failure_status;
        } else {
            tbx_type_malloc(buffer, char, cp->bufsize+1);
            status = gop_sync_exec_status(lio_cp_lio2local_gop(slfd, dffd, cp->bufsize, buffer, cp->rw_hints));
        }
        if (slfd != NULL) gop_sync_exec(lio_close_gop(slfd));
        if (dffd != NULL) fclose(dffd);
    } else {               //** both source and dest are lio
        info_printf(lio_ifd, 0, "copy %s@%s:%s %s@%s:%s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);
        gop_sync_exec(lio_open_gop(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, LIO_READ_MODE, NULL, &slfd, 60));
        gop_sync_exec(lio_open_gop(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, lio_fopen_flags("w"), NULL, &dlfd, 60));
        if ((dlfd == NULL) || (slfd == NULL)) { //** Got an error
            if (slfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
            if (dlfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
            status = gop_failure_status;
        } else {
            tbx_type_malloc(buffer, char, cp->bufsize+1);
            status = gop_sync_exec_status(lio_cp_lio2lio_gop(slfd, dlfd, cp->bufsize, buffer, cp->slow, cp->rw_hints));
        }
        if (slfd != NULL) gop_sync_exec(lio_close_gop(slfd));
        if (dlfd != NULL) {
            close_status = gop_sync_exec_status(lio_close_gop(dlfd));
            if (close_status.op_status != OP_STATE_SUCCESS) status = close_status;
        }
    }

    if (buffer != NULL) free(buffer);

    return(status);
}

//*************************************************************************
// lio_cp_create_dir - Ensures the new directory exists and updates the valid
//     dir table
//*************************************************************************

int lio_cp_create_dir(tbx_list_t *table, lio_path_tuple_t tuple)
{
    int i, n, err, error_code, skip_insert;
    struct stat s;
    char *dname = tuple.path;
    char *dstate;

    error_code = 0;
    n = strlen(dname);
    for (i=1; i<n; i++) {
        if ((dname[i] == '/') || (i==n-1)) {
            dstate = tbx_list_search(table, dname);
            if (dstate == NULL) {  //** Need to make the dir
                skip_insert = 0;
                if (i<n-1) dname[i] = 0;
                if (tuple.is_lio == 0) { //** Local dir
                    err = mkdir(dname, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
                    if (err != 0) { //** Check if it was already created by someone else
                        err = stat(dname, &s);
                    }
                    err = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
                } else {
                    err = gop_sync_exec(lio_create_gop(tuple.lc, tuple.creds, dname, OS_OBJECT_DIR_FLAG, NULL, NULL));
                    if (err != OP_STATE_SUCCESS) {  //** See if it was created by someone else
                        err = lio_exists(tuple.lc, tuple.creds, dname);
                        err = ((err & OS_OBJECT_DIR_FLAG) > 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
                        skip_insert = 1;  //** Either an error or it already exists so don't add it to the list
                    }
                }

                //** Add the path to the table
                if (err != OP_STATE_SUCCESS) error_code = 1;
                if (skip_insert == 0) tbx_list_insert(table, dname, dname);

                if (i<n-1) dname[i] = '/';
            }
        }
    }

    return(error_code);
}

//*************************************************************************
// lio_path_copy_op - Copies a regex to a dest *dir*
//*************************************************************************

gop_op_status_t lio_path_copy_op(void *arg, int id)
{
    lio_cp_path_t *cp = (lio_cp_path_t *)arg;
    lio_unified_object_iter_t *it;
    lio_path_tuple_t create_tuple;
    int ftype, prefix_len, slot, count, nerr;
    char *dstate;
    char dname[OS_PATH_MAX];
    char *fname, *dir, *file;
    tbx_list_t *dir_table;
    lio_cp_file_t *cplist, *c;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    gop_op_status_t status;

    log_printf(15, "START src=%s dest=%s max_spawn=%d bufsize=" XOT "\n", cp->src_tuple.path, cp->dest_tuple.path, cp->max_spawn, cp->bufsize);
    tbx_log_flush();

    it = lio_unified_object_iter_create(cp->src_tuple, cp->path_regex, cp->obj_regex, cp->obj_types, cp->recurse_depth);
    if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation src_path=%s\n", cp->src_tuple.path);
        return(gop_failure_status);
    }

    tbx_type_malloc_clear(cplist, lio_cp_file_t, cp->max_spawn);
    dir_table = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, NULL);

    q = gop_opque_new();
    nerr = 0;
    slot = 0;
    count = 0;
    while ((ftype = lio_unified_next_object(it, &fname, &prefix_len)) > 0) {
        snprintf(dname, OS_PATH_MAX, "%s/%s", cp->dest_tuple.path, &(fname[prefix_len+1]));
//info_printf(lio_ifd, 0, "copy dtuple=%s sfname=%s  dfname=%s plen=%d\n", cp->dest_tuple.path, fname, dname, prefix_len);

        if ((ftype & OS_OBJECT_DIR_FLAG) > 0) { //** Got a directory
            dstate = tbx_list_search(dir_table, fname);
            if (dstate == NULL) { //** New dir so have to check and possibly create it
                create_tuple = cp->dest_tuple;
                create_tuple.path = fname;
                lio_cp_create_dir(dir_table, create_tuple);
            }

            free(fname);  //** Clean up
            continue;  //** Nothing else to do so go to the next file.
        }

        lio_os_path_split(dname, &dir, &file);
        dstate = tbx_list_search(dir_table, dir);
        if (dstate == NULL) { //** New dir so have to check and possibly create it
            create_tuple = cp->dest_tuple;
            create_tuple.path = dir;
            lio_cp_create_dir(dir_table, create_tuple);
        }
        if (dir) {
            free(dir);
            dir = NULL;
        }
        if (file) {
            free(file);
            file = NULL;
        }

        c = &(cplist[slot]);
        c->src_tuple = cp->src_tuple;
        c->src_tuple.path = fname;
        c->dest_tuple = cp->dest_tuple;
        c->dest_tuple.path = strdup(dname);
        c->bufsize = cp->bufsize;
        c->slow = cp->slow;

        gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, lio_file_copy_op, (void *)c, NULL, 1);
        gop_set_myid(gop, slot);
        log_printf(1, "gid=%d i=%d sname=%s dname=%s\n", gop_id(gop), slot, fname, dname);
        gop_opque_add(q, gop);

        count++;

        if (count >= cp->max_spawn) {
            gop = opque_waitany(q);
            slot = gop_get_myid(gop);
            c = &(cplist[slot]);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                nerr++;
                info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path);
            }
            free(c->src_tuple.path);
            free(c->dest_tuple.path);
            gop_free(gop, OP_DESTROY);
        } else {
            slot = count;
        }
    }

    lio_unified_object_iter_destroy(it);

    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        slot = gop_get_myid(gop);
        c = &(cplist[slot]);
        log_printf(15, "slot=%d fname=%s\n", slot, c->src_tuple.path);
        if (status.op_status != OP_STATE_SUCCESS) {
            nerr++;
            info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path);
        }
        free(c->src_tuple.path);
        free(c->dest_tuple.path);
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);

    free(cplist);
    tbx_list_destroy(dir_table);

    status = gop_success_status;
    if (nerr > 0) {
        status.op_status = OP_STATE_FAILURE;
        status.error_code = nerr;
    }
    return(status);
}


//***********************************************************************
// The misc I/O routines: lio_seek, lio_tell, lio_size
//***********************************************************************


//***********************************************************************
// lio_seek - Sets the file position
//***********************************************************************

ex_off_t lio_seek(lio_fd_t *fd, ex_off_t offset, int whence)
{
    ex_off_t moveto;

    switch (whence) {
    case (SEEK_SET) :
        moveto = offset;
        break;
    case (SEEK_CUR) :
        segment_lock(fd->fh->seg);
        moveto = offset + fd->curr_offset;
        segment_unlock(fd->fh->seg);
        break;
    case (SEEK_END) :
        moveto = segment_size(fd->fh->seg) - offset;
        break;
    default :
        return(-EINVAL);
    }

    //** Check if the seek is out of range
    if (moveto < 0) return(-ERANGE);

    segment_lock(fd->fh->seg);
    fd->curr_offset = moveto;
    segment_lock(fd->fh->seg);

    return(moveto);
}

//***********************************************************************
// lio_tell - Return the current position
//***********************************************************************

ex_off_t lio_tell(lio_fd_t *fd)
{
    ex_off_t offset;

    segment_lock(fd->fh->seg);
    offset = fd->curr_offset;
    segment_lock(fd->fh->seg);

    return(offset);
}


//***********************************************************************
// lio_size - Return the file size
//***********************************************************************

ex_off_t lio_size(lio_fd_t *fd)
{
    return(segment_size(fd->fh->seg));
}

//***********************************************************************
// lio_truncate - Truncates an open LIO file
//***********************************************************************

gop_op_status_t lio_truncate_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    lio_file_handle_t *fh = op->slfd->fh;

    if (op->bufsize != segment_size(fh->seg)) {
        status = gop_sync_exec_status(lio_segment_truncate(fh->seg, fh->lc->da, op->bufsize, fh->lc->timeout));
        segment_lock(fh->seg);
        fh->modified = 1;
        op->slfd->curr_offset = op->bufsize;
        segment_unlock(fh->seg);
    } else {
        //** Move the FD to the end
        segment_lock(fh->seg);
        op->slfd->curr_offset = op->bufsize;
        segment_unlock(fh->seg);
        status = gop_success_status;
    }

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_truncate_gop(lio_fd_t *fd, ex_off_t newsize)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->bufsize = newsize;
    op->slfd = fd;

    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_truncate_fn, (void *)op, free, 1));
}


