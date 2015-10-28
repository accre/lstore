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

#define _log_module_index 189

#include "type_malloc.h"
#include "lio.h"
#include "log.h"
#include "string_token.h"

#define _n_fsck_keys 4
static char *_fsck_keys[] = { "system.owner", "system.inode", "system.exnode", "system.exnode.size" };

#define _n_lio_file_keys 7
#define _n_lio_dir_keys 6
#define _n_lio_create_keys 7

static char *_lio_create_keys[] = { "system.owner", "os.timestamp.system.create", "os.timestamp.system.modify_data",
                                    "os.timestamp.system.modify_attr", "system.inode", "system.exnode", "system.exnode.size"
                                  };

char *_lio_stat_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count", "os.link" };

typedef struct {
    lio_config_t *lc;
    creds_t *creds;
    char *src_path;
    char *dest_path;
    char *id;
    char *ex;
    int type;
} lio_mk_mv_rm_t;

typedef struct {
    lio_config_t *lc;
    creds_t *creds;
    os_regex_table_t *rpath;
    os_regex_table_t *robj;
    int recurse_depth;
    int obj_types;
    int np;
} lio_remove_regex_t;

typedef struct {
    char *fname;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    int ftype;
} lio_fsck_task_t;

struct lio_fsck_iter_s {
    lio_config_t *lc;
    creds_t *creds;
    char *path;
    os_regex_table_t *regex;
    os_object_iter_t *it;
    int owner_mode;
    int exnode_mode;
    char *owner;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    lio_fsck_task_t *task;
    opque_t *q;
    int n;
    int firsttime;
    ex_off_t visited_count;
};

typedef struct {
    lio_config_t *lc;
    creds_t *creds;
    char *path;
    char **val;
    int *v_size;
    int ftype;
    int full;
    int owner_mode;
    int exnode_mode;
    char *owner;
} lio_fsck_check_t;

int ex_id_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b);
skiplist_compare_t ex_id_compare = {.fn=ex_id_compare_fn, .arg=NULL };

//***********************************************************************
// Core LFS functionality
//***********************************************************************

//************************************************************************
//  ex_id_compare_fn  - ID comparison function
//************************************************************************

int ex_id_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b)
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

//***********************************************************************
// gop_lio_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

op_generic_t *gop_lio_exists(lio_config_t *lc, creds_t *creds, char *path)
{
    return(os_exists(lc->os, creds, path));
}

//***********************************************************************
// lio_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

int lio_exists(lio_config_t *lc, creds_t *creds, char *path)
{
    op_status_t status;

    status = gop_sync_exec_status(os_exists(lc->os, creds, path));
    return(status.error_code);
}

//***********************************************************************
// lio_free_mk_mv_rm
//***********************************************************************

void lio_free_mk_mv_rm(void *arg)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;

    if (op->src_path != NULL) free(op->src_path);
    if (op->dest_path != NULL) free(op->dest_path);
    if (op->id != NULL) free(op->id);
    if (op->ex != NULL) free(op->ex);

    free(op);
}

//***********************************************************************
// lio_create_object_fn - Does the actual object creation
//***********************************************************************

op_status_t lio_create_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    os_fd_t *fd;
    char *dir, *fname;
    exnode_exchange_t *exp;
    exnode_t *ex, *cex;
    ex_id_t ino;
    char inode[32];
    char *val[_n_lio_create_keys];
    op_status_t status;
    int v_size[_n_lio_create_keys];
    int err;
    int ex_key = 5;

    status = op_success_status;

    val[ex_key] = NULL;

    log_printf(15, "START op->ex=%p !!!!!!!!!\n fname=%s\n",  op->ex, op->src_path);

    //** Make the base object
    err = gop_sync_exec(os_create_object(op->lc->os, op->creds, op->src_path, op->type, op->id));
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR creating object fname=%s\n", op->src_path);
        status = op_failure_status;
        goto fail_bad;
    }

    //** Get the parent exnode to dup
    if (op->ex == NULL) {
        os_path_split(op->src_path, &dir, &fname);
        log_printf(15, "dir=%s\n fname=%s\n", dir, fname);

        err = gop_sync_exec(os_open_object(op->lc->os, op->creds, dir, OS_MODE_READ_IMMEDIATE, op->id, &fd, op->lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR opening parent=%s\n", dir);
            free(dir);
            status = op_failure_status;
            goto fail;
        }
        free(fname);

        v_size[0] = -op->lc->max_attr;
        err = gop_sync_exec(os_get_attr(op->lc->os, op->creds, fd, "system.exnode", (void **)&(val[ex_key]), &(v_size[0])));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR opening parent=%s\n", dir);
            free(dir);
            status = op_failure_status;
            goto fail;
        }

        //** Close the parent
        err = gop_sync_exec(os_close_object(op->lc->os, fd));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR closing parent fname=%s\n", dir);
            free(dir);
            status = op_failure_status;
            goto fail;
        }

        free(dir);
    } else {
        val[ex_key] = op->ex;
    }

    if (val[ex_key] == NULL) { //** Oops no valid exnode!
        log_printf(0, "ERROR No valid exnode could be located.  fname=%s\n", op->src_path);
        status = op_failure_status;
        goto fail;
    }

    //** For a directory we can just copy the exnode.  For a file we have to
    //** Clone it to get unique IDs
    if ((op->type & OS_OBJECT_DIR) == 0) {
        //** If this has a caching segment we need to disable it from being added
        //** to the global cache table cause there could be multiple copies of the
        //** same segment being serialized/deserialized.

        //** Deserialize it
        exp = exnode_exchange_text_parse(val[ex_key]);
        ex = exnode_create();
        if (exnode_deserialize(ex, exp, op->lc->ess_nocache) != 0) {
            log_printf(15, "ERROR parsing parent exnode fname=%s\n", dir);
            status = op_failure_status;
            exnode_exchange_destroy(exp);
            exnode_destroy(ex);
            goto fail;
        }

        //** Execute the clone operation
        err = gop_sync_exec(exnode_clone(op->lc->tpc_unlimited, ex, op->lc->da, &cex, NULL, CLONE_STRUCTURE, op->lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR cloning parent fname=%s\n", dir);
            status = op_failure_status;
            exnode_exchange_destroy(exp);
            exnode_destroy(ex);
            exnode_destroy(cex);
            goto fail;
        }

        //** Serialize it for storage
        exnode_exchange_free(exp);
        exnode_serialize(cex, exp);
        val[ex_key] = exp->text.text;
        exp->text.text = NULL;
        exnode_exchange_destroy(exp);
        exnode_destroy(ex);
        exnode_destroy(cex);
    }


    //** Open the object so I can add the required attributes
    err = gop_sync_exec(os_open_object(op->lc->os, op->creds, op->src_path, OS_MODE_WRITE_IMMEDIATE, op->id, &fd, op->lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR opening object fname=%s\n", op->src_path);
        status = op_failure_status;
        goto fail;
    }

    //** Now add the required attributes
    val[0] = an_cred_get_id(op->creds);
    v_size[0] = strlen(val[0]);
    val[1] = op->id;
    v_size[1] = (op->id == NULL) ? 0 : strlen(op->id);
    val[2] = op->id;
    v_size[2] = v_size[1];
    val[3] = op->id;
    v_size[3] = v_size[1];
    ino = 0;
    generate_ex_id(&ino);
    snprintf(inode, 32, XIDT, ino);
    val[4] = inode;
    v_size[4] = strlen(inode);
    v_size[ex_key] = strlen(val[ex_key]);
    val[6] = "0";
    v_size[6] = 1;

    log_printf(15, "NEW ino=%s exnode=%s\n", val[4], val[ex_key]);
    flush_log();

    err = gop_sync_exec(os_set_multiple_attrs(op->lc->os, op->creds, fd, _lio_create_keys, (void **)val, v_size, (op->type & OS_OBJECT_FILE) ? _n_lio_file_keys : _n_lio_dir_keys));
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR setting default attr fname=%s\n", op->src_path);
        status = op_failure_status;
    }


    //** Close the file
    err = gop_sync_exec(os_close_object(op->lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR closing object fname=%s\n", op->src_path);
        status = op_failure_status;
    }

fail:
    if (status.op_status != OP_STATE_SUCCESS) gop_sync_exec(os_remove_object(op->lc->os, op->creds, op->src_path));

fail_bad:
    if (val[ex_key] != NULL) free(val[ex_key]);

    return(status);
}

//*************************************************************************
//  gop_lio_create_object - Generate a create object task
//*************************************************************************

op_generic_t *gop_lio_create_object(lio_config_t *lc, creds_t *creds, char *path, int type, char *ex, char *id)
{
    lio_mk_mv_rm_t *op;

    type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = type;
    op->id = (id != NULL) ? strdup(id) : NULL;
    op->ex = (ex != NULL) ? strdup(ex) : NULL;
    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_create_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}

//***********************************************************************
// lio_remove_object - Removes an object
//***********************************************************************

op_status_t lio_remove_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    char *ex_data, *val[2];
    char *hkeys[] = { "os.link_count", "system.exnode" };
    exnode_exchange_t *exp;
    exnode_t *ex;
    int err, v_size, ex_remove, vs[2], n;
    op_status_t status = op_success_status;

    //** First remove and data associated with the object
    v_size = -op->lc->max_attr;

    //** If no object type need to retrieve it
    if (op->type == 0) op->type = lio_exists(op->lc, op->creds, op->src_path);

    ex_remove = 0;
    if ((op->type & OS_OBJECT_HARDLINK) > 0) { //** Got a hard link so check if we do a data removal
        val[0] = val[1] = NULL;
        vs[0] = vs[1] = -op->lc->max_attr;
        lio_get_multiple_attrs(op->lc, op->creds, op->src_path, op->id, hkeys, (void **)val, vs, 2);

        if (val[0] == NULL) {
            log_printf(15, "Missing link count for fname=%s\n", op->src_path);
            if (val[1] != NULL) free(val[1]);
            return(op_failure_status);
        }

        n = 100;
        sscanf(val[0], "%d", &n);
        free(val[0]);
        if (n <= 1) {
            ex_remove = 1;
            if (op->ex == NULL) {
                op->ex = val[1];
            } else {
                if (val[1] != NULL) free(val[1]);
            }
        } else {
            if (val[1] != NULL) free(val[1]);
        }
    } else if ((op->type & (OS_OBJECT_SYMLINK|OS_OBJECT_DIR)) == 0) {
        ex_remove = 1;
    }

    ex_data = op->ex;
    if ((op->ex == NULL) && (ex_remove == 1)) {
        lio_get_attr(op->lc, op->creds, op->src_path, op->id, "system.exnode", (void **)&ex_data, &v_size);
    }

    //** Remove the OS entry first.  This way if it fails we'll just kick out and the data is still good.
    err = gop_sync_exec(os_remove_object(op->lc->os, op->creds, op->src_path));
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR: removing file: %s err=%d\n", op->src_path, err);
        status = op_failure_status;
        return(status);
    }

    //** Load the exnode and remove it if needed.
    //** Only done for normal files.  No links or dirs
    if ((ex_remove == 1) && (ex_data != NULL)) {
        //** Deserialize it
        exp = exnode_exchange_text_parse(ex_data);
        ex = exnode_create();
        if (exnode_deserialize(ex, exp, op->lc->ess) != 0) {
            log_printf(15, "ERROR removing data for object fname=%s\n", op->src_path);
            status = op_failure_status;
        } else {  //** Execute the remove operation since we have a good exnode
            err = gop_sync_exec(exnode_remove(op->lc->tpc_unlimited, ex, op->lc->da, op->lc->timeout));
            if (err != OP_STATE_SUCCESS) {
                log_printf(15, "ERROR removing data for object fname=%s\n", op->src_path);
                status = op_failure_status;
            }
        }

        //** Clean up
        if (op->ex != NULL) exp->text.text = NULL;  //** The inital exnode is free()-ed by the TP op
        exnode_exchange_destroy(exp);
        exnode_destroy(ex);
    }

    return(status);
}

//*************************************************************************
// gop_lio_remove_object
//*************************************************************************

op_generic_t *gop_lio_remove_object(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional)
{
    lio_mk_mv_rm_t *op;

    type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->ex = ex_optional;
    op->type = ftype_optional;
    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_remove_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}

//***********************************************************************
// lio_remove_regex_object - Removes objects using regex's
//***********************************************************************

op_status_t lio_remove_regex_object_fn(void *arg, int id)
{
    lio_remove_regex_t *op = (lio_remove_regex_t *)arg;
    os_object_iter_t *it;
    opque_t *q;
    op_generic_t *gop;
    int n, nfailed, atype, prefix_len;
    char *ex, *fname;
    char *key[1];
    int v_size[1];
    op_status_t status2;
    op_status_t status = op_success_status;

    key[0] = "system.exnode";
    ex = NULL;
    v_size[0] = -op->lc->max_attr;
    it = os_create_object_iter_alist(op->lc->os, op->creds, op->rpath, op->robj, op->obj_types, op->recurse_depth, key, (void **)&ex, v_size, 1);
    if (it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation\n");
        return(op_failure_status);
    }

    //** Cycle through removing the objects
    q = new_opque();
    n = 0;
    nfailed = 0;
    while ((atype = os_next_object(op->lc->os, it, &fname, &prefix_len)) > 0) {

        //** If it's a directory so we need to flush all existing rm's first
        //** Otherwire the rmdir will see pending files
        if ((atype & OS_OBJECT_DIR) > 0) {
            opque_waitall(q);
        }

        gop = gop_lio_remove_object(op->lc, op->creds, fname, ex, atype);
        ex = NULL;  //** Freed in lio_remove_object
        free(fname);
        opque_add(q, gop);

        if (opque_tasks_left(q) > op->np) {
            gop = opque_waitany(q);
            status2 = gop_get_status(gop);
            if (status2.op_status != OP_STATE_SUCCESS) {
                printf("Failed with gid=%d\n", gop_id(gop));
                nfailed++;
            }
            gop_free(gop, OP_DESTROY);
        }

        n++;
    }

    os_destroy_object_iter(op->lc->os, it);

    opque_waitall(q);
    nfailed += opque_tasks_failed(q);
    opque_free(q, OP_DESTROY);

    status.op_status = (nfailed > 0) ? OP_STATE_FAILURE : OP_STATE_SUCCESS;
    status.error_code = n;
    return(status);
}

//*************************************************************************
// gop_lio_remove_regex_object
//*************************************************************************

op_generic_t *gop_lio_remove_regex_object(lio_config_t *lc, creds_t *creds, os_regex_table_t *rpath, os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np)
{
    lio_remove_regex_t *op;

    type_malloc_clear(op, lio_remove_regex_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->rpath = rpath;
    op->robj = object_regex;
    op->obj_types = obj_types;
    op->recurse_depth = recurse_depth;
    op->np = np;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_remove_regex_object_fn, (void *)op, free, 1));
}

//*************************************************************************
// gop_lio_regex_object_set_multiple_attrs - Sets multiple object attributes
//*************************************************************************

op_generic_t *gop_lio_regex_object_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n)
{
    return(os_regex_object_set_multiple_attrs(lc->os, creds, id, path, object_regex, object_types, recurse_depth, key, val, v_size, n));
}

//*************************************************************************
// gop_lio_abort_regex_object_set_multiple_attrs - Aborts an ongoing set attr call
//*************************************************************************

op_generic_t *gop_lio_abort_regex_object_set_multiple_attrs(lio_config_t *lc, op_generic_t *gop)
{
    return(os_abort_regex_object_set_multiple_attrs(lc->os, gop));
}

//*************************************************************************
// gop_lio_move_object - Renames an object
//*************************************************************************

op_generic_t *gop_lio_move_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path)
{
    return(os_move_object(lc->os, creds, src_path, dest_path));
}


//***********************************************************************
// lio_link_object_fn - Does the actual object creation
//***********************************************************************

op_status_t lio_link_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    os_fd_t *dfd;
    opque_t *q;
    int err;
    ex_id_t ino;
    char inode[32];
    op_status_t status;
    char *lkeys[] = {"system.exnode", "system.exnode.size"};
    char *spath[2];
    char *vkeys[] = {"system.owner", "system.inode", "os.timestamp.system.create", "os.timestamp.system.modify_data", "os.timestamp.system.modify_attr"};
    char *val[5];
    int vsize[5];

    //** Link the base object
    if (op->type == 1) { //** Symlink
        err = gop_sync_exec(os_symlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
    } else {
        err = gop_sync_exec(os_hardlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
    }
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR linking base object sfname=%s dfname=%s\n", op->src_path, op->dest_path);
        status = op_failure_status;
        goto finished;
    }

    if (op->type == 0) {  //** HArd link so exit
        status = op_success_status;
        goto finished;
    }

    q = new_opque();

    //** Open the Destination object
    opque_add(q, os_open_object(op->lc->os, op->creds, op->dest_path, OS_MODE_READ_IMMEDIATE, op->id, &dfd, op->lc->timeout));
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR opening src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        status = op_failure_status;
        goto open_fail;
    }

    //** Now link the exnode and size
    spath[0] = op->src_path;
    spath[1] = op->src_path;
    opque_add(q, os_symlink_multiple_attrs(op->lc->os, op->creds, spath, lkeys, dfd, lkeys, 2));

    //** Store the owner, inode, and dates
    val[0] = an_cred_get_id(op->creds);
    vsize[0] = strlen(val[0]);
    ino = 0;
    generate_ex_id(&ino);
    snprintf(inode, 32, XIDT, ino);
    val[1] = inode;
    vsize[1] = strlen(inode);
    val[2] = op->id;
    vsize[2] = (op->id == NULL) ? 0 : strlen(op->id);
    val[3] = op->id;
    vsize[3] = vsize[2];
    val[4] = op->id;
    vsize[4] = vsize[2];
    opque_add(q, os_set_multiple_attrs(op->lc->os, op->creds, dfd, vkeys, (void **)val, vsize, 5));


    //** Wait for everything to complete
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR with attr link or owner set src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        status = op_failure_status;
        goto open_fail;
    }

    status = op_success_status;

open_fail:
    if (dfd != NULL) opque_add(q, os_close_object(op->lc->os, dfd));
    opque_waitall(q);

    opque_free(q, OP_DESTROY);

finished:
    return(status);

}

//***********************************************************************
// gop_lio_link_object - Generates a link object task
//***********************************************************************

op_generic_t *gop_lio_link_object(lio_config_t *lc, creds_t *creds, int symlink, char *src_path, char *dest_path, char *id)
{
    lio_mk_mv_rm_t *op;

    type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->type = symlink;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);
    op->id = (id != NULL) ? strdup(id) : NULL;
    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_link_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}


//*************************************************************************
//  gop_lio_symlink_object - Create a symbolic link to another object
//*************************************************************************

op_generic_t *gop_lio_symlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id)
{
    return(gop_lio_link_object(lc, creds, 1, src_path, dest_path, id));
}


//*************************************************************************
//  gop_lio_hardlink_object - Create a hard link to another object
//*************************************************************************

op_generic_t *gop_lio_hardlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id)
{
    return(gop_lio_link_object(lc, creds, 1, src_path, dest_path, id));
}



//*************************************************************************
// lio_create_object_iter - Creates an object iterator using a regex for the attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max)
{
    return(os_create_object_iter(lc->os, creds, path, obj_regex, object_types, attr, recurse_dpeth, it, v_max));
}

//*************************************************************************
// lio_create_object_iter_alist - Creates an object iterator using a fixed attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter_alist(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    return(os_create_object_iter_alist(lc->os, creds, path, obj_regex, object_types, recurse_depth, key, val, v_size, n_keys));
}


//*************************************************************************
// lio_next_object - Returns the next iterator object
//*************************************************************************

int lio_next_object(lio_config_t *lc, os_object_iter_t *it, char **fname, int *prefix_len)
{
    return(os_next_object(lc->os, it, fname, prefix_len));
}


//*************************************************************************
// lio_destroy_object_iter - Destroy's an object iterator
//*************************************************************************

void lio_destroy_object_iter(lio_config_t *lc, os_object_iter_t *it)
{
    os_destroy_object_iter(lc->os, it);
}


//***********************************************************************
// lio_*_attrs - Get/Set LIO attribute routines
//***********************************************************************

typedef struct {
    lio_config_t *lc;
    creds_t *creds;
    const char *path;
    char *id;
    char **mkeys;
    void **mvals;
    int *mv_size;
    char *skey;
    void *sval;
    int *sv_size;
    int n_keys;
} lio_attrs_op_t;

//***********************************************************************
// lio_get_multiple_attrs
//***********************************************************************

int lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        return(err);
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    serr = gop_sync_exec(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR getting attributes object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************

op_status_t lio_get_multiple_attrs_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    op_status_t status;
    int err;

    err = lio_get_multiple_attrs(op->lc, op->creds, (char *)op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
    status.error_code = err;
    status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
    return(status);
}

//***********************************************************************

op_generic_t *gop_lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->mkeys = key;
    op->mvals = val;
    op->mv_size = v_size;
    op->n_keys = n_keys;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_get_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_get_attr - Returns an attribute
//***********************************************************************

int lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        return(err);
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    serr = gop_sync_exec(os_get_attr(lc->os, creds, fd, key, val, v_size));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR getting attribute object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************

op_status_t lio_get_attr_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    op_status_t status;
    int err;

    err = lio_get_attr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->sv_size);
    status.error_code = 0;
    status.op_status = err;
    return(status);
}

//***********************************************************************

op_generic_t *gop_lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->skey = key;
    op->sval = val;
    op->sv_size = v_size;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_get_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_set_multiple_attrs_real - Returns an attribute
//***********************************************************************

int lio_set_multiple_attrs_real(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        return(err);
    }

    serr = gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR setting attributes object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************
// lio_set_multiple_attrs - Returns an attribute
//***********************************************************************

int lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
    int err;

    err = lio_set_multiple_attrs_real(lc, creds, path, id, key, val, v_size, n);
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = lio_set_multiple_attrs_real(lc, creds, path, id, key, val, v_size, n);
    }

    return(err);
}

//***********************************************************************

op_status_t lio_set_multiple_attrs_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    op_status_t status;
    int err;

    err = lio_set_multiple_attrs(op->lc, op->creds, op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
    status.error_code = err;
    status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
    return(status);
}

//***********************************************************************

op_generic_t *gop_lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->mkeys = key;
    op->mvals = val;
    op->mv_size = v_size;
    op->n_keys = n_keys;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_set_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_set_attr_real - Sets an attribute
//***********************************************************************

int lio_set_attr_real(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        return(err);
    }

    serr = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR setting attribute object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************
// lio_set_attr - Sets a single attribute
//***********************************************************************

int lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    int err;

    err = lio_set_attr_real(lc, creds, path, id, key, val, v_size);
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = lio_set_attr_real(lc, creds, path, id, key, val, v_size);
    }

    return(err);
}

//***********************************************************************

op_status_t lio_set_attr_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    op_status_t status;

    status.op_status = lio_set_attr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->n_keys); //** NOTE: n_keys = v_size
    status.error_code = 0;
    return(status);
}

//***********************************************************************

op_generic_t *gop_lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->skey = key;
    op->sval = val;
    op->n_keys = v_size;  //** Double use for the vaiable

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_set_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_create_attr_iter - Creates an attribute iterator
//***********************************************************************

os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, creds_t *creds, const char *path, os_regex_table_t *attr, int v_max)
{
    return(os_create_attr_iter(lc->os, creds, (char *)path, attr, v_max));
}

//***********************************************************************
// lio_next_attr - Returns the next attribute from the iterator
//***********************************************************************

int lio_next_attr(lio_config_t *lc, os_attr_iter_t *it, char **key, void **val, int *v_size)
{
    return(os_next_attr(lc->os, it, key, val, v_size));
}

//***********************************************************************
// lio_destroy_attr_iter - Destroy the attribute iterator
//***********************************************************************

void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it)
{
    os_destroy_attr_iter(lc->os, it);
}

//*************************************************************************
// ftype_lio2posix - Converts a LIO filetype to a posix mode
//*************************************************************************

mode_t ftype_lio2posix(int ftype)
{
    mode_t mode;

    mode = 0;
    if (ftype & OS_OBJECT_SYMLINK) {
        mode = S_IFLNK | 0777;
    } else if (ftype & OS_OBJECT_DIR) {
        mode = S_IFDIR | 0755;
    } else {
//     mode = S_IFREG | 0444;
        mode = S_IFREG | 0666;  //** Make it so that everything has RW access
    }

    return(mode);
}

//*************************************************************************
// _lio_parse_stat_vals - Parses the stat values received
//   NOTE: All the val[*] strings are free'ed!
//*************************************************************************

void _lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char *mount_prefix, char **flink)
{
    int i, n, readlink;
    char *link;
    ex_id_t ino;
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
    if (val[1] != NULL) lio_get_timestamp(val[2], &ts, NULL);
    stat->st_ctime = ts;
    stat->st_atime = stat->st_ctime;

    //** Get the symlink if it exists and optionally store it
    readlink = 0;
    if (val[6] != NULL) {
        link = val[6];
        readlink = strlen(link);
        log_printf(15, "inode->link=%s mount_point=%s moun_point_len=%d\n", link, mount_prefix, strlen(mount_prefix));
        if (link[0] == '/') { //** If an absolute link then we need to add the mount prefix back
            readlink += strlen(mount_prefix) + 1;
            if (flink != NULL) {
                type_malloc(*flink, char, readlink+1);
                snprintf(*flink, readlink+1, "%s%s", mount_prefix, link);
            }
        } else if (flink != NULL) {
            *flink = link;
            val[6] = NULL;  //** Don't want to delete it on cleanup
        }

    }

    //** File types
    n = 0;
    if (val[4] != NULL) sscanf(val[4], "%d", &n);
    stat->st_mode = ftype_lio2posix(n);

    len = 0;
    if (val[3] != NULL) sscanf(val[3], XOT, &len);

    stat->st_size = (n & OS_OBJECT_SYMLINK) ? readlink : len;
    stat->st_blksize = 4096;
    stat->st_blocks = stat->st_size / 512;
    if (stat->st_size < 1024) stat->st_blksize = 1024;

    //** N-links
    n = 0;
    if (val[5] != NULL) sscanf(val[5], "%d", &n);
    stat->st_nlink = n;

    //** Clean up
    for (i=0; i<_lio_stat_key_size; i++) {
        if (val[i] != NULL) free(val[i]);
    }
}

//***********************************************************************
// lio_stat - Do a simple file stat
//***********************************************************************

int lio_stat(lio_config_t *lc, creds_t *creds, char *fname, struct stat *stat, char *mount_prefix, char **readlink)
{
    char *val[_lio_stat_key_size];
    int v_size[_lio_stat_key_size], i, err;

    log_printf(1, "fname=%s\n", fname);
    flush_log();

    for (i=0; i<_lio_stat_key_size; i++) v_size[i] = -lc->max_attr;
    err = lio_get_multiple_attrs(lc, creds, fname, NULL, _lio_stat_keys, (void **)val, v_size, _lio_stat_key_size);

    if (err != OP_STATE_SUCCESS) {
        return(-ENOENT);
    }
    _lio_parse_stat_vals(fname, stat, val, v_size, mount_prefix, readlink);

    log_printf(1, "END fname=%s err=%d\n", fname, err);
    flush_log();

    return(0);

}


//***********************************************************************
//***********************************************************************
//  FSCK related routines
//***********************************************************************
//***********************************************************************


//***********************************************************************
// lio_fsck_check_file - Checks a file for errors and optionally repairs them
//***********************************************************************

int lio_fsck_check_object(lio_config_t *lc, creds_t *creds, char *path, int ftype, int owner_mode, char *owner, int exnode_mode, char **val, int *v_size)
{
    int state, err, srepair, ex_mode, index, vs, ex_index;
    char *dir, *file, ssize[128], *v;
    ex_id_t ino;
    ex_off_t nbytes;
    exnode_exchange_t *exp;
    exnode_t *ex, *cex;
    segment_t *seg;
    int do_clone;

    ex_index = 2;
    state = 0;

    srepair = exnode_mode & LIO_FSCK_SIZE_REPAIR;
    ex_mode = (srepair > 0) ? exnode_mode - LIO_FSCK_SIZE_REPAIR : exnode_mode;

    log_printf(15, "fname=%s vs[0]=%d vs[1]=%d vs[2]=%d\n", path, v_size[0], v_size[1], v_size[2]);

    //** Check the owner
    index = 0;
    v = val[index];
    vs= v_size[index];
    if (vs <= 0) { //** Missing owner
        switch (owner_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_OWNER;
            log_printf(15, "fname=%s missing owner\n", path);
            break;
        case LIO_FSCK_PARENT:
            os_path_split(path, &dir, &file);
            log_printf(15, "fname=%d parent=%s file=%s\n", path, dir, file);
            free(file);
            file = NULL;
            vs = -lc->max_attr;
            lio_get_attr(lc, creds, dir, NULL, "system.owner", (void **)&file, &vs);
            log_printf(15, "fname=%d parent=%s owner=%s\n", path, dir, file);
            if (vs > 0) {
                lio_set_attr(lc, creds, path, NULL, "system.owner", (void *)file, strlen(file));
                free(file);
            } else {
                state |= LIO_FSCK_MISSING_OWNER;
            }
            free(dir);
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(gop_lio_remove_object(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        case LIO_FSCK_USER:
            lio_set_attr(lc, creds, path, NULL, "system.owner", (void *)owner, strlen(owner));
            break;
        }
    }

    //** Check the inode
    index = 1;
    v = val[index];
    vs= v_size[index];
    if (vs <= 0) { //** Missing inode
        switch (owner_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_INODE;
            log_printf(15, "fname=%s missing owner\n", path);
            break;
        case LIO_FSCK_PARENT:
        case LIO_FSCK_USER:
            ino = 0;
            generate_ex_id(&ino);
            snprintf(ssize, sizeof(ssize),  XIDT, ino);
            lio_set_attr(lc, creds, path, NULL, "system.inode", (void *)ssize, strlen(ssize));
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(gop_lio_remove_object(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        }
    }

    //** Check if we have an exnode
    do_clone = 0;
    index = 2;
    v = val[index];
    vs= v_size[index];
    if (vs <= 0) {
        switch (ex_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_EXNODE;
            return(state);
            break;
        case LIO_FSCK_PARENT:
            os_path_split(path, &dir, &file);
            free(file);
            file = NULL;
            vs = -lc->max_attr;
            lio_get_attr(lc, creds, dir, NULL, "system.exnode", (void **)&file, &vs);
            if (vs > 0) {
                val[index] = file;
                do_clone = 1;  //** flag we need to clone and store it
            } else {
                state |= LIO_FSCK_MISSING_EXNODE;
                free(dir);
                return(state);
            }
            free(dir);
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(gop_lio_remove_object(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        }
    }

    //** Make sure it's valid by loading it
    //** If this has a caching segment we need to disable it from being adding
    //** to the global cache table cause there could be multiple copies of the
    //** same segment being serialized/deserialized.
    //** Deserialize it
    exp = exnode_exchange_text_parse(val[ex_index]);
    ex = exnode_create();
    if (exnode_deserialize(ex, exp, lc->ess_nocache) != 0) {
        log_printf(15, "ERROR parsing parent exnode fname=%s\n", dir);
        state |= LIO_FSCK_MISSING_EXNODE;
        exp->text.text = NULL;
        goto finished;
    }
    exp->text.text = NULL;

    //** Execute the clone operation if needed
    if (do_clone == 1) {
        err = gop_sync_exec(exnode_clone(lc->tpc_unlimited, ex, lc->da, &cex, NULL, CLONE_STRUCTURE, lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR cloning parent fname=%s\n", dir);
            state |= LIO_FSCK_MISSING_EXNODE;
            goto finished;
        }

        //** Serialize it for storage
        exnode_serialize(cex, exp);
        lio_set_attr(lc, creds, path, NULL, "system.exnode", (void *)exp->text.text, strlen(exp->text.text));
        exnode_destroy(ex);
        ex = cex;   //** WE use the clone for size checking
    }

    if ((ftype & OS_OBJECT_DIR) > 0) goto finished;  //** Nothing else to do if a directory

    //** Get the default view to use
    seg = exnode_get_default(ex);
    if (seg == NULL) {
        state |= LIO_FSCK_MISSING_EXNODE;
        goto finished;
    }

    index = 3;
    v = val[index];
    vs= v_size[index];
    if (vs <= 0) {  //** No size of correct if they want to
        if (srepair == LIO_FSCK_SIZE_REPAIR) {
            state |= LIO_FSCK_MISSING_EXNODE_SIZE;
            goto finished;
        }
        sprintf(ssize, I64T, segment_size(seg));
        lio_set_attr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
        goto finished;
    }

    //** Verify the size
    sscanf(v, XOT, &nbytes);
    if (nbytes != segment_size(seg)) {
        if (srepair == LIO_FSCK_SIZE_REPAIR) {
            state |= LIO_FSCK_MISSING_EXNODE_SIZE;
            goto finished;
        }
        sprintf(ssize, I64T, segment_size(seg));
        lio_set_attr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
    }

    //** Clean up
finished:
    exnode_destroy(ex);
    exnode_exchange_destroy(exp);

    log_printf(15, "fname=%s state=%d\n", path, state);

    return(state);
}


//***********************************************************************
// lio_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_status_t lio_fsck_object_fn(void *arg, int id)
{
    lio_fsck_check_t *op = (lio_fsck_check_t *)arg;
    int err, i;
    op_status_t status;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    log_printf(15, "fname=%s START\n", op->path);
    flush_log();

    if (op->ftype <= 0) { //** Bad Ftype so see if we can figure it out
        op->ftype = lio_exists(op->lc, op->creds, op->path);
    }

    if (op->ftype == 0) { //** No file
        status = op_failure_status;
        status.error_code = LIO_FSCK_MISSING;
        return(status);
    }

    if (op->full == 0) {
        log_printf(15, "fname=%s getting attrs\n", op->path);
        flush_log();
        for (i=0; i<_n_fsck_keys; i++) {
            val[i] = NULL;
            v_size[i] = -op->lc->max_attr;
        }
        lio_get_multiple_attrs(op->lc, op->creds, op->path, NULL, _fsck_keys, (void **)&val, v_size, _n_fsck_keys);
        err = lio_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, val, v_size);
        for (i=0; i<_n_fsck_keys; i++) if (val[i] != NULL) free(val[i]);
    } else {
        err = lio_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, op->val, op->v_size);
    }

    log_printf(15, "fname=%s status=%d\n", op->path, err);
    status = op_success_status;
    status.error_code = err;
    return(status);
}

//***********************************************************************
// lio_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_generic_t *lio_fsck_object(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode)
{
    lio_fsck_check_t *op;

    log_printf(15, "fname=%s START\n", fname);
    flush_log();

    type_malloc_clear(op, lio_fsck_check_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->ftype = ftype;
    op->path = fname;
    op->owner_mode = owner_mode;
    op->owner = owner;
    op->exnode_mode = exnode_mode;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_fsck_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_generic_t *lio_fsck_object_full(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode, char **val, int *v_size)
{
    lio_fsck_check_t *op;

    type_malloc(op, lio_fsck_check_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->ftype = ftype;
    op->path = fname;
    op->owner_mode = owner_mode;
    op->owner = owner;
    op->exnode_mode = exnode_mode;
    op->val = val;
    op->v_size = v_size;
    op->full = 1;

    return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_fsck_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_next_fsck - Returns the next broken object
//***********************************************************************

int lio_next_fsck(lio_config_t *lc, lio_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;
    int i, prefix_len, slot;
    lio_fsck_task_t *task;
    op_generic_t *gop;
    op_status_t status;

    if (it->firsttime == 1) {  //** First time through so fill up the tasks
        it->firsttime = 2;
        for (slot=0; slot< it->n; slot++) {
            task = &(it->task[slot]);
            task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
            if (task->ftype <= 0) break;  //** No more tasks
            log_printf(15, "fname=%s slot=%d\n", task->fname, slot);

            memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
            memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

            gop = lio_fsck_object_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
            gop_set_myid(gop, slot);
            opque_add(it->q, gop);
        }
    }

    log_printf(15, "main loop start nque=%d\n", opque_tasks_left(it->q));

    //** Start processing the results
    while ((gop = opque_waitany(it->q)) != NULL) {
        it->visited_count++;
        slot = gop_get_myid(gop);
        task = &(it->task[slot]);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
        *bad_atype = task->ftype;  //** Preserve the info before launching a new one
        *bad_fname = task->fname;
        log_printf(15, "fname=%s slot=%d state=%d\n", task->fname, slot, status.error_code);
        for (i=0; i<_n_fsck_keys; i++) {
            if (task->val[i] != NULL) free(task->val[i]);
        };

        if (it->firsttime == 2) {  //** Only go here if we hanve't finished iterating
            task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
            if (task->ftype <= 0) {
                it->firsttime = 3;
            } else {
                memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
                memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

                gop = lio_fsck_object_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
                gop_set_myid(gop, slot);
                opque_add(it->q, gop);
            }
        }

        log_printf(15, "fname=%s state=%d LIO_FSCK_GOOD=%d\n", *bad_fname, status.error_code, LIO_FSCK_GOOD);
        if (status.error_code != LIO_FSCK_GOOD) { //** Found one
            log_printf(15, "ERROR fname=%s state=%d\n", *bad_fname, status.error_code);
            return(status.error_code);
        }

        free(*bad_fname);  //** IF we made it here we can throw away the old fname
    }

    log_printf(15, "nothing left\n");
    *bad_atype = 0;
    *bad_fname = NULL;
    return(LIO_FSCK_FINISHED);

}

//***********************************************************************
// lio_create_fsck_iter - Creates an FSCK iterator
//***********************************************************************

lio_fsck_iter_t *lio_create_fsck_iter(lio_config_t *lc, creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode)
{
    lio_fsck_iter_t *it;
    int i;

    type_malloc_clear(it, lio_fsck_iter_t, 1);

    it->lc = lc;
    it->creds = creds;
    it->path = strdup(path);
    it->owner_mode = owner_mode;
    it->owner = owner;
    it->exnode_mode = exnode_mode;

    it->regex = os_path_glob2regex(it->path);

    for (i=0; i<_n_fsck_keys; i++) {
        it->v_size[i] = -lc->max_attr;
        it->val[i] = NULL;
    }

    it->it = os_create_object_iter_alist(it->lc->os, creds, it->regex, NULL, OS_OBJECT_ANY, 10000, _fsck_keys, (void **)it->val, it->v_size, _n_fsck_keys);
    if (it->it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
        return(NULL);
    }

    it->n = lio_parallel_task_count;
    it->firsttime = 1;
    type_malloc_clear(it->task, lio_fsck_task_t, it->n);
    it->q = new_opque();
    opque_start_execution(it->q);

    return((lio_fsck_iter_t *)it);
}

//***********************************************************************
// lio_destroy_fsck_iter - Creates an FSCK iterator
//***********************************************************************

void lio_destroy_fsck_iter(lio_config_t *lc, lio_fsck_iter_t *oit)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;
    op_generic_t *gop;
    int slot;

    while ((gop = opque_waitany(it->q)) != NULL) {
        slot = gop_get_myid(gop);
        if (it->task[slot].fname != NULL) free(it->task[slot].fname);
    }
    opque_free(it->q, OP_DESTROY);

    os_destroy_object_iter(it->lc->os, it->it);

    os_regex_table_destroy(it->regex);
    free(it->path);
    free(it->task);
    free(it);

    return;
}

//***********************************************************************
// lio_fsck_visited_count - Returns the number of files checked
//***********************************************************************

ex_off_t lio_fsck_visited_count(lio_config_t *lc, lio_fsck_iter_t *oit)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;

    return(it->visited_count);
}

