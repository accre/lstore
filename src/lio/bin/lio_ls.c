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

#define _log_module_index 196

#include <apr_time.h>
#include <errno.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>

typedef struct {
    char *fname;
    char *link;
    char *vals[5];
    int v_size[5];
    int link_size;
    int prefix_len;
    int ftype;
} ls_entry_t;

lio_path_tuple_t tuple;

//*************************************************************************
// ls_format_entry - Prints an LS entry
//*************************************************************************

void ls_format_entry(tbx_log_fd_t *ifd, ls_entry_t *lse)
{
    char *dtype;
    char *perms;
    char *owner;
    char dt_create[128], dt_modify[128];
    apr_time_t dt;
    int64_t n;
    long int fsize;
    int nlink;

    if ((lse->ftype & OS_OBJECT_SYMLINK_FLAG) > 0) {
        if ((lse->ftype & OS_OBJECT_BROKEN_LINK_FLAG) > 0) {
            perms = "L---------";
        } else {
            perms = "l---------";
        }
    } else if ((lse->ftype & OS_OBJECT_DIR_FLAG) > 0) {
        perms = "d---------";
    } else {
        perms = "----------";
    }

    dtype = ((lse->ftype & OS_OBJECT_DIR_FLAG) > 0) ? "/" : "";

    owner = lse->vals[0];
    if (owner == NULL) owner = "root";

    n = 0;
    if (lse->vals[1] != NULL) sscanf(lse->vals[1], I64T, &n);
    fsize = n;

    memset(dt_create, '-', 24);
    dt_create[24] = 0;
    n = -1;
    if (lse->vals[3] != NULL) sscanf(lse->vals[3], I64T, &n);
    if (n>0) {
        n = apr_time_from_sec(n);
        apr_ctime(dt_create, n);
    }

    memcpy(dt_modify, dt_create, 25);
    n = -1;
    if (lse->vals[2] != NULL) sscanf(lse->vals[2], I64T, &n);
    if (n > 0) {
        dt = apr_time_from_sec(n);
        apr_ctime(dt_modify, dt);
    }

    nlink = 1;
    if (lse->vals[4] != NULL) sscanf(lse->vals[4], "%d", &nlink);

    if (lse->link == NULL) {
        info_printf(ifd, 0, "%s  %3d  %10s  %10ld  %s  %s  %s%s\n", perms, nlink, owner, fsize, dt_create, dt_modify, lse->fname, dtype);
    } else {
        info_printf(ifd, 0, "%s  %3d  %10s  %10ld  %s  %s  %s%s -> %s\n", perms, nlink, owner, fsize, dt_create, dt_modify, lse->fname, dtype, lse->link);
    }

    return;
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, ftype, rg_mode, start_option, start_index, prefix_len, nosort, err;
    ex_off_t fcount;
    char *fname;
    ls_entry_t *lse;
    tbx_list_t *table;
    lio_os_regex_table_t *rp_single, *ro_single;
    os_object_iter_t *it;
    tbx_list_iter_t lit;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    char *keys[] = { "system.owner", "system.exnode.size", "system.modify_data", "os.create",  "os.link_count" };
    char *vals[5];
    int v_size[5];
    int n_keys = 5;
    int recurse_depth = 0;
    int obj_types = OS_OBJECT_ANY_FLAG;
    int return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_ls LIO_COMMON_OPTIONS [-rd recurse_depth] [-ns] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        printf("    -ns                - Don't sort the output\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    rp_single = ro_single = NULL;
    nosort = 0;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        if (argc == 1) {
            break;
        }
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
            i++;
            obj_types = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-ns") == 0) {  //** Strip off the path prefix
            i++;
            nosort = 1;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;

    if (rg_mode == 0) {
        if (i>=argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_index--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    fcount = 0;

    q = gop_opque_new();
    table = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);


    for (j=start_index; j<argc; j++) {
        log_printf(5, "path_index=%d argc=%d rg_mode=%d\n", j, argc, rg_mode);
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, argv[j]);
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        for (i=0; i<n_keys; i++) v_size[i] = -tuple.lc->max_attr;
        memset(vals, 0, sizeof(vals));
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, recurse_depth, keys, (void **)vals, v_size, n_keys);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            log_printf(0, "ERROR: Failed with object_iter creation\n");
            return_code = EIO;
            goto finished;
        }

        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            tbx_type_malloc_clear(lse, ls_entry_t, 1);
            lse->fname = fname;
            lse->ftype = ftype;
            lse->prefix_len = prefix_len;
            memcpy(lse->v_size, v_size, sizeof(v_size));
            memcpy(lse->vals, vals, sizeof(vals));

            for (i=0; i<n_keys; i++) v_size[i] = -tuple.lc->max_attr;
            memset(vals, 0, sizeof(vals));

            //** Check if we have a link.  If so we need to resolve the link path
            if ((ftype & OS_OBJECT_SYMLINK_FLAG) > 0) {
                lse->link_size = -64*1024;
                gop = lio_getattr_op(tuple.lc, tuple.creds, lse->fname, NULL, "os.link", (void **)&(lse->link), &(lse->link_size));
                gop_set_private(gop, lse);
                gop_opque_add(q, gop);
                if (nosort == 1) opque_waitall(q);
            }

            if (fcount == 0) {
                info_printf(lio_ifd, 0, "  Perms     Ref   Owner        Size           Creation date              Modify date             Filename [-> link]\n");
                info_printf(lio_ifd, 0, "----------  ---  ----------  ----------  ------------------------  ------------------------  ------------------------------\n");
            }
            fcount++;

            if (nosort == 1) {
                ls_format_entry(lio_ifd, lse);
            } else {
                tbx_list_insert(table, lse->fname, lse);
            }
        }

        lio_destroy_object_iter(tuple.lc, it);

        lio_path_release(&tuple);
        if (rp_single != NULL) {
            lio_os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            lio_os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
    }

    //** Wait for any readlinks to complete
    err = (gop_opque_task_count(q) > 0) ? opque_waitall(q) : OP_STATE_SUCCESS;
    if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "ERROR: Failed with readlink operation!\n");
        return_code = EIO;
    }

    //** Now sort and print things if needed
    if (nosort == 0) {
        lit = tbx_list_iter_search(table, NULL, 0);
        while ((tbx_list_next(&lit, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&lse)) == 0) {
            ls_format_entry(lio_ifd, lse);
        }
    }

    tbx_list_destroy(table);

    if (fcount == 0) return_code = 2;

finished:
    gop_opque_free(q, OP_DESTROY);

    lio_shutdown();

    return(return_code);
}

