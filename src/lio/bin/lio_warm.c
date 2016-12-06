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

#define _log_module_index 207

#include <leveldb/c.h>
#include <apr.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/authn.h>
#include <lio/ds.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "warmer_helpers.h"


typedef struct {
    char *rid_key;
    ex_off_t good;
    ex_off_t bad;
    ex_off_t nbytes;
    ex_off_t dtime;
} warm_hash_entry_t;

typedef struct {
   char *cap;
   ex_off_t nbytes;
} warm_cap_info_t;

typedef struct {
    warm_cap_info_t *cap;
    char *fname;
    char *exnode;
    lio_creds_t *creds;
    ibp_context_t *ic;
    apr_hash_t *hash;
    apr_pool_t *mpool;
    int n;
    ex_id_t inode;
    int write_err;
} warm_t;

apr_hash_t *tagged_rids = NULL;
apr_pool_t *tagged_pool = NULL;
tbx_stack_t *tagged_keys = NULL;
leveldb_t *db_rid = NULL;
leveldb_t *db_inode = NULL;
int verbose = 0;

static int dt = 86400;

//*************************************************************************
// parse_tag_file - Parse the file contianing the RID's for tagging
//*************************************************************************

void parse_tag_file(char *fname)
{
    tbx_inip_file_t *fd;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *key, *value, *v;

    fd = tbx_inip_file_read(fname);
    if (fd == NULL) return;

    apr_pool_create(&tagged_pool, NULL);
    tagged_rids = apr_hash_make(tagged_pool);
    tagged_keys = tbx_stack_new();

    //** Cycle through the blocks storing both the segment block information and also the cap blocks
    g = tbx_inip_group_find(fd, "tag");
    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "rid_key") == 0) {
            v = tbx_inip_ele_get_value(ele);
            value = strdup(v);
            info_printf(lio_ifd, 0, "Tagging RID %s\n", value);
            apr_hash_set(tagged_rids, value, APR_HASH_KEY_STRING, value);
            tbx_stack_push(tagged_keys, value);
        }

        ele = tbx_inip_ele_next(ele);
    }

    tbx_inip_destroy(fd);

    if (apr_hash_count(tagged_rids) == 0) {
        tbx_stack_free(tagged_keys, 0);
        apr_pool_destroy(tagged_pool);
        tagged_pool = NULL;
        tagged_rids = NULL;
    } else {
        info_printf(lio_ifd, 0, "\n");
    }
}

//*************************************************************************
//  gen_warm_task
//*************************************************************************

gop_op_status_t gen_warm_task(void *arg, int id)
{
    warm_t *w = (warm_t *)arg;
    gop_op_status_t status;
    gop_op_generic_t *gop;
    tbx_inip_file_t *fd;
    int i, nfailed, state;
    warm_hash_entry_t *wrid = NULL;
    char *etext;
    gop_opque_t *q;

    log_printf(15, "warming fname=%s, dt=%d\n", w->fname, dt);
    fd = tbx_inip_string_read(w->exnode);
    tbx_inip_group_t *g;

    q = gop_opque_new();
    opque_start_execution(q);

    tbx_type_malloc(w->cap, warm_cap_info_t, tbx_inip_group_count(fd));
    g = tbx_inip_group_first(fd);
    w->n = 0;
    while (g) {
        if (strncmp(tbx_inip_group_get(g), "block-", 6) == 0) { //** Got a data block
            //** Get the RID key
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "rid_key", NULL);
            if (etext != NULL) {
                wrid = apr_hash_get(w->hash, etext, APR_HASH_KEY_STRING);
                if (wrid == NULL) { //** 1st time so need to make an entry
                    tbx_type_malloc_clear(wrid, warm_hash_entry_t, 1);
                    wrid->rid_key = etext;
                    apr_hash_set(w->hash, wrid->rid_key, APR_HASH_KEY_STRING, wrid);
                } else {
                    free(etext);
                }
            }

            //** Get the data size and update the counts
            w->cap[w->n].nbytes += tbx_inip_get_integer(fd, tbx_inip_group_get(g), "max_size", 0);
            wrid->nbytes += w->cap[w->n].nbytes;

            //** Get the manage cap
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "manage_cap", "");
            log_printf(1, "fname=%s cap[%d]=%s alloc_max_size=" XOT "\n", w->fname, w->n, etext, w->cap[w->n].nbytes);
            w->cap[w->n].cap = tbx_stk_unescape_text('\\', etext);
            free(etext);

            //** Add the task
            gop = ibp_modify_alloc_gop(w->ic, w->cap[w->n].cap, -1, dt, -1, lio_gc->timeout);
            gop_set_myid(gop, w->n);
            gop_set_private(gop, wrid);
            gop_opque_add(q, gop);
            w->n++;

            //** Check if it was tagged
            if (tagged_rids != NULL) {
                if (apr_hash_get(tagged_rids, wrid->rid_key, APR_HASH_KEY_STRING) != NULL) {
                    info_printf(lio_ifd, 0, "RID_TAG: %s  rid_key=%s\n", w->fname, wrid->rid_key);
                }
            }
        }
        g = tbx_inip_group_next(g);
    }

    tbx_inip_destroy(fd);

    nfailed = 0;
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        wrid = gop_get_private(gop);
        i = gop_get_myid(gop);

        wrid->dtime += gop_time_exec(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            wrid->good++;
         } else {
            nfailed++;
            wrid->bad++;
            info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", w->fname, w->cap[i].cap);
        }
        warm_put_rid(db_rid, wrid->rid_key, w->inode, w->cap[i].nbytes, 0);

        gop_free(gop, OP_DESTROY);
    }

    state = (w->write_err == 0) ? 0 : WFE_WRITE_ERR;
    if (nfailed == 0) {
        status = gop_success_status;
        state |= WFE_SUCCESS;
        if (verbose == 1) info_printf(lio_ifd, 0, "Succeeded with file %s with %d allocations\n", w->fname, w->n);
    } else {
        status = gop_failure_status;
        state |= WFE_FAIL;
        info_printf(lio_ifd, 0, "Failed with file %s on %d out of %d allocations\n", w->fname, nfailed, w->n);
    }
    warm_put_inode(db_inode, w->inode, state, nfailed, w->fname);

    etext = NULL;
    i = 0;
    lio_setattr(lio_gc, w->creds, w->fname, NULL, "os.timestamp.system.warm", (void *)etext, i);

    gop_opque_free(q, OP_DESTROY);

    free(w->exnode);
    free(w->fname);
    for (i=0; i<w->n; i++) free(w->cap[i].cap);
    free(w->cap);

    return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, start_option, rg_mode, ftype, prefix_len;
    char *fname, *path;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    char *keys[] = { "system.exnode", "system.write_errors", "system.inode" };
    char *vals[3];
    char *db_base = "/lio/log/warm";
    int slot, v_size[3];
    os_object_iter_t *it;
    lio_os_regex_table_t *rp_single, *ro_single;
    tbx_list_t *master;
    apr_hash_index_t *hi;
    apr_ssize_t klen;
    char *rkey, *config, *value;
    char *line_end;
    warm_hash_entry_t *mrid, *wrid;
    tbx_inip_file_t *ifd;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    void *piter;
    char ppbuf[128], ppbuf2[128], ppbuf3[128];
    lio_path_tuple_t tuple;
    ex_off_t total, good, bad, nbytes, submitted, werr, missing_err;
    tbx_list_iter_t lit;
    tbx_stack_t *stack;
    int recurse_depth = 10000;
    int summary_mode;
    warm_t *w;
    double dtime, dtime_total;

    if (argc < 2) {
        printf("\n");
        printf("lio_warm LIO_COMMON_OPTIONS [-db DB_output_dir] [-t tag.cfg] [-rd recurse_depth] [-dt time] [-sb] [-sf] [ -v] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -db DB_output_dir   - Output Directory for the DBes. Default is %s\n", db_base);
        printf("    -t tag.cfg         - INI file with RID to tag by printing any files usign the RIDs\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -dt time           - Duration time in sec.  Default is %d sec\n", dt);
        printf("    -sb                - Print the summary but only list the bad RIDs\n");
        printf("    -sf                - Print the the full summary\n");
        printf("    -v                 - Print all Success/Fail messages instead of just errors\n");
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the path args
    rp_single = ro_single = NULL;
    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    summary_mode = 0;
    verbose = 0;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db") == 0) { //** DB output base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-dt") == 0) { //** Time
            i++;
            dt = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-sb") == 0) { //** Print only bad RIDs
            i++;
            summary_mode = 1;
        } else if (strcmp(argv[i], "-sf") == 0) { //** Print the full summary
            i++;
            summary_mode = 2;
        } else if (strcmp(argv[i], "-v") == 0) { //** Verbose printing
            i++;
            verbose = 1;
        } else if (strcmp(argv[i], "-t") == 0) { //** Got a list of RIDs to tag
            i++;
            parse_tag_file(argv[i]);
            i++;
        }

    } while ((start_option < i) && (i<argc));
    start_option = i;


    if (rg_mode == 0) {
        if (i>=argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_option--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    piter = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));

    create_warm_db(db_base, &db_inode, &db_rid);  //** Create the DB

    q = gop_opque_new();
    opque_start_execution(q);

    tbx_type_malloc_clear(w, warm_t, lio_parallel_task_count);
    for (j=0; j<lio_parallel_task_count; j++) {
        apr_pool_create(&(w[j].mpool), NULL);
        w[j].hash = apr_hash_make(w[j].mpool);
    }

    submitted = good = bad = werr = missing_err = 0;

    while ((path = tbx_stdinarray_iter_next(piter)) != NULL) {
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }
        free(path);  //** No longer needed.  lio_path_resolve will strdup

        v_size[0] = v_size[1] = -tuple.lc->max_attr; v_size[2] = -tuple.lc->max_attr;
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE_FLAG, recurse_depth, keys, (void **)vals, v_size, 3);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            goto finished;
        }


        slot = 0;
        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            if ((ftype & OS_OBJECT_SYMLINK) || (v_size[0] == -1)) { //** We skip symlinked files and files missing exnodes
                info_printf(lio_ifd, 0, "MISSING_EXNODE_ERROR for file %s\n", fname);
                missing_err++;
                free(fname);
                for (i=-0; i<3; i++) {
                    if (v_size[i] > 0) free(vals[i]);
                }
                continue;
            }
            w[slot].fname = fname;
            w[slot].exnode = vals[0];
            w[slot].creds = tuple.lc->creds;
            w[slot].ic = hack_ds_ibp_context_get(tuple.lc->ds);
            w[slot].write_err = 0;

            if (v_size[1] != -1) {
                werr++;
                w[slot].write_err = 1;
                info_printf(lio_ifd, 0, "WRITE_ERROR for file %s\n", fname);
                if (vals[1] != NULL) {
                    free(vals[1]);
                    vals[1] = NULL;
                }
            }

            w[slot].inode = 0;
            if (v_size[2] > 0) {
               sscanf(vals[2], XIDT, &(w[slot].inode));
               free(vals[2]);
               vals[2] = NULL;
            }

            vals[0] = NULL;
            fname = NULL;
            submitted++;

            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, gen_warm_task, (void *)&(w[slot]), NULL, 1);
            gop_set_myid(gop, slot);
            gop_opque_add(q, gop);

            if (submitted >= lio_parallel_task_count) {
                gop = opque_waitany(q);
                status = gop_get_status(gop);
                if (status.op_status == OP_STATE_SUCCESS) {
                    good++;
                } else {
                    bad++;
                }
                slot = gop_get_myid(gop);
                gop_free(gop, OP_DESTROY);
            } else {
                slot++;
            }
        }

        lio_destroy_object_iter(lio_gc, it);

        while ((gop = opque_waitany(q)) != NULL) {
            status = gop_get_status(gop);
            if (status.op_status == OP_STATE_SUCCESS) {
                good++;
            } else {
                bad++;
            }
            gop_free(gop, OP_DESTROY);
        }

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

    gop_opque_free(q, OP_DESTROY);

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Submitted: " XOT "   Success: " XOT "   Fail: " XOT "    Write Errors: " XOT "   Missing Exnodes: " XOT "\n", submitted, good, bad, werr, missing_err);
    if (submitted != (good+bad)) {
        info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
    }
    if (bad > 0) {
        info_printf(lio_ifd, 0, "ERROR Some files failed to warm!\n");
    }


    if (submitted == 0) goto cleanup;

    //** Merge the data from all the tables
    master = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
    for (i=0; i<lio_parallel_task_count; i++) {
        hi = apr_hash_first(NULL, w[i].hash);
        while (hi != NULL) {
            apr_hash_this(hi, (const void **)&rkey, &klen, (void **)&wrid);
            mrid = tbx_list_search(master, wrid->rid_key);
            if (mrid == NULL) {
                tbx_list_insert(master, wrid->rid_key, wrid);
            } else {
                mrid->good += wrid->good;
                mrid->bad += wrid->bad;
                mrid->nbytes += wrid->nbytes;
                mrid->dtime += wrid->dtime;

                apr_hash_set(w[i].hash, wrid->rid_key, APR_HASH_KEY_STRING, NULL);
                free(wrid->rid_key);
                free(wrid);
            }

            hi = apr_hash_next(hi);
        }
    }

    //** Get the RID config which is used in the summary
    config = rs_get_rid_config(lio_gc->rs);
    ifd = tbx_inip_string_read(config);FATAL_UNLESS(ifd);

    //** Convert it for easier lookup
    ig = tbx_inip_group_first(ifd);
    while (ig != NULL) {
        rkey = tbx_inip_group_get(ig);
        if (strcmp("rid", rkey) == 0) {  //** Found a resource
            //** Now cycle through the attributes
            ele = tbx_inip_ele_first(ig);
            while (ele != NULL) {
                rkey = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(rkey, "rid_key") == 0) {
                    tbx_inip_group_free(ig);
                    tbx_inip_group_set(ig, strdup(value));
                }

                ele = tbx_inip_ele_next(ele);
            }
        }

        ig = tbx_inip_group_next(ig);
    }

    //** Print the summary
    info_printf(lio_ifd, 0, "\n");
    info_printf(lio_ifd, 0, "                                                              Allocations\n");
    info_printf(lio_ifd, 0, "                 RID Key                    Size    Avg Time(us)   Total       Good         Bad\n");
    info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");
    nbytes = good = bad = j = i = 0;
    stack = tbx_stack_new();
    dtime_total = 0;
    lit = tbx_list_iter_search(master, NULL, 0);
    while (tbx_list_next(&lit, (tbx_list_key_t **)&rkey, (tbx_list_data_t **)&mrid) == 0) {
        j++;
        nbytes += mrid->nbytes;
        good += mrid->good;
        bad += mrid->bad;
        total = mrid->good + mrid->bad;
        if (mrid->bad > 0) i++;

        tbx_stack_push(stack, mrid);

        if ((summary_mode == 0) || ((summary_mode == 1) && (mrid->bad == 0))) continue;
        dtime_total += mrid->dtime;
        dtime = mrid->dtime / (double)total;
        line_end = (mrid->bad == 0) ? "\n" : "  RID_ERR\n";
        rkey = tbx_inip_get_string(ifd, mrid->rid_key, "ds_key", mrid->rid_key);
        info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "%s", rkey,
                    tbx_stk_pretty_print_double_with_scale(1024, (double)mrid->nbytes, ppbuf),  tbx_stk_pretty_print_double_with_scale(1024, dtime, ppbuf2),
                    total, mrid->good, mrid->bad, line_end);
        free(rkey);
    }
    if (summary_mode != 0) info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");

    snprintf(ppbuf2, sizeof(ppbuf2), "SUM (%d RIDs, %d bad)", j, i);
    total = good + bad;
    dtime_total = dtime_total / (double)total;
    info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "\n", ppbuf2,
                tbx_stk_pretty_print_double_with_scale(1024, (double)nbytes, ppbuf), tbx_stk_pretty_print_double_with_scale(1024, dtime_total, ppbuf3), total, good, bad);

    tbx_list_destroy(master);

    tbx_inip_destroy(ifd);
    free(config);

    while ((mrid = tbx_stack_pop(stack)) != NULL) {
        free(mrid->rid_key);
        free(mrid);
    }
    tbx_stack_free(stack, 0);
cleanup:
    for (j=0; j<lio_parallel_task_count; j++) {
        apr_pool_destroy(w[j].mpool);
    }

    free(w);

finished:
    if (tagged_rids != NULL) {
        tbx_stack_free(tagged_keys, 1);
        apr_pool_destroy(tagged_pool);
    }

    close_warm_db(db_inode, db_rid);  //** Close the DBs

    tbx_stdinarray_iter_destroy(piter);

    lio_shutdown();

    return(0);
}


