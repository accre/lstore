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

#define _log_module_index 208

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <limits.h>
#include <lio/segment.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <strings.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>
#include <tbx/stack.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/authn.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>

#define n_inspect 10
char *inspect_opts[] = { "DUMMY", "inspect_quick_check",  "inspect_scan_check",  "inspect_full_check",
                         "inspect_quick_repair", "inspect_scan_repair", "inspect_full_repair",
                         "inspect_soft_errors",  "inspect_hard_errors", "inspect_migrate"
                       };

#define SMODE_EQ      0
#define SMODE_NEQ     1
#define SMODE_EXISTS  2
#define SMODE_MISSING 3

char *select_mode_string[] = { "eq", "neq", "exists", "missing" };

#define DELTA_MODE_NOTHING 0
#define DELTA_MODE_PERCENT 1
#define DELTA_MODE_AUTO    2
#define DELTA_MODE_ABS     3

typedef struct {
    char *name;
    char *log;
    double delta;
    double tolerance;
    int delta_mode;
    int tolerance_mode;
    int unspecified;
    apr_hash_t *pick_from;
    tbx_stack_t *groups;
    tbx_stack_t *rids;
} pool_entry_t;

typedef struct {
    char *fname;
    char *exnode;
    char *set_key;
    char *set_success;
    char *set_fail;
    char *log_name;
    int  id;
    int  get_exnode;
    int  set_success_size;
    int  set_fail_size;
    int ftype;
    int pslot;
} inspect_t;

typedef struct {
    lio_rid_inspect_tweak_t ri;
    tbx_inip_group_t *ig;
    int status;
    ex_off_t total;
    ex_off_t free;
    ex_off_t used;
} rid_prep_entry_t;

static lio_creds_t *creds;
static int global_whattodo;
static ex_off_t bufsize;
rs_query_t *query;
lio_path_tuple_t *tuple_list;
char **argv_list = NULL;
int error_only_check = 0;

apr_thread_mutex_t *rid_lock = NULL;
apr_hash_t *rid_changes = NULL;
apr_pool_t *rid_mpool = NULL;

apr_thread_mutex_t *lock = NULL;
tbx_list_t *seg_index;

int shutdown_now = 0;
apr_thread_mutex_t *shutdown_lock;
apr_pool_t *shutdown_mpool;

//*************************************************************************
//  signal_shutdown - QUIT signal handler
//*************************************************************************

void signal_shutdown(int sig)
{
    char date[128];
    apr_ctime(date, apr_time_now());

    log_printf(0, "Shutdown requested on %s\n", date);
    info_printf(lio_ifd, 0, "========================= Shutdown requested on %s ======================\n", date);

    apr_thread_mutex_lock(shutdown_lock);
    shutdown_now = 1;
    apr_thread_mutex_unlock(shutdown_lock);

    return;
}

//*************************************************************************
//  install_signal_handler - Installs the QUIT handler
//*************************************************************************

void install_signal_handler()
{
    //** Make the APR stuff
    assert_result(apr_pool_create(&shutdown_mpool, NULL), APR_SUCCESS);
    apr_thread_mutex_create(&shutdown_lock, APR_THREAD_MUTEX_DEFAULT, shutdown_mpool);

    //***Attach the signal handler for shutdown
    shutdown_now = 0;
    apr_signal_unblock(SIGQUIT);
    apr_signal(SIGQUIT, signal_shutdown);
}

//*************************************************************************
// process_pool - Does the final pool processing converting it into what
//    lio_inspect expects.
//*************************************************************************

void process_pool(pool_entry_t *pe, tbx_stack_t *parent_rid_stack)
{
    pool_entry_t *p;
    rid_prep_entry_t *re;
    ex_off_t total_used, total_bytes, dsum;
    double avg, percent, d1, d2;

    log_printf(5, "START pool=%s mode=%d\n", pe->name, pe->delta_mode);

    //** Recursively handle all the groups
    tbx_stack_move_to_top(pe->groups);
    while ((p = tbx_stack_get_current_data(pe->groups)) != NULL) {
        log_printf(5, " p->name=%s\n", p->name);
        process_pool(p, pe->rids);
        tbx_stack_move_down(pe->groups);
    }

    percent = (pe->tolerance_mode == DELTA_MODE_PERCENT) ? pe->tolerance/100.0 : 0.05;

    switch(pe->delta_mode) {
    case (DELTA_MODE_AUTO) :
        //** Need to get the total size to calculate the fraction taget
        total_used = total_bytes = 0;
        tbx_stack_move_to_top(pe->rids);
        while ((re = tbx_stack_get_current_data(pe->rids)) != NULL) {
            total_used += re->used;
            total_bytes += re->total;
            log_printf(5, "AUTO RID=%s used=" XOT " total=" XOT "\n", re->ri.rid->rid_key, re->used, re->total);
            tbx_stack_move_down(pe->rids);
        }

        avg = (double)total_used / ((double)total_bytes);

        //** Make the tweaks
        dsum = 0;
        tbx_stack_move_to_top(pe->rids);
        while ((re = tbx_stack_get_current_data(pe->rids)) != NULL) {
            re->ri.rid->delta = avg * re->total - re->used;
            re->ri.rid->tolerance = 0.05*re->total;
            re->ri.rid->tolerance = (pe->tolerance_mode == DELTA_MODE_ABS) ? fabs(pe->tolerance) : percent * re->total;
            d1 = re->ri.rid->delta;
            d2 = re->ri.rid->tolerance;
            re->ri.rid->state = ((fabs(d1) <= d2) || (re->ri.rid->tolerance == 0) || (re->total == 0)) ? 1 : 0;
            dsum += re->ri.rid->delta;

            log_printf(5, "AUTO RID=%s delta=" XOT " tol=" XOT "\n", re->ri.rid->rid_key, re->ri.rid->delta, re->ri.rid->tolerance);
            tbx_stack_move_down(pe->rids);
        }

        log_printf(5, "AUTO total_used=" XOT " total_bytes=" XOT " avg=%lf dsum=" XOT "\n", total_used, total_bytes, avg, dsum);
        break;
    case (DELTA_MODE_PERCENT) :
    case (DELTA_MODE_ABS) :
        //** Make the tweaks
        tbx_stack_move_to_top(pe->rids);
        while ((re = tbx_stack_get_current_data(pe->rids)) != NULL) {
            re->ri.rid->delta = (pe->delta_mode == DELTA_MODE_PERCENT) ? pe->delta/100.0 * re->total : pe->delta;
            re->ri.rid->tolerance = (pe->tolerance_mode == DELTA_MODE_PERCENT) ? fabs(pe->tolerance)/100.0 * re->total : fabs(pe->tolerance);
            d1 = re->ri.rid->delta;
            d2 = re->ri.rid->tolerance;
            re->ri.rid->state = ((fabs(d1) <= d2) || (re->ri.rid->tolerance == 0) || (re->total == 0)) ? 1 : 0;

            log_printf(5, "RID=%s delta=" XOT " tol=" XOT " state=%d\n", re->ri.rid->rid_key, re->ri.rid->delta, re->ri.rid->tolerance, re->ri.rid->state);

            tbx_stack_move_down(pe->rids);
        }
        break;
    }

    //** Move all the RIDS to the parent pool if provided
    if (!parent_rid_stack) return;

    int cnt = 0;
    log_printf(0, "parent_rid_stack=%p pe->rids=%p\n", parent_rid_stack, pe->rids);
    while ((re = tbx_stack_pop(pe->rids)) != NULL) {
        log_printf(0, "count=%d stack_size=%d\n", cnt, tbx_stack_count(pe->rids));
        cnt++;
        log_printf(5, "re->ri.rid->rid_key=%s re=%p\n", re->ri.rid->rid_key, re);
        tbx_stack_push(parent_rid_stack, re);
    }

    log_printf(5, "END pool=%s\n", pe->name);
}

//*************************************************************************
// prep_rid_table - Preps the RID table for use in generating a pool config
//*************************************************************************

apr_hash_t *prep_rid_table(tbx_inip_file_t *fd, apr_pool_t *mpool)
{
    apr_hash_t *table;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    rid_prep_entry_t *re;
    int n;
    char *key, *value;

    table = apr_hash_make(mpool);

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        key = tbx_inip_group_get(ig);
        if (strcmp("rid", key) == 0) {  //** Found a resource
            tbx_type_malloc_clear(re, rid_prep_entry_t, 1);
            tbx_type_malloc_clear(re->ri.rid, lio_rid_change_entry_t, 1);
            re->ig = ig;

            //** Now cycle through the attributes
            ele = tbx_inip_ele_first(ig);
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "rid_key") == 0) {  //** This is the RID so store it separate
                    re->ri.rid->rid_key = strdup(value);
                } else if (strcmp(key, "ds_key") == 0) {  //** This is the RID so store it separate
                    re->ri.rid->ds_key = strdup(value);
                } else if (strcmp(key, "status") == 0) {  //** Status
                    sscanf(value, "%d", &n);
                    re->status = ((n>=0) && (n<=3)) ? n : 4;
                } else if (strcmp(key, "space_free") == 0) {  //** Free space
                    sscanf(value, XOT, &(re->free));
                } else if (strcmp(key, "space_used") == 0) {  //** Used space
                    sscanf(value, XOT, &(re->used));
                } else if (strcmp(key, "space_total") == 0) {  //** Total space
                    sscanf(value, XOT, &(re->total));
                }

                ele = tbx_inip_ele_next(ele);
            }

            apr_hash_set(table, re->ri.rid->rid_key, APR_HASH_KEY_STRING, re);
        }

        ig = tbx_inip_group_next(ig);
    }

    return(table);
}

//*************************************************************************
// add_wildcard - Adds a wildcard list or resources to the pool
//*************************************************************************

void add_wildcard(pool_entry_t *p, apr_hash_t *rid_table, char *mkey, char *mvalue)
{
    tbx_inip_element_t *ele;
    char *key, *value, *hkey;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    rid_prep_entry_t *re;
    int match;

    for (hi=apr_hash_first(NULL, rid_table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&hkey, &hlen, (void **)&re);

        match = 0;
        if (mkey != NULL) {
            for (ele=tbx_inip_ele_first(re->ig); ele != NULL; ele = tbx_inip_ele_next(ele)) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);

                if ((strcmp(key, mkey) == 0) && (strcmp(value, mvalue) == 0)) {
                    match = 1;
                    break;
                }
            }
        } else {
            match = 1;
        }

        if (match == 1) {  //** Got a match so move it to the pool/group
            tbx_stack_push(p->rids, re);
            apr_hash_set(rid_table, hkey, hlen, NULL);
        }
    }
}

//*************************************************************************
//  load_pool - Loads a pool
//*************************************************************************

pool_entry_t *load_pool(tbx_stack_t *pools, char *name, tbx_inip_file_t *pfd, tbx_inip_file_t *rfd, apr_hash_t *rid_table, tbx_inip_group_t *pg, pool_entry_t **unspecified)
{
    tbx_inip_element_t *ele;
    char *key, *value;
    pool_entry_t *p;
    tbx_inip_group_t *psg;
    int n;
    char subgroup[4096];

    tbx_type_malloc_clear(p, pool_entry_t, 1);

    p->name = strdup(name);
    p->groups = tbx_stack_new();
    p->rids = tbx_stack_new();
    log_printf(5, "START POOL=%s\n", p->name);

    //** Now cycle through the attributes
    ele = tbx_inip_ele_first(pg);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        value = tbx_inip_ele_get_value(ele);
        if (strcmp(key, "_delta") == 0) {  //** Delta value
            if (strcmp(value, "auto") == 0) {
                p->delta_mode = DELTA_MODE_AUTO;
            } else {
                n = strlen(value);
                if (value[n-1] == '%') {
                    p->delta_mode = DELTA_MODE_PERCENT;
                    value[n-1] = 0;
                } else {
                    p->delta_mode = DELTA_MODE_ABS;
                }

                p->delta = tbx_stk_string_get_double(value);
                log_printf(0, "value=%s p->delta==%lf\n", value, p->delta);
            }
        } else if (strcmp(key, "_tolerance") == 0) {  //** Tolerance
            n = strlen(value);
            if (value[n-1] == '%') {
                p->tolerance_mode = DELTA_MODE_PERCENT;
                value[n-1] = 0;
            } else {
                p->tolerance_mode = DELTA_MODE_ABS;
            }

            p->tolerance = tbx_stk_string_get_double(value);
        } else if (strcmp(key, "_unspecified") == 0) {  //** This is where we dump the unspecified tasks
            p->unspecified = 1;
            *unspecified = p;
        } else if (strcmp(key, "_group") == 0) {  //** Got a sub group
            snprintf(subgroup, sizeof(subgroup), "group-%s", value);
            psg = tbx_inip_group_find(pfd, subgroup);
            log_printf(5, "adding group=%s psig=%p\n", value, psg);

            if (psg != NULL) {
                load_pool(p->groups, value, pfd, rfd, rid_table, psg, unspecified);  //** Add the sub group
            } else {
                printf("ERROR:  Missing subgroup.  Parent=%s missing=%s\n", p->name, value);
            }
        } else {  //**It's a wild card search so directly add them
            add_wildcard(p, rid_table, key, value);
            log_printf(5, "adding wildcard key=%s val=%s\n", key, value);
        }

        ele = tbx_inip_ele_next(ele);
    }

    log_printf(5, "END POOL=%s delta=%lf dmode=%d tol=%lf tmode=%d\n", p->name, p->delta, p->delta_mode, p->tolerance, p->tolerance_mode);

    tbx_stack_push(pools, p);  //** Add ourselves to the stack;

    return(p);
}


//*************************************************************************
// load_pool_config - Loads the rebalance pool configration
//*************************************************************************

apr_hash_t *load_pool_config(char *fname, apr_pool_t *mpool, tbx_stack_t *my_pool_list)
{
    tbx_inip_file_t *pfd, *rfd;
    char *rid_config, *key;
    tbx_inip_group_t *ig;
    apr_hash_t *rid_table;
    apr_hash_t *pools;
    pool_entry_t *pe;
    tbx_stack_t *pool_list;
    rid_prep_entry_t *re;
    pool_entry_t *unspecified = NULL;

    //** Make the rid changes
    pools = apr_hash_make(mpool);

    pool_list = (my_pool_list == NULL) ? tbx_stack_new() : my_pool_list;

    //** Load the pool config
    pfd = tbx_inip_file_read(fname);FATAL_UNLESS(pfd != NULL);

    //** Do the same for RID config converting it to something useful
    rid_config = rs_get_rid_config(lio_gc->rs);FATAL_UNLESS(rid_config != NULL);
    rfd = tbx_inip_string_read(rid_config);FATAL_UNLESS(rfd != NULL);

    //** Load the RID config into a usable table for converting to pools
    rid_table = prep_rid_table(rfd, mpool);

    //** Cycle through all the "pool" stanzas making the pools
    ig = tbx_inip_group_first(pfd);
    while (ig != NULL) {
        key = tbx_inip_group_get(ig);
        log_printf(5, "key=%s\n", key);
        if (strncmp("pool-", key, 5) == 0) {  //** Found a Pool definition so load it
            load_pool(pool_list, key + 5, pfd, rfd, rid_table, ig, &unspecified);
        }

        ig = tbx_inip_group_next(ig);
    }

    //** Finish off by handling the unspecified group if needed
    if (unspecified != NULL) {
        add_wildcard(unspecified, rid_table, NULL, NULL);
    }

    //** Now make the final pass since the "unspecified" has been resolved.
    tbx_stack_move_to_top(pool_list);
    while ((pe = tbx_stack_get_current_data(pool_list)) != NULL) {
        process_pool(pe, NULL);

        pe->pick_from = apr_hash_make(mpool);

        //** Add it to the RID changes
        while ((re = tbx_stack_pop(pe->rids)) != NULL) {
            re->ri.pick_pool = pe->pick_from;
            apr_hash_set(pools, re->ri.rid->rid_key, APR_HASH_KEY_STRING, &(re->ri));
            apr_hash_set(pe->pick_from, re->ri.rid->rid_key, APR_HASH_KEY_STRING, re->ri.rid);
        }

        tbx_stack_move_down(pool_list);
    }

    if (my_pool_list == NULL) tbx_stack_free(pool_list, 1);

    free(rid_config);
    tbx_inip_destroy(rfd);
    tbx_inip_destroy(pfd);

    return(pools);
}


//*************************************************************************
// rebalance_pool - Generates a rebalance pool based on the supplied RID key
//*************************************************************************

apr_hash_t *rebalance_pool(apr_pool_t *mpool, tbx_stack_t *my_pool_list, char *key_rebalance, double tolerance, int tolerance_mode)
{
    tbx_inip_file_t *pfd, *rfd;
    char *rid_config, *key, *value;
    tbx_inip_group_t *ig;
    apr_hash_t *rid_table;
    apr_hash_t *pools;
    pool_entry_t *pe;
    tbx_stack_t *pool_list;
    rid_prep_entry_t *re;
    tbx_list_t *master;
    char pool_text[4096], tstr[128];
    pool_entry_t *unspecified = NULL;

    //** Make the rid changes
    pools = apr_hash_make(mpool);
    master = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);

    pool_list = (my_pool_list == NULL) ? tbx_stack_new() : my_pool_list;
    if (tolerance_mode == DELTA_MODE_ABS) {
        snprintf(tstr, sizeof(tstr), "%lf", tolerance);
    } else {
        snprintf(tstr, sizeof(tstr), "%lf%%", tolerance);
    }

    log_printf(5, "tstr=%s\n", tstr);

    //** Process the RID config converting it to something useful
    rid_config = rs_get_rid_config(lio_gc->rs);FATAL_UNLESS(rid_config != NULL);
    rfd = tbx_inip_string_read(rid_config);FATAL_UNLESS(rfd != NULL);

    //** Load the RID config into a usable table for converting to pools
    rid_table = prep_rid_table(rfd, mpool);

    if (strcmp(key_rebalance, "auto") == 0) { //** Just make 1 big pool
        //** Make a pool config string
        snprintf(pool_text, sizeof(pool_text), "[all]\n"
                 "_delta=auto\n"
                 "_tolerance=%s\n"
                 "_unspecified\n", tstr);

        //** and load it
        pfd = tbx_inip_string_read(pool_text);FATAL_UNLESS(pfd != NULL);
        load_pool(pool_list, "all", pfd, rfd, rid_table, tbx_inip_group_first(pfd), &unspecified);
        tbx_inip_destroy(pfd);
        add_wildcard(unspecified, rid_table, NULL, NULL);  //** This populates the unspecified wildcard
    } else {  //** Creating pools based on RID key/value
        //** Cycle through all the "pool" stanzas making the pools
        ig = tbx_inip_group_first(rfd);
        while (ig != NULL) {
            key = tbx_inip_group_get(ig);
            log_printf(5, "key=%s\n", key);
            if (strcmp("rid", key) == 0) {  //** Found a RID so check it for the key
                value = tbx_inip_find_key(ig, key_rebalance);  //** Get the value
                log_printf(5, "%s=%s\n", key_rebalance, value);
                if (value != NULL) { //** If it exists
                    pe = tbx_list_search(master, value); //** Check if we already have a pool with the value
                    if (pe == NULL) {  //** New pool
                        //** Make a pool config string
                        snprintf(pool_text, sizeof(pool_text), "[%s]\n"
                                 "_delta=auto\n"
                                 "_tolerance=%s\n"
                                 "%s=%s\n", value, tstr, key_rebalance, value);

                        //** and load it
                        pfd = tbx_inip_string_read(pool_text);FATAL_UNLESS(pfd != NULL);
                        pe = load_pool(pool_list, value, pfd, rfd, rid_table, tbx_inip_group_first(pfd), &unspecified);
                        tbx_inip_destroy(pfd);

                        //** Store it in the pool table
                        tbx_list_insert(master, value, pe);
                        pe = tbx_list_search(master, value);
                        log_printf(5, "looking for %s=%p\n", value, pe);
                    }
                }
            }

            ig = tbx_inip_group_next(ig);
        }
    }

    //** Now make the final pass since the "unspecified" has been resolved.
    tbx_stack_move_to_top(pool_list);
    while ((pe = tbx_stack_get_current_data(pool_list)) != NULL) {
        process_pool(pe, NULL);

        pe->pick_from = apr_hash_make(mpool);

        //** Add it to the RID changes
        while ((re = tbx_stack_pop(pe->rids)) != NULL) {
            re->ri.pick_pool = pe->pick_from;
            apr_hash_set(pools, re->ri.rid->rid_key, APR_HASH_KEY_STRING, &(re->ri));
            apr_hash_set(pe->pick_from, re->ri.rid->rid_key, APR_HASH_KEY_STRING, re->ri.rid);
        }

        tbx_stack_move_down(pool_list);
    }

    if (my_pool_list == NULL) tbx_stack_free(pool_list, 1);

    free(rid_config);
    tbx_inip_destroy(rfd);
    tbx_list_destroy(master);

    return(pools);
}

//*************************************************************************
// dump_pools - Prints the pools
//*************************************************************************

void dump_pools(tbx_log_fd_t *ifd, tbx_stack_t *pools, int scale)
{
    pool_entry_t *pe;
//  lio_rid_inspect_tweak_t *ri;
    lio_rid_change_entry_t *rid;
    apr_hash_index_t *hi;
    char pp1[128], pp2[128];
    char *key;
    double d1, d2;
    ex_off_t tneg, tpos, pneg, ppos;
    tbx_list_t *master;
    tbx_list_iter_t it;
    int total_finished, finished, total_todo, todo, total, ntodo, ptodo, total_ntodo, total_ptodo;

    tbx_stack_move_to_top(pools);
    tneg = tpos = 0;
    total_ntodo = total_ptodo = total_todo = total_finished = 0;
    while ((pe = tbx_stack_get_current_data(pools)) != NULL) {

        //** Make the sorted list by DS-key
        master = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
        pneg = ppos = 0;
        ptodo = ntodo = todo = finished = 0;
        for (hi = apr_hash_first(NULL, pe->pick_from); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, NULL, (void **)&rid);
            tbx_list_insert(master, rid->ds_key, rid);
            if (rid->state == 0) {
                todo++;
                if (rid->delta > 0) {
                    ptodo++;
                    ppos += rid->delta;
                } else {
                    ntodo++;
                    pneg += rid->delta;
                }
            } else {
                finished++;
            }
        }


        //** And print it
        total = todo + finished;
        d1 = pneg;
        d2 = ppos;
        info_printf(ifd, 0, "POOL: %s  Total: %d   Finished: %d  Todo: %d (n:%d [%s], p:%d [%s])\n", pe->name,
                    total, finished, todo, ntodo, tbx_stk_pretty_print_double_with_scale(scale, d1, pp1),
                    ptodo, tbx_stk_pretty_print_double_with_scale(scale, d2, pp1));

        it = tbx_list_iter_search(master, NULL, 0);
        while (tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&rid) == 0) {
            d1 = rid->delta;
            d2 = rid->tolerance;
            info_printf(ifd, 0, "RID:%s  DELTA: %s TOL: %s STATE: %d\n", rid->ds_key, tbx_stk_pretty_print_double_with_scale(scale, d1, pp1),
                        tbx_stk_pretty_print_double_with_scale(scale, d2, pp2), rid->state);
        }
        tbx_list_destroy(master);
        info_printf(ifd, 0, "\n");

        total_ntodo += ntodo;
        total_ptodo += ptodo;
        total_todo += todo;
        total_finished += finished;
        tpos += ppos;
        tneg += pneg;
        tbx_stack_move_down(pools);
    }

    total = total_finished + total_todo;
    d1 = tneg;
    d2 = tpos;
    info_printf(ifd, 0, "---------- Total: %d   Finished: %d  Todo: %d (n:%d [%s], p:%d [%s]) -----------\n\n",
                total, total_finished, total_todo, total_ntodo, tbx_stk_pretty_print_double_with_scale(scale, d1, pp1),
                total_ptodo, tbx_stk_pretty_print_double_with_scale(scale, d2, pp1));
}

//*************************************************************************
// check_pools - Checks
//*************************************************************************

void check_pools(tbx_stack_t *pools, apr_thread_mutex_t *lock, int todo_mode, int *finished, int *todo)
{
    pool_entry_t *pe;
    lio_rid_change_entry_t *rid;
    apr_hash_index_t *hi;
    int ntodo, ptodo;

    if (lock) apr_thread_mutex_lock(lock);
    tbx_stack_move_to_top(pools);
    *finished = *todo = ntodo = ptodo = 0;
    while ((pe = tbx_stack_get_current_data(pools)) != NULL) {
        for (hi = apr_hash_first(NULL, pe->pick_from); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, NULL, (void **)&rid);
            if (rid->state == 0) {
                if (rid->delta > 0) {
                    ptodo++;
                } else {
                    ntodo++;
                }
            } else {
                (*finished)++;
            }
        }

        tbx_stack_move_down(pools);
    }

    if (lock) apr_thread_mutex_unlock(lock);

    if (todo_mode == 1) {
        *todo = ntodo;
    } else if (todo_mode == 2) {
        *todo = ptodo;
    } else {
        *todo = ntodo + ptodo;
    }
}

//*************************************************************************
//  inspect_task
//*************************************************************************

gop_op_status_t inspect_task(void *arg, int id)
{
    inspect_t *w = (inspect_t *)arg;
    tbx_log_fd_t  *lfd;
    FILE *fd;
    gop_op_status_t status;
    gop_op_generic_t *gop;
    lio_exnode_t *ex;
    lio_exnode_exchange_t *exp, *exp_out;
    lio_segment_t *seg;
    char *keys[7];
    char *val[7];
    char buf[32], ebuf[128];
    char *dsegid, *ptr, *exnode2, *msg;
    lio_segment_errors_t serr;
    int v_size[7], n, repair_mode;
    int whattodo, count, err;
    tbx_inip_file_t *ifd;
    lio_inspect_args_t args;

    msg ="";
    whattodo = global_whattodo;
    keys[6] = NULL;

    log_printf(15, "inspecting fname=%s global_whattodo=%d\n", w->fname, global_whattodo);

    if (w->log_name) {  //** Using individual log files
        fd = fopen(w->log_name, "w+");
        if (fd == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Unable to open log file: %s\n", w->log_name);
            log_printf(0, "ERROR: Unable to open log file: %s\n", w->log_name);
            return(gop_failure_status);
        }

        lfd = tbx_info_create(fd, INFO_HEADER_NONE, 20);
    } else {
        lfd = lio_ifd;
        fd = NULL;
    }

    if (w->get_exnode == 1) {
        count = - lio_gc->max_attr;
        lio_getattr(lio_gc, lio_gc->creds, w->fname, NULL, "system.exnode", (void **)&w->exnode, &count);
    }

    if (w->exnode == NULL) {
        msg = " No exnode!";
        status = gop_failure_status;
        goto fini_1;
    }

    //** Kind of kludgy to load the ex twice but this is more of a prototype fn
    ifd = tbx_inip_string_read(w->exnode);
    dsegid = tbx_inip_get_string(ifd, "view", "default", NULL);
    tbx_inip_destroy(ifd);
    if (dsegid == NULL) {
        msg = "No default segment!";
        free(w->exnode);
        status = gop_failure_status;
        goto fini_1;
    }

    apr_thread_mutex_lock(lock);
    log_printf(15, "checking fname=%s segid=%s\n", w->fname, dsegid);
    tbx_log_flush();
    ptr = tbx_list_search(seg_index, dsegid);
    log_printf(15, "checking fname=%s segid=%s got=%s\n", w->fname, dsegid, ptr);
    tbx_log_flush();
    if (ptr != NULL) {
        apr_thread_mutex_unlock(lock);
        info_printf(lfd, 0, "Skipping file %s (ftype=%d). Already loaded/processed.\n", w->fname, w->ftype);
        free(dsegid);
        free(w->exnode);
        status = gop_failure_status;
        goto fini_1;
    }
    tbx_list_insert(seg_index, dsegid, dsegid);
    apr_thread_mutex_unlock(lock);

    //** If we made it here the exnode is unique and loaded.
    //** Load it
    exp = lio_lio_exnode_exchange_text_parse(w->exnode);
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, lio_gc->ess) != 0) {
        info_printf(lfd, 0, "ERROR  Failed with file %s (ftype=%d). Problem parsing exnode!\n", w->fname, w->ftype);
        status = gop_failure_status;
        goto finished;
    }

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        info_printf(lfd, 0, "ERROR  Failed with file %s (ftype=%d). No default segment!\n", w->fname, w->ftype);
        status = gop_failure_status;
        goto finished;
    }

    info_printf(lfd, 1, XIDT ": Inspecting file %s\n", segment_id(seg), w->fname);

    log_printf(15, "whattodo=%d\n", whattodo);
    //** Execute the inspection operation
    memset(&args, 0, sizeof(args));
    args.rid_lock = rid_lock;
    args.rid_changes = rid_changes;
    args.query = query;
    args.qs = gop_opque_new();
    args.qf = gop_opque_new();
    gop = segment_inspect(seg, lio_gc->da, lfd, whattodo, bufsize, &args, lio_gc->timeout);
    if (gop == NULL) {
        printf("File not found.\n");
        status = gop_failure_status;
        goto finished;
    }

    log_printf(15, "fname=%s inspect_gid=%d whattodo=%d bufsize=" XOT "\n", w->fname, gop_id(gop), whattodo, bufsize);

    tbx_log_flush();
    gop_waitall(gop);
    tbx_log_flush();
    status = gop_get_status(gop);
    log_printf(15, "fname=%s inspect_gid=%d status=%d %d\n", w->fname, gop_id(gop), status.op_status, status.error_code);

    gop_free(gop, OP_DESTROY);

    //** Print out the results
    whattodo = whattodo & INSPECT_COMMAND_BITS;
    repair_mode = 0;

    switch(whattodo) {
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
        repair_mode= 1;
    }

    //** NOTE:  if status.error_code & INSPECT_RESULT_FULL_CHECK that means the underlying segment inspect did a full byte level check.
    if ((status.op_status == OP_STATE_SUCCESS) && (status.error_code & INSPECT_RESULT_FULL_CHECK)) {
        //** Store the updated exnode back to disk
        exp_out = lio_exnode_exchange_create(EX_TEXT);
        lio_exnode_serialize(ex, exp_out);

        val[0] = NULL;
        v_size[0] = 0;
        keys[0] = "os.timestamp.system.inspect";
        val[1] = NULL;
        v_size[1] = -1;
        keys[1] = "system.inspect_errors";  //** Remove the system.*_errors
        val[2] = NULL;
        v_size[2] = -1;
        keys[2] = "system.hard_errors";
        val[3] = NULL;
        v_size[3] = -1;
        keys[3] = "system.soft_errors";
        val[4] = NULL;
        v_size[4] = -1;
        keys[4] = "system.write_errors";
        n = 5;

        count = strcmp(exp->text.text, exp_out->text.text);  //** Only update the exnode if it's changed
        if (count != 0) {  //** Do a further check to make sure the exnode hans't changed during the inspection
            count = -lio_gc->max_attr;
            exnode2 = NULL;
            lio_getattr(lio_gc, creds, w->fname, NULL, "system.exnode", (void **)&exnode2, &count);
            if (exnode2 != NULL) {
                count = strcmp(exnode2, exp->text.text);
                free(exnode2);
                if (count != 0) {
                    info_printf(lfd, 0, "WARN Exnode changed during inspection for file %s (ftype=%d). Aborting exnode update\n", w->fname, w->ftype);
                } else {
                    val[n] = exp_out->text.text;
                    v_size[n]= strlen(val[n]);
                    keys[n] = "system.exnode";
                    n++;
                }
            } else {
                val[n] = exp_out->text.text;
                v_size[n]= strlen(val[n]);
                keys[n] = "system.exnode";
                n++;
            }
        }

        if (w->set_key != NULL) {
            keys[n] = w->set_key;
            val[n] = w->set_success;
            v_size[n] = w->set_success_size;
            n++;
        }
        err= lio_multiple_setattr_op(lio_gc, creds, w->fname, NULL, keys, (void **)val, v_size, n);
        if (err != OP_STATE_SUCCESS) status.op_status = OP_STATE_FAILURE;
        lio_exnode_exchange_destroy(exp_out);
    } else {
        n = 1;
        val[0] = NULL;
        v_size[0] = 0;
        keys[0] = "os.timestamp.system.inspect";
        if (status.error_code & (INSPECT_RESULT_SOFT_ERROR|INSPECT_RESULT_HARD_ERROR)) {
            keys[1] = "system.inspect_errors";
            v_size[1] = snprintf(buf, 32, "%d", status.error_code);
            val[1] = buf;
            n++;;
        }
        if (w->set_key != NULL) {
            keys[n] = w->set_key;
            if (status.op_status == OP_STATE_SUCCESS) {
                val[n] = w->set_success;
                v_size[n] = w->set_success_size;
            } else {
                val[n] = w->set_fail;
                v_size[n] = w->set_fail_size;
            }
            n++;
        }

        exp_out = NULL;
        if (status.op_status == OP_STATE_SUCCESS) { //** Only store an updated exnode on success
            exp_out = lio_exnode_exchange_create(EX_TEXT);
            lio_exnode_serialize(ex, exp_out);
            count = strcmp(exp->text.text, exp_out->text.text);  //** Only update the exnode if it's changed
            if (count != 0) {  //** Do a further check to make sure the exnode hans't changed during the inspection
                count = -lio_gc->max_attr;
                exnode2 = NULL;
                lio_getattr(lio_gc, creds, w->fname, NULL, "system.exnode", (void **)&exnode2, &count);
                if (exnode2 != NULL) {
                    count = strcmp(exnode2, exp->text.text);
                    free(exnode2);
                    if (count != 0) {
                        info_printf(lfd, 0, "WARN Exnode changed during inspection for file %s (ftype=%d). Aborting exnode update\n", w->fname, w->ftype);
                    } else {
                        val[n] = exp_out->text.text;
                        v_size[n]= strlen(val[n]);
                        keys[n] = "system.exnode";
                        n++;
                        log_printf(15, "updating exnode\n");
                    }
                } else {
                    val[n] = exp_out->text.text;
                    v_size[n]= strlen(val[n]);
                    keys[n] = "system.exnode";
                    n++;
                    log_printf(15, "updating exnode\n");
                }
            }
        }

        if (repair_mode == 1) {
            lio_get_error_counts(lio_gc, seg, &serr);
            n += lio_encode_error_counts(&serr, &(keys[n]), &(val[n]), ebuf, &(v_size[n]), 0);
        }

        err = lio_multiple_setattr_op(lio_gc, creds, w->fname, NULL, keys, (void **)val, v_size, n);
        if (err != OP_STATE_SUCCESS) status.op_status = OP_STATE_FAILURE;
        if (exp_out != NULL) lio_exnode_exchange_destroy(exp_out);  //** Free the output exnode if it exists
    }


    //** Clean up

    //** Do the post-processing cleanup tasks
    if (status.op_status == OP_STATE_SUCCESS) {
        opque_waitall(args.qs);
    } else {
        opque_waitall(args.qf);
    }
    gop_opque_free(args.qs, OP_DESTROY);
    gop_opque_free(args.qf, OP_DESTROY);

finished:
    lio_exnode_exchange_destroy(exp);

    lio_exnode_destroy(ex);

fini_1:

    if (fd) {
        snprintf(ebuf, sizeof(ebuf), "[%d] ", w->id);
    } else {
        ebuf[0] = 0;
    }

    if (status.op_status == OP_STATE_SUCCESS) {
        info_printf(lfd, 0, "%sSuccess with file %s\n", ebuf, w->fname);
        if (fd) info_printf(lio_ifd, 0, "%sSuccess with file %s\n", ebuf, w->fname);
    } else {
        info_printf(lfd, 0, "%sERROR: Failed with file %s status=%d error_code=%d%s\n", ebuf, w->fname, status.op_status, status.error_code, msg);
        if (fd) info_printf(lio_ifd, 0, "%sERROR: Failed with file %s status=%d error_code=%d%s\n", ebuf, w->fname, status.op_status, status.error_code, msg);
    }

    free(w->fname);

    if (fd) {
        tbx_info_destroy(lfd);
        fclose(fd);
        free(w->log_name);
    }

    return(status);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j,  start_option, rg_mode, ftype, prefix_len, err, log_prefix_len;
    int force_repair, option;
    char ppbuf[32];
    char *fname, *qstr, *path, *pool_cfg, *logname, *log_prefix;
    rs_query_t *rsq;
    apr_pool_t *mpool;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    char *vals[100];
    char *keys[100];
    ex_off_t dump_iter, iter;
    int v_size[100], acount;
    int slot, pslot, q_count, gotone;
    os_object_iter_t *it;
    lio_os_regex_table_t *rp_single, *ro_single;
    lio_path_tuple_t static_tuple, tuple;
    double rtol;
    char *key_rebalance, *value;
    int submitted, good, bad, do_print, print_pools, assume_skip, base, rtol_mode;
    int pool_finished, pool_todo, check_iter, todo_mode;
    int recurse_depth = 10000;
    inspect_t *w;
    char *set_key, *set_success, *set_fail, *select_key, *select_value;
    int set_success_size, set_fail_size, select_mode, select_index;
    tbx_stack_t *pools;
    void *piter;

    bufsize = 20*1024*1024;
    base = 1;
    dump_iter = 100;
    check_iter = 100;
    todo_mode = 0;
    select_mode = INT_MIN;
    log_prefix = NULL;  //** Disable individual logs by default
    log_prefix_len = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_inspect LIO_COMMON_OPTIONS [-rd recurse_depth] [-b bufsize] [-es] [-eh] [-ew] [-rerr] [-werr] [-h | -hi][-f] [-s] [-r]\n");
        printf("            [-pc pool.cfg] [-pp iter] [-rebalance [auto|key]] [-q extra_query] [-bl key value] [-p] -o inspect_opt [LIO_PATH_OPTIONS | -]\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -1 log_prefix      - Have information logs for each file.\n");
        printf("                         The log naming convention used will be ${log_prefix}.N where N corresponds\n");
        printf("                         to the index provided in the Success/Failure line printed to the global information log.\n");
        printf("    -b bufsize         - Buffer size to use for *each* inspect. Units supported (Default=%s)\n", tbx_stk_pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    -s                 - Report soft errors, like a missing RID in the config file but the allocation is good.\n");
        printf("                         The default is to ignore these type of errors.\n");
        printf("    -r                 - Use reconstruction for all repairs. Even for data placement issues.\n");
        printf("                         Not always successful.The default is to use depot-to-depot copies if possible.\n");
        printf("                         This can lead to drive hotspots if migrating data from a failing drive\n");
        printf("    -pc pool_cfg       - Load a pool config file for use in rebalancing data across resources.\n");
        printf("    -pp iter           - Print the resulting RID pools every *iter* iteratations\n");
        printf("    -pcheck iter n|p|np - How often to check for pool convergence and the convergence criteria.  Default is %d iterations\n", check_iter);
        printf("                       The convergence criteria corresponds to exiting when all negative (n) RIDs have converged or positive (p), or both (np).\n");
        printf("    -rebalance auto|key tol - Don't use a config file instead just rebalance using pools created using the given key and tolerance.\n");
        printf("                         If the key=auto then a single pool is creted using all resources.\n");
        printf("    -h                 - Print pools using base 1000\n");
        printf("    -hi                - Print print pools using base 1024\n");
        printf("    -q  extra_query    - Extra RS query for data placement. AND-ed with default query\n");
        printf("    -bl key value      - Blacklist the given key/value combination. Multiple -bl options can be provided\n");
        printf("                         For a RID use: rid_key rid     Hostname: host hostname\n");
        printf("    -f                 - Forces data replacement even if it would result in data loss\n");
        printf("    -x                 - Stop scanning a file if an unrecoverable error is detected.\n");
        printf("    -p                 - Print the resulting query string\n");
        printf("    -es                - Check files that have SOFT errors\n");
        printf("    -eh                - Check files that have HARD errors\n");
        printf("    -ew                - Check files that have WRITE errors\n");
        printf("    -ei                - Check files that have INSPECT errors\n");
        printf("    -assume_skip       - Assume thet most files will be skipped and only get the exnode of files to inspect\n");
        printf("    -select attr mode [val] - Use the given attribute to select files for inspection\n");
        printf("                         Valid options for mode are: eq, neq, exists, and missing\n");
        printf("    -set attr success fail  - Sets the given attribute and stores the corresponding values based on success or failure\n");
        printf("                         To remove the attribute set the corresponding value to REMOVE or to store nothing use NULL\n");
        printf("    -rerr              - Force new allocates to be created on read errors\n");
        printf("    -werr              - Force new allocates to be created on write errors\n");
        printf("    -o inspect_opt     - Inspection option.  One of the following:\n");
        for (i=1; i<n_inspect; i++) {
            printf("                 %s\n", inspect_opts[i]);
        }
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        return(1);
    }

    lio_init(&argc, &argv);
    argv_list = argv;

    err = 0;

    //*** Parse the path args
    rp_single = ro_single = NULL;
    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &static_tuple, &rp_single, &ro_single);

    i=1;
    force_repair = 0;
    option = INSPECT_QUICK_CHECK;
    global_whattodo = 0;
    query = rs_query_new(lio_gc->rs);
    do_print = 0;
    print_pools = 0;
    pools = NULL;
    pool_cfg = NULL;
    q_count = 0;
    set_key = set_success = set_fail = NULL;
    set_success_size = set_fail_size = 0;
    select_key = select_value = NULL;
    select_index = -1;
    assume_skip = 0;
    acount = 1;
    key_rebalance = NULL;
    rtol = 0.05;
    rtol_mode = DELTA_MODE_PERCENT;
    keys[0] = "system.exnode";

    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-1") == 0) {  //** Enable individual log files 
            i++;
            log_prefix = argv[i];
            log_prefix_len = strlen(log_prefix);
            i++;
        } else if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
            i++;
            bufsize = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-f") == 0) { //** Force repair
            i++;
            force_repair = INSPECT_FORCE_REPAIR;
        } else if (strcmp(argv[i], "-es") == 0) { //** Check files that have soft errors
            i++;
            keys[acount] = "system.soft_errors";
            acount++;
        } else if (strcmp(argv[i], "-eh") == 0) { //** Check files that have hard errors
            i++;
            keys[acount] = "system.hard_errors";
            acount++;
        } else if (strcmp(argv[i], "-ew") == 0) { //** Check files that have write errors
            i++;
            keys[acount] = "system.write_errors";
            acount++;
        } else if (strcmp(argv[i], "-ei") == 0) { //** Check files that have inspect errors
            i++;
            keys[acount] = "system.inspect_errors";
            acount++;
        } else if (strcmp(argv[i], "-assume_skip") == 0) { //** Skip getting the exnode for all objects
            i++;
            assume_skip = 1;
            acount = 0;
        } else if (strcmp(argv[i], "-r") == 0) { //** Force reconstruction
            i++;
            global_whattodo |= INSPECT_FORCE_RECONSTRUCTION;
        } else if (strcmp(argv[i], "-x") == 0) { //** Kick out if an unrecoverable error is detected
            i++;
            global_whattodo |= INSPECT_FAIL_ON_ERROR;
        } else if (strcmp(argv[i], "-s") == 0) { //** Report soft errors
            i++;
            global_whattodo |= INSPECT_SOFT_ERROR_FAIL;
        } else if (strcmp(argv[i], "-rerr") == 0) { //** Repair read errors
            i++;
            global_whattodo |= INSPECT_FIX_READ_ERROR;
        } else if (strcmp(argv[i], "-werr") == 0) { //** Repair write errors
            i++;
            global_whattodo |= INSPECT_FIX_WRITE_ERROR;
        } else if (strcmp(argv[i], "-set") == 0) { //** Set clause
            i++;
            set_key = argv[i];
            i++;
            set_success = argv[i];
            i++;
            set_fail = argv[i];
            i++;

            if (set_success != NULL) {
                if (strcmp(set_success, "NULL") == 0) {
                    set_success = NULL;
                    set_success_size = 0;
                } else if (strcmp(set_success, "REMOVE") == 0) {
                    set_success = NULL;
                    set_success_size = -1;
                } else {
                    set_success_size = strlen(set_success);
                }
            }

            if (set_fail != NULL) {
                if (strcmp(set_fail, "NULL") == 0) {
                    set_fail = NULL;
                    set_fail_size = 0;
                } else if (strcmp(set_fail, "REMOVE") == 0) {
                    set_fail = NULL;
                    set_fail_size = -1;
                } else {
                    set_fail_size = strlen(set_fail);
                }
            }
        } else if (strcmp(argv[i], "-select") == 0) { //** USer specified selection
            i++;
            select_key = argv[i];
            i++;
            for (select_mode=0; select_mode < 4; select_mode++) {
                if (strcmp(select_mode_string[select_mode], argv[i]) == 0) break;
            }
            if (select_mode == 4) {
                printf("Invalid select mode: %s\n", argv[i]);
                return(1);
            }
            i++;
            if ((select_mode == SMODE_EQ) || (select_mode == SMODE_NEQ)) {
                select_value = argv[i];
                i++;
            }

            select_index = acount;
            keys[acount] = select_key;
            acount++;
        } else if (strcmp(argv[i], "-p") == 0) { //** Print resulting query string
            i++;
            do_print = 1;
        } else if (strcmp(argv[i], "-pp") == 0) { //** Print the pool configs
            i++;
            print_pools = 1;
            pools = tbx_stack_new();
            dump_iter = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-pcheck") == 0) { //** Change the converged iteration check interval
            i++;
            check_iter = tbx_stk_string_get_integer(argv[i]);
            i++;
            if (strcmp(argv[i], "n") == 0) {
                todo_mode = 1;
            } else if (strcmp(argv[i], "p") == 0) {
                todo_mode = 2;
            } else if (strcmp(argv[i], "np") == 0) {
                todo_mode = 3;
            } else {
                printf("ERROR:  Invalid convergence mode!  Got %s should be either n, p, or np,\n", argv[i]);
            }
            i++;
        } else if (strcmp(argv[i], "-pc") == 0) { //** Load a pool config for rebalancing
            i++;
            pool_cfg = argv[i];
            i++;

        } else if (strcmp(argv[i], "-rebalance") == 0) { //** Auto generate a pool config using the given key/tol
            i++;
            key_rebalance = argv[i];
            i++;

            value = argv[i];
            j = strlen(value);
            if (value[j-1] == '%') {
                rtol_mode = DELTA_MODE_PERCENT;
                value[j-1] = 0;
            } else {
                rtol_mode = DELTA_MODE_ABS;
            }

            rtol = tbx_stk_string_get_double(value);
            i++;
            if (todo_mode == 0) todo_mode = 3;
            log_printf(5, "REBALANCE: key=%s tmode=%d tol=%lf\n", key_rebalance, rtol_mode, rtol);
        } else if (strcmp(argv[i], "-h") == 0) {  //** Use base 10
            i++;
            base = 1000;
        } else if (strcmp(argv[i], "-hi") == 0) {  //** Use base 2
            i++;
            base = 1024;
        } else if (strcmp(argv[i], "-q") == 0) { //** Add additional query
            i++;
            rsq = rs_query_parse(lio_gc->rs, argv[i]);
            if (rsq == NULL) {
                printf("ERROR parsing Query: %s\nAborting!\n",argv[i]);
                exit(1);
            }
            q_count++;
            rs_query_append(lio_gc->rs, query, rsq);
            rs_query_destroy(lio_gc->rs, rsq);
            i++;
        } else if (strcmp(argv[i], "-bl") == 0) { //** Blacklist
            i++;
            q_count++;
            rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_KV, argv[i], RSQ_BASE_KV_EXACT, argv[i+1], RSQ_BASE_KV_EXACT);
            rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_NOT, "*", RSQ_BASE_KV_ANY, "*", RSQ_BASE_KV_ANY);
            i = i + 2;
        } else if (strcmp(argv[i], "-o") == 0) { //** Inspect option
            i++;
            option = -1;
            for(j=1; j<n_inspect; j++) {
                if (strcasecmp(inspect_opts[j], argv[i]) == 0) {
                    option = j;
                    break;
                }
            }
            if (option == -1) {
                printf("Invalid inspect option:  %s\n", argv[i]);
                abort();
            }
            i++;
        }

    } while ((start_option < i) && (i<argc));
    start_option = i;

    //** Finish forming the query.  We need to add all the AND operations
    if (q_count == 0) {
        rs_query_destroy(lio_gc->rs, query);
        query = NULL;
    } else {
        for (j=0; j<q_count-1; j++) {
            rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_AND, "*", RSQ_BASE_KV_ANY, "*", RSQ_BASE_KV_ANY);
        }
    }

    //** Print the resulting query
    if (do_print == 1) {
        qstr = rs_query_print(lio_gc->rs, query);
        printf("RS query=%s\n", qstr);
        free(qstr);
    }

    //** See if we need to load the pool config for a rebalance
    if ((pool_cfg != NULL) || (key_rebalance != NULL)) {
        assert_result(apr_pool_create(&rid_mpool, NULL), APR_SUCCESS);
        apr_thread_mutex_create(&rid_lock, APR_THREAD_MUTEX_DEFAULT, rid_mpool);
        rid_changes = (pool_cfg != NULL) ? load_pool_config(pool_cfg, rid_mpool, pools) : rebalance_pool(rid_mpool, pools, key_rebalance, rtol, rtol_mode);
        if (print_pools) {
            info_printf(lio_ifd, 0, "------------------Starting POOL state-----------------\n");
            dump_pools(lio_ifd, pools, base);
        }
    }


    global_whattodo |= option;
    if ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR)) global_whattodo |= force_repair;

    if (rg_mode == 0) {
        if (argc <= start_option) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_option--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    //** Make the path iterator
    piter = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));

    tbx_type_malloc_clear(w, inspect_t, lio_parallel_task_count);
    seg_index = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_simple_free, NULL);
    assert_result(apr_pool_create(&mpool, NULL), APR_SUCCESS);
    apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool);

    q = gop_opque_new();
    opque_start_execution(q);

    slot = pslot = 0;
    submitted = good = bad = 0;
    iter = 0;
    pool_todo = 1;  //** Set it to a positive value to make it into the loop unless the pools are actually being used.

    if (rid_changes != NULL) {
        check_pools(pools, rid_lock, todo_mode, &pool_finished, &pool_todo);
        if (pool_todo <= 0) {
            info_printf(lio_ifd, 0, "==================== All pools have converged! iteration: " XOT " =====================\n", iter);
        }
    }

    install_signal_handler();

    apr_thread_mutex_lock(shutdown_lock);
    while (((path = tbx_stdinarray_iter_next(piter)) != NULL) && (pool_todo > 0) && (shutdown_now == 0)) {
        apr_thread_mutex_unlock(shutdown_lock);
        log_printf(5, "path=%s argc=%d rg_mode=%d pslot=%d\n", path, argc, rg_mode, pslot);

        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
            free(path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        creds = tuple.lc->creds;

        for (i=0; i< acount; i++) v_size[i] = -tuple.lc->max_attr;
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE_FLAG, recurse_depth, keys, (void **)vals, v_size, acount);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            err = 2;
            goto finished;
        }

        apr_thread_mutex_lock(shutdown_lock);
        while (((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) && (pool_todo > 0) && (shutdown_now == 0)) {
            apr_thread_mutex_unlock(shutdown_lock);
            gotone = ((acount == 1) && (assume_skip == 0)) ? 1 : 0;
            for (i=1; i<acount; i++) {
                if ((vals[i] != NULL) && (i != select_index)) {
                    free(vals[i]);
                    gotone = 1;
                }
            }

            if (select_key != NULL) {
                switch(select_mode) {
                case (SMODE_EQ):
                    if (v_size[select_index] > 0) {
                        if (strcmp(select_value, vals[select_index]) == 0) gotone = 1;
                    }
                    break;
                case (SMODE_NEQ):
                    if (v_size[select_index] > 0) {
                        if (strcmp(select_value, vals[select_index]) != 0) gotone = 1;
                    }
                    break;
                case (SMODE_EXISTS):
                    if (v_size[select_index] > -1) gotone = 1;
                    break;
                case (SMODE_MISSING):
                    if (v_size[select_index] < 0) gotone = 1;
                    break;
                }

                if (vals[select_index] != NULL) free(vals[select_index]);
            }

            if (gotone == 1) {
                if (print_pools) {
                    if (dump_iter > 0) {
                        if ((iter % dump_iter) == 0) {
                            apr_thread_mutex_lock(rid_lock);
                            info_printf(lio_ifd, 0, "------------------ POOL state for iteration " XOT " -----------------\n", iter);
                            info_printf(lio_ifd, 0, "ITERATION: " XOT "\n", iter);
                            dump_pools(lio_ifd, pools, base);
                            apr_thread_mutex_unlock(rid_lock);
                        }
                    }
                }

                if (rid_changes != NULL) {
                    if ((iter % check_iter) == 0) {
                        check_pools(pools, rid_lock, todo_mode, &pool_finished, &pool_todo);
                        if (pool_todo <= 0) {
                            info_printf(lio_ifd, 0, "==================== All pools have converged! iteration: " XOT " =====================\n", iter);
                        }
                    }
                }
                iter++;

                w[slot].exnode = (assume_skip == 0) ? vals[0] : NULL;
                w[slot].get_exnode = assume_skip;
                w[slot].fname = fname;
                w[slot].ftype = ftype;
                w[slot].set_key = set_key;
                w[slot].set_success = set_success;
                w[slot].set_success_size = set_success_size;
                w[slot].set_fail = set_fail;
                w[slot].set_fail_size = set_fail_size;

                if (log_prefix != NULL) {
                    tbx_type_malloc(logname, char, log_prefix_len + 20);
                    snprintf(logname, log_prefix_len + 20, "%s." XOT, log_prefix, iter);
                    w[slot].id = iter;
                    w[slot].log_name = logname;
                } else {
                    w[slot].log_name = NULL;
                }

                vals[0] = NULL;
                fname = NULL;

                submitted++;
                gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, inspect_task, (void *)&(w[slot]), NULL, 1);
                gop_set_myid(gop, slot);
                log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), slot, fname);
//info_printf(lio_ifd, 0, "n=%d gid=%d slot=%d fname=%s\n", submitted, gop_id(gop), slot, fname);
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
            } else {  //** Do some cleanup since we aren't doing a check
                free(fname);
                if (vals[0] != NULL) free(vals[0]);
            }
        }
        apr_thread_mutex_unlock(shutdown_lock);

        lio_destroy_object_iter(tuple.lc, it);

        if (rp_single != NULL) {
            lio_os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            lio_os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
        lio_path_release(&tuple);
    }
    apr_thread_mutex_unlock(shutdown_lock);

    //** Wait for everything to complete
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            good++;
        } else {
            bad++;
        }
        gop_free(gop, OP_DESTROY);
    }


    gop_opque_free(q, OP_DESTROY);

    apr_thread_mutex_destroy(lock);
    apr_pool_destroy(mpool);
    tbx_list_destroy(seg_index);

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Submitted: %d   Success: %d   Fail: %d\n", submitted, good, bad);

    if (submitted != (good+bad)) {
        info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
        err = 2;
    }
    if (bad > 0) {
        info_printf(lio_ifd, 0, "ERROR Some files failed inspection!\n");
        err = 1;
    }


    if (print_pools) {
        if (dump_iter != 0) {
            info_printf(lio_ifd, 0, "------------------Final POOL state-----------------\n");
            dump_pools(lio_ifd, pools, base);
        }
    }

    free(w);

    tbx_stdinarray_iter_destroy(piter);
    if (rid_lock != NULL) apr_thread_mutex_destroy(rid_lock);
    if (rid_mpool != NULL) apr_pool_destroy(rid_mpool);
finished:
    lio_shutdown();

    return(err);
}
