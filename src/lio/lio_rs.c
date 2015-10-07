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

#define _log_module_index 219


#include <assert.h>
#include <apr_signal.h>
#include "lio_fuse.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

apr_thread_mutex_t *lock;
apr_thread_cond_t *cond;
apr_pool_t *mpool;

typedef struct {
    char *rid;
    char *host;
    char *ds_key;
    int status;
    ex_off_t total;
    ex_off_t free;
    ex_off_t used;
} rid_summary_t;


//*************************************************************************
// print_rid_summary - Prints the RID summary
//*************************************************************************

void print_rid_summary(char *config, int base)
{
    list_t *table;
    list_iter_t it;
    inip_group_t *ig;
    inip_file_t *kf;
    inip_element_t *ele;
    char *key, *value;
    char fbuf[20], ubuf[20], tbuf[20];
    char *state[5] = { "UP      ", "IGNORE  ", "NO_SPACE", "DOWN    ", "INVALID " };
    int n, n_usable;
    rid_summary_t *rsum;
    ex_off_t space_total, space_free, space_used;
    ex_off_t up_total, up_free, up_used;

    space_total = space_free = space_used = 0;
    up_total = up_free = up_used = n_usable = 0;

    //** Create the table where we hold the info
    table = list_create(0, &list_string_compare, NULL, NULL, free);

    //** Open the file
    assert(kf = inip_read_text(config));

    //** And load it
    ig = inip_first_group(kf);
    while (ig != NULL) {
        key = inip_get_group(ig);
        if (strcmp("rid", key) == 0) {  //** Found a resource
            type_malloc_clear(rsum, rid_summary_t, 1);

            //** Now cycle through the attributes
            ele = inip_first_element(ig);
            while (ele != NULL) {
                key = inip_get_element_key(ele);
                value = inip_get_element_value(ele);
                if (strcmp(key, "rid_key") == 0) {  //** This is the RID so store it separate
                    rsum->rid = value;
                } else if (strcmp(key, "ds_key") == 0) {  //** Data service key
                    rsum->ds_key = value;
                } else if (strcmp(key, "host") == 0) {  //** Host
                    rsum->host = value;
                } else if (strcmp(key, "status") == 0) {  //** Free space
                    sscanf(value, "%d", &n);
                    rsum->status = ((n>=0) && (n<=3)) ? n : 4;
                } else if (strcmp(key, "space_free") == 0) {  //** Free space
                    sscanf(value, XOT, &(rsum->free));
                    space_free += rsum->free;
                } else if (strcmp(key, "space_used") == 0) {  //** Used space
                    sscanf(value, XOT, &(rsum->used));
                    space_used += rsum->used;
                } else if (strcmp(key, "space_total") == 0) {  //** Total space
                    sscanf(value, XOT, &(rsum->total));
                    space_total += rsum->total;
                }

                ele = inip_next_element(ele);
            }

            if (rsum->status == 0) {
                n_usable++;
                up_free += rsum->free;
                up_used += rsum->used;
                up_total += rsum->total;
            }
            list_insert(table, rsum->rid, rsum);
        }

        ig = inip_next_group(ig);
    }


    //** Now print the summary
    printf("        RID             State             Host                      Used       Free        Total\n");
    printf("--------------------  --------  ------------------------------   ---------   ---------   ---------\n");
    it = list_iter_search(table, NULL, 0);
    while (list_next(&it, (list_key_t **)&key, (list_data_t **)&rsum) == 0) {
        printf("%-20s  %8s  %-30s   %8s   %8s   %8s\n", rsum->rid, state[rsum->status], rsum->host,
               pretty_print_double_with_scale(base, (double)rsum->used, ubuf),
               pretty_print_double_with_scale(base, (double)rsum->free, fbuf),
               pretty_print_double_with_scale(base, (double)rsum->total, tbuf));
    }

    printf("--------------------------------------------------------------   ---------   ---------   ---------\n");
    printf("Usable Resources:%4d                                            %8s   %8s   %8s\n", n_usable,
           pretty_print_double_with_scale(base, (double)up_used, ubuf),
           pretty_print_double_with_scale(base, (double)up_free, fbuf),
           pretty_print_double_with_scale(base, (double)up_total, tbuf));
    printf("Total Resources: %4d                                            %8s   %8s   %8s\n", list_key_count(table),
           pretty_print_double_with_scale(base, (double)space_used, ubuf),
           pretty_print_double_with_scale(base, (double)space_free, fbuf),
           pretty_print_double_with_scale(base, (double)space_total, tbuf));

    list_destroy(table);

    //** Close the file
    inip_destroy(kf);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int start_option, i, watch, summary, base;
    rs_mapping_notify_t notify, me;
    apr_time_t dt;
    char *config;

//printf("argc=%d\n", argc);

    if (argc < 2) {
        printf("\n");
        printf("lio_rs LIO_COMMON_OPTIONS [-w] [-b2 | -b10] [-s | -f]\n");
        lio_print_options(stdout);
        printf("    -w                 - Watch for RID configuration changes. Press ^C to exit\n");
        printf("    -b2                - Use powers of 2 for units(default)\n");
        printf("    -b10               - Use powers of 10 for units\n");
        printf("    -s                 - Print a RID space usage summary\n");
        printf("    -f                 - Print the full RID configuration\n");
        return(1);
    }

    lio_init(&argc, &argv);

    watch = 0;
    summary = 0;
    base = 1024;

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-w") == 0) { //** Watch for any RID changes
            i++;
            watch = 1;
        } else if (strcmp(argv[i], "-s") == 0) {  //** Print space summary instead of full conifg
            i++;
            summary = 1;
        } else if (strcmp(argv[i], "-b2") == 0) {  //** base-2 units
            i++;
            base = 1024;
        } else if (strcmp(argv[i], "-b10") == 0) {  //** base-10 units
            i++;
            base = 1000;
        }
    } while ((start_option < i) && (i<argc));

    //** Make the APR stuff
    assert(apr_pool_create(&mpool, NULL) == APR_SUCCESS);
    apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool);
    apr_thread_cond_create(&cond, mpool);

//if (watch == 1) {
//  printf("Sleeping for 60s\n"); fflush(stdout);
//  sleep(60);
//  printf("Woken up\n"); fflush(stdout);
//}

    memset(&notify, 0, sizeof(notify));
    notify.lock = lock;
    notify.cond = cond;
    me = notify;
    me.map_version = 1; //** This triggers the initial load
    rs_register_mapping_updates(lio_gc->rs, &notify);
    dt = apr_time_from_sec(1);
    do {
        //** Check for an update
        apr_thread_mutex_lock(lock);
        if (watch == 1) apr_thread_cond_timedwait(notify.cond, notify.lock, dt);
        i = ((me.map_version != notify.map_version) || (me.status_version != notify.status_version)) ? 1 : 0;
        me = notify;
        apr_thread_mutex_unlock(lock);

        if (i != 0) {
            config = rs_get_rid_config(lio_gc->rs);

            printf("Map Version: %d  Status Version: %d\n", me.map_version, me.status_version);
            printf("--------------------------------------------------------------------------------------------------\n");

            if (config == NULL) {
                printf("ERROR NULL config!\n");
            } else if (summary == 1) {
                print_rid_summary(config, base);
            } else {
                printf("%s", config);
            }

            printf("--------------------------------------------------------------------------------------------------\n");

            if (config != NULL) free(config);
        }
    } while (watch == 1);

    info_printf(lio_ifd, 5, "BEFORE unregister\n");
    info_flush(lio_ifd);
    rs_unregister_mapping_updates(lio_gc->rs, &notify);
    info_printf(lio_ifd, 5, "AFTER unregister\n");
    info_flush(lio_ifd);

    //** Cleanup
    apr_pool_destroy(mpool);

//  info_printf(lio_ifd, 5, "AFTER shutdown\n"); info_flush(lio_ifd);
    lio_shutdown();

    return(0);
}
