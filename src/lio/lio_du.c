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

#define _log_module_index 194

#include <assert.h>
#include <tbx/assert_result.h>
#include "exnode.h"
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "thread_pool.h"
#include "lio.h"
#include <tbx/string_token.h>

typedef struct {
    char *fname;
    int64_t bytes;
    int64_t count;
    int ftype;
} du_entry_t;

lio_path_tuple_t tuple;

int base = 1;

//*************************************************************************
// ls_format_entry - Prints an LS entry
//*************************************************************************

void du_format_entry(tbx_log_fd_t *ifd, du_entry_t *de, int sumonly)
{
    char *dname;
    char ppsize[128];
    double fsize;
    long int n;

    fsize = de->bytes;
    if (base == 1) {
        sprintf(ppsize, I64T, de->bytes);
    } else {
        tbx_stk_pretty_print_double_with_scale(base, fsize, ppsize);
    }

    dname = de->fname;

    if (sumonly == 1) {
        n = ((de->ftype & OS_OBJECT_DIR) > 0) ? de->count : 1;
        info_printf(ifd, 0, "%10s  %10ld  %s\n", ppsize, n, dname);
    } else {
        info_printf(ifd, 0, "%10s  %s\n", ppsize, dname);
    }

    if (dname != de->fname) free(dname);

    return;
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, ftype, rg_mode, start_index, start_option, nosort, prefix_len, plen;
    char *fname;
    du_entry_t *de;
    tbx_list_t *table, *sum_table, *lt;
    os_regex_table_t *rp_single, *ro_single;
    os_object_iter_t *it;
    tbx_list_iter_t lit;
    char *key = "system.exnode.size";
    char *val, *file;
    int64_t bytes;
    ex_off_t total_files, total_bytes;
    int v_size, sumonly, ignoreln;
    int recurse_depth = 10000;
    int return_code = 0;
    du_entry_t du_total;

    if (argc < 2) {
        printf("\n");
        printf("lio_du LIO_COMMON_OPTIONS [-rd recurse_depth] [-ns] [-h|-hi] [-s] [-ln] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -ns                - Don't sort the output\n");
        printf("    -h                 - Print using base 1000\n");
        printf("    -hi                - Print using base 1024\n");
        printf("    -s                 - Print directory summaries only\n");
        printf("    -ln                - Follow links.  Otherwise they are ignored\n");
        return(1);
    }

    lio_init(&argc, &argv);

    rp_single = ro_single = NULL;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    //*** Parse the args
    nosort = 0;
    base = 1;
    ignoreln = 1;
    sumonly = 0;

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-ns") == 0) {  //** Strip off the path prefix
            i++;
            nosort = 1;
        } else if (strcmp(argv[i], "-s") == 0) {  //** Summary only
            i++;
            sumonly = 1;
        } else if (strcmp(argv[i], "-h") == 0) {  //** Use base 10
            i++;
            base = 1000;
        } else if (strcmp(argv[i], "-hi") == 0) {  //** Use base 2
            i++;
            base = 1024;
        } else if (strcmp(argv[i], "-ln") == 0) {  //** Follow links
            i++;
            ignoreln = 0;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;

    if (sumonly == 1) {
        nosort = 0;  //** Doing a tally overides the no sort option
        sum_table = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);
    }

    if (rg_mode == 0) {
        if (i>=argc) {
            printf("Missing directory!\n");
            return(2);
        }
    } else {
        start_index--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    if (sumonly == 1) {
        info_printf(lio_ifd, 0, "  Size      File count            Filename\n");
        info_printf(lio_ifd, 0, "----------  ----------  ------------------------------\n");
    } else {
        info_printf(lio_ifd, 0, "  Size               Filename\n");
        info_printf(lio_ifd, 0, "----------  ------------------------------\n");
    }

    table = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);

    total_files = total_bytes = 0;

    for (j=start_index; j<argc; j++) {
        log_printf(5, "path_index=%d argc=%d rg_mode=%d\n", j, argc, rg_mode);
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, argv[j]);
            lio_path_wildcard_auto_append(&tuple);
            rp_single = os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        //** Make the toplevel list
        if (sumonly == 1) {
            log_printf(15, "MAIN SUMONLY=1\n");
            v_size = -1024;
            val = NULL;
            it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_ANY, 0, &key, (void **)&val, &v_size, 1);
            if (it == NULL) {
                log_printf(0, "ERROR: Failed with object_iter creation\n");
                return_code = EIO;
                goto finished;
            }

            while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
                if (((ftype & OS_OBJECT_SYMLINK) > 0) && (ignoreln == 1)) continue;  //** Ignoring links

                log_printf(15, "sumonly inserting fname=%s\n", fname);
                tbx_type_malloc_clear(de, du_entry_t, 1);
                plen = strlen(fname);
                tbx_type_malloc(de->fname, char, plen + 2);
                memcpy(de->fname, fname, plen);
                de->fname[plen] = '/';
                de->fname[plen+1] = 0;
                free(fname);
                de->ftype = ftype;

                if (val != NULL) sscanf(val, I64T, &(de->bytes));
                tbx_list_insert(sum_table, de->fname, de);

                v_size = -1024;
                free(val);
                val = NULL;
            }

            lio_destroy_object_iter(tuple.lc, it);

            log_printf(15, "sum_table=%d\n", tbx_list_key_count(sum_table));
        }

        log_printf(15, "MAIN LOOP\n");

        v_size = -1024;
        val = NULL;
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_ANY, recurse_depth, &key, (void **)&val, &v_size, 1);
        if (it == NULL) {
            log_printf(0, "ERROR: Failed with object_iter creation\n");
            return_code = EIO;
            goto finished;
        }

        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            if (((ftype & OS_OBJECT_SYMLINK) > 0) && (ignoreln == 1)) continue;  //** Ignoring links

            if ((sumonly == 1) && ((ftype & OS_OBJECT_FILE) > 0)) {
                bytes = 0;
                if (val != NULL) sscanf(val, I64T, &bytes);

                lit = tbx_list_iter_search(sum_table, NULL, 0);
                while ((tbx_list_next(&lit, (tbx_list_key_t **)&file, (tbx_list_data_t **)&de)) == 0) {
                    if ((strncmp(de->fname, fname, strlen(de->fname)) == 0) && ((de->ftype & OS_OBJECT_DIR) > 0)) {
                        log_printf(15, "accum de->fname=%s fname=%s\n", de->fname, fname);
                        de->bytes += bytes;
                        de->count++;
                    }
                }
                free(fname);
            } else {
                tbx_type_malloc_clear(de, du_entry_t, 1);
                de->fname = fname;
                de->ftype = ftype;

                if (val != NULL) sscanf(val, I64T, &(de->bytes));

                if (nosort == 1) {
                    du_format_entry(lio_ifd, de, sumonly);
                    free(de->fname);
                    free(de);
                } else {
                    tbx_list_insert(table, de->fname, de);
                }
            }

            v_size = -1024;
            free(val);
            val = NULL;
        }

        lio_destroy_object_iter(tuple.lc, it);

        lio_path_release(&tuple);
        if (rp_single != NULL) {
            os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
    }

    if ((nosort == 1) && (sumonly == 0)) goto finished;  //** Check if we're done

    //** Now summarize and print things
    lt = (sumonly == 1) ? sum_table : table;

    lit = tbx_list_iter_search(lt, "", 0);
    while ((tbx_list_next(&lit, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&de)) == 0) {
        total_bytes += de->bytes;
        total_files += (de->ftype & OS_OBJECT_FILE) ? 1 : de->count;
        du_format_entry(lio_ifd, de, sumonly);
    }

    if (sumonly == 1) {
        info_printf(lio_ifd, 0, "----------  ----------  ------------------------------\n");
    } else {
        info_printf(lio_ifd, 0, "----------  ------------------------------\n");
    }

    du_total.fname = "TOTAL";
    du_total.bytes = total_bytes;
    du_total.count = total_files;
    du_total.ftype = OS_OBJECT_DIR;
    du_format_entry(lio_ifd, &du_total, sumonly);

    if (sumonly == 1) tbx_list_destroy(sum_table);
    tbx_list_destroy(table);

finished:
    lio_shutdown();

    return(return_code);
}

