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

#define _log_module_index 200

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

#define MAX_SET 1000


//*************************************************************************
// load_file - Loads a file from disk
//*************************************************************************

void load_file(char *fname, char **val, int *v_size)
{
    FILE *fd;
    int i;

    *v_size = 0;
    *val = NULL;

    fd = fopen(fname, "r");
    if (fd == NULL) {
        info_printf(lio_ifd, 0, "ERROR opeing file=%s!  Exiting\n", fname);
        exit(1);
    }
    fseek(fd, 0, SEEK_END);

    i = ftell(fd);
    type_malloc(*val, char, i+1);
    (*val)[i] = 0;
    *v_size = i;

    fseek(fd, 0, SEEK_SET);
    if (fread(*val, i, 1, fd) != 1) { //**
        info_printf(lio_ifd, 0, "ERROR reading file=%s! Exiting\n", fname);
        exit(1);
    }
    fclose(fd);

//info_printf(lio_ifd, 0, "fname=%s size=%d val=%s\n", fname, i, *val);

}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, rg_mode, start_option, start_index, err, fin, nfailed;
    lio_path_tuple_t tuple;
    os_regex_table_t *rp_single, *ro_single;
    os_object_iter_t *it;
    os_fd_t *fd;  //** This is just used for manipulating symlink attributes
    char *bstate;
    char *key[MAX_SET];
    char *val[MAX_SET];
    int v_size[MAX_SET];
    int n_keys;
    char *dkey[MAX_SET], *tmp;
    char *sobj[MAX_SET], *skey[MAX_SET];
    int n_skeys, return_code;
    int ftype, prefix_len;
    char *fname;

    memset(dkey, 0, sizeof(dkey));
    memset(sobj, 0, sizeof(sobj));
    memset(skey, 0, sizeof(skey));

    char *delims = "=";

    int recurse_depth = 10000;
    int obj_types = OS_OBJECT_FILE;
    return_code = 0;

//printf("argc=%d\n", argc);
    if (argc < 2) {
        printf("\n");
        printf("lio_setattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] -as key=value | -ar key | -af key=vfilename | -al key=obj_path/dkey LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        printf("    -delim c           - Key/value delimeter characters.  Defauls is %s.\n", delims);
        printf("    -as key=value      - Breaks up the literal string into the key/value pair and stores it.\n");
        printf("    -ar key            - Remove the key.\n");
        printf("    -af key=vfilename  - Similar to -as but the value is loaded from the given vfilename.\n");
        printf("    -al key=sobj_path/skey - Symlink the key to another objects (sobj_path) key(skey).\n");
        printf("\n");
        printf("       NOTE: Multiple -as/-ar/-af/-al clauses are allowed\n\n");
        return(1);
    }

    lio_init(&argc, &argv);


    //*** Parse the args
    rp_single = ro_single = NULL;
    n_keys = 0;
    n_skeys = 0;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
            i++;
            obj_types = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-delim") == 0) {  //** Get the delimiter
            i++;
            delims = argv[i];
            i++;
        } else if (strcmp(argv[i], "-as") == 0) {  //** String attribute
            i++;
            key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
            val[n_keys] = bstate;  //** Everything else is the value
            v_size[n_keys] = strlen(val[n_keys]);
            if (strcmp(val[n_keys], "") == 0) val[n_keys] = NULL;
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-ar") == 0) {  //** Remove the attribute
            i++;
            key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
            val[n_keys] = NULL;
            v_size[n_keys] = -1;
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-af") == 0) {  //** File attribute
            i++;
            key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
            load_file(string_token(NULL, delims, &bstate, &fin), &(val[n_keys]), &(v_size[n_keys]));
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-al") == 0) {  //** Symlink attributes
            i++;
            dkey[n_skeys] = string_token(argv[i], delims, &bstate, &fin);
            tmp = string_token(NULL, delims, &bstate, &fin);
            os_path_split(tmp, &(sobj[n_skeys]), &(skey[n_skeys]));
            n_skeys++;
            i++;
        }
    } while ((start_option < i) && (i<argc));
    start_index = i;

    if (rg_mode == 0) {
        if (start_index >= argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_index--;  //** The 1st entry will be the rp created in lio_parse_path_options
    }

    nfailed = 0;
    for (j=start_index; j<argc; j++) {
        log_printf(5, "path_index=%d argc=%d rg_mode=%d\n", j, argc, rg_mode);
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, argv[j]);
            rp_single = os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        if (n_keys > 0) {
            err = gop_sync_exec(gop_lio_regex_object_set_multiple_attrs(tuple.lc, tuple.creds, NULL, rp_single,  ro_single, obj_types, recurse_depth, key, (void **)val, v_size, n_keys));
            if (err != OP_STATE_SUCCESS) {
                return_code = EIO;
                info_printf(lio_ifd, 0, "ERROR with operation! \n");
                nfailed++;
            }
        }

        if (n_skeys > 0) {  //** For symlink attrs we have to manually iterate
            it = lio_create_object_iter(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, NULL, recurse_depth, NULL, 0);
            if (it == NULL) {
                info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
                nfailed++;
                return_code = EIO;
                goto finished;
            }

            while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
                err = gop_sync_exec(os_open_object(tuple.lc->os, tuple.creds, fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, 30));
                if (err != OP_STATE_SUCCESS) {
                    info_printf(lio_ifd, 0, "ERROR: opening file: %s.  Skipping.\n", fname);
                    nfailed++;
                } else {
                    //** Do the symlink
                    err = gop_sync_exec(os_symlink_multiple_attrs(tuple.lc->os, tuple.creds, sobj, skey, fd, dkey, n_skeys));
                    if (err != OP_STATE_SUCCESS) {
                        return_code = EIO;
                        info_printf(lio_ifd, 0, "ERROR: with linking file: %s\n", fname);
                        nfailed++;
                    }

                    //** Close the file
                    err = gop_sync_exec(os_close_object(tuple.lc->os, fd));
                    if (err != OP_STATE_SUCCESS) {
                        return_code = EIO;
                        info_printf(lio_ifd, 0, "ERROR: closing file: %s\n", fname);
                        nfailed++;
                    }
                }

                free(fname);
            }

            lio_destroy_object_iter(tuple.lc, it);
        }

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

finished:
    for (i=0; i<n_skeys; i++) {
        if (sobj[i] != NULL) free(sobj[i]);
        if (skey[i] != NULL) free(skey[i]);
    }

    lio_shutdown();

    return(return_code);
}


