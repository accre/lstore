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

#define _log_module_index 195

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <lio/lio.h>
#include <lio/os.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, ftype, rg_mode, start_option, start_index, prefix_len, nopre, return_code;
    char *fname, *path;
    lio_path_tuple_t tuple;
    lio_os_regex_table_t *rp_single, *ro_single;
    tbx_stdinarray_iter_t *it_args;
    os_object_iter_t *it = NULL;

    int recurse_depth = 10000;
    int obj_types = OS_OBJECT_FILE_FLAG;
    return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_find LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-nopre] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        printf("    -nopre             - Don't print the scan common prefix\n");
        return(1);
    }

    lio_init(&argc, &argv);


    //*** Parse the args
    rp_single = ro_single = NULL;
    nopre = 0;

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
        } else if (strcmp(argv[i], "-nopre") == 0) {  //** Strip off the path prefix
            i++;
            nopre = 1;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;


    if (rg_mode == 0) {
        if (i>=argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(EINVAL);
        }
    } else {
        start_index--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    it_args = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while (1) {
        if (rg_mode == 0) {
            //** Create the simple path iterator
            path = tbx_stdinarray_iter_next(it_args);
            if (!path) break;
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {  //** Can't resolve path
                fprintf(stderr, "Unable to resolve path: %s\n", path);
                free(path);
                return_code = EINVAL;
                continue;
            }
            free(path);
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        it = lio_create_object_iter(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, NULL, recurse_depth, NULL, 0);
        if (it == NULL) {
            log_printf(0, "ERROR: Failed with object_iter creation\n");
            goto finished;
        }

        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            if (nopre == 1) {
                info_printf(lio_ifd, 0, "%s\n", &(fname[prefix_len+1]));
            } else {
                info_printf(lio_ifd, 0, "%s\n", fname);
            }

            free(fname);
        }

        lio_destroy_object_iter(tuple.lc, it);

        if (ftype < 0) {
            fprintf(stderr, "ERROR getting the next object!\n");
            return_code = EIO;
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

finished:
    tbx_stdinarray_iter_destroy(it_args);
    lio_shutdown();
    if ((it == NULL) || (ftype != 0)) return_code = EIO;
    return(return_code);
}


