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

#define _log_module_index 201

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/lio.h>
#include <lio/os.h>

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
    tbx_type_malloc(*val, char, i+1);
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
    int i, rg_mode, start_option, start_index, fin, ftype, prefix_len;
    lio_path_tuple_t tuple;
    lio_os_regex_table_t *rp_single, *ro_single, *attr_regex;
    os_object_iter_t *it;
    os_attr_iter_t *ait;
    tbx_stdinarray_iter_t *it_args;
    char *bstate, *fname, *path;
    char *key[MAX_SET];
    char *val[MAX_SET];
    int v_size[MAX_SET];
    int n_keys, n_keys_al;
    int return_code;
    int max_attr = 1024*1024;
    char *new_obj_fmt = "object=%s\\n";
    char *end_obj_fmt = "\\n";
    char *attr_fmt = "\t%s=%s\\n";
    char *attr_sep = "";

    int recurse_depth = 0;
    int obj_types = OS_OBJECT_FILE_FLAG;
    return_code = 0;

    // Initialize to garbage since Alan didn't at all
    n_keys = INT_MIN;

    if (argc < 2) {
        printf("\n");
        printf("lio_getattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-vmax max_val] PRINT_OPTIONS [-ga attr_glob | -ra attr_regex | -al key1,key2,...keyN] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    PRINT_OPTIONS: [-new_obj new_obj_fmt] [-end_obj end_obj_fmt] [-attr_fmt attr_fmt] [-attr_sep attr_sep]\n");
        printf("       -list                 - Prints object and attibute names but not their values\n");
        printf("       -list1                - Prints 1 line per file only listing the attributes. Not their values\n");
        printf("       -single               - Prints 1 line per file with all attributes space separated\n");
        printf("       -new_obj new_obj_fmt  - New object C format string.  Default is \"%s\"\n", new_obj_fmt);
        printf("       -end_obj end_obj_fmt  - End object C format string.  Default is \"%s\"\n", end_obj_fmt);
        printf("       -attr_fmt attr_fmt    - Attribute C format string.  Default is \"%s\"\n", attr_fmt);
        printf("       -attr_sep attr_sep    - Attribute separator C format string.  Default is \"%s\"\n", attr_sep);
        printf("\n");
        printf("       The format strings are string together according to the following code snippet:\n");
        printf("           printf(new_obj_fmt, object_name);\n");
        printf("           printf(attr_fmt, key[0], val[0]);\n");
        printf("           for (i=1; i<nattr; i++) {\n");
        printf("             printf(attr_sep);\n");
        printf("             printf(attr_fmt, key[i], val[i]);\n");
        printf("           }\n");
        printf("           printf(end_obj_fmt, object_name);\n");
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        printf("    -vmax max_val      - Max size of attribute value allowed.  Default value is %d\n", max_attr);
        printf("    -ga attr_glob      - Attribute glob string\n");
        printf("    -ra attr_regex     - Attribute regex string\n");
        printf("    -al key1,key2,...  - Comma separated list of keys to retrieve\n");
        return(1);
    }

    lio_init(&argc, &argv);

    max_attr = lio_gc->max_attr;

    //*** Parse the args
    rp_single = ro_single = attr_regex = NULL;
    n_keys_al = 0;

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
        } else if (strcmp(argv[i], "-vmax") == 0) {  //** MAx attribute size
            i++;
            max_attr = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-new_obj") == 0) {  //**  New object format
            i++;
            new_obj_fmt = argv[i];
            i++;
        } else if (strcmp(argv[i], "-end_obj") == 0) {  //**  End object format
            i++;
            end_obj_fmt = argv[i];
            i++;
        } else if (strcmp(argv[i], "-attr_fmt") == 0) {  //**  Attribute format
            i++;
            attr_fmt = argv[i];
            i++;
        } else if (strcmp(argv[i], "-attr_sep") == 0) {  //**  Attribute separator
            i++;
            attr_sep = argv[i];
            i++;
        } else if (strcmp(argv[i], "-single") == 0) {  //**  Single line/object
            i++;
            new_obj_fmt = "%s";
            attr_fmt = " %s=%s";
            attr_sep = "";
            end_obj_fmt = "\\n";
        } else if (strcmp(argv[i], "-list1") == 0) {  //**  List all the attributes on 1 line
            i++;
            new_obj_fmt = "%s";
            attr_fmt = " %s";
            attr_sep = "";
            end_obj_fmt = "\\n";
        } else if (strcmp(argv[i], "-list") == 0) {  //**  List all the attributes
            i++;
            attr_fmt = "\t%s\\n";
        } else if (strcmp(argv[i], "-ga") == 0) {  //**  Attribute glob
            i++;
            attr_regex = lio_os_path_glob2regex(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-ra") == 0) {  //**  Attribute regex
            i++;
            attr_regex = lio_os_regex2table(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-al") == 0) {  //**  Attribute list
            i++;
            key[0]=tbx_stk_string_token(argv[i], ",", &bstate, &fin);
            n_keys = 0;
            do {
                n_keys++;
                key[n_keys] = tbx_stk_string_token(NULL, ",", &bstate, &fin);
            } while (fin == 0);
            n_keys_al = n_keys;
            i++;
            //for(err=0; err<n_keys; err++) printf("key[%d]=%s\n", err, key[err]);
        }
    } while ((start_option - i < 0) && (i<argc));
    start_index = i;

    //** Convert all the format strings
    new_obj_fmt = tbx_stk_argv2format(new_obj_fmt);
    end_obj_fmt = tbx_stk_argv2format(end_obj_fmt);
    attr_fmt = tbx_stk_argv2format(attr_fmt);
    attr_sep = tbx_stk_argv2format(attr_sep);

    //** MAke the path if needed
    if (rg_mode == 0) {
        if (i >= argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_index--;  //** The 1st entry will be the rp created in lio_parse_path_options
    }

    if ((n_keys_al == 0) && (attr_regex == NULL)) { //** No attributes specified so default to everything
        attr_regex = lio_os_path_glob2regex("*");
    }

    it_args = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    ftype = 0; //** Make old GCC happy
    while (1) {
        if (rg_mode == 0) {
            path = tbx_stdinarray_iter_next(it_args);
            if (!path) break;

            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
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

        //** Make the iterator.  It depends on if we have a regex or a list
        if (n_keys_al > 0) {  //** Using a fixed list of keys
            n_keys = n_keys_al;
            for (i=0; i<n_keys; i++) {
                v_size[i] = -max_attr;
                val[i] = NULL;
            }
            it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, recurse_depth, key, (void **)val, v_size, n_keys);
        } else {   //** Got a regex for attribute selection
            v_size[0] = -max_attr;
            it = lio_create_object_iter(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, attr_regex, recurse_depth, &ait, v_size[0]);
        }

        if (it == NULL) {
            fprintf(stderr, "ERROR creating iterator!\n");
            return_code = EIO;
        }

        log_printf(15, "before main loop\n");
        //**Now iterate through the objects
        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            if (attr_regex != NULL) {  //** Got an attr regex so load all the attr
                n_keys = 0;
                v_size[n_keys] = -1*max_attr;
                val[n_keys] = NULL;
                log_printf(15, "v_size = %d\n", v_size[n_keys]);
                while (lio_next_attr(tuple.lc, ait, &(key[n_keys]), (void **)&(val[n_keys]), &(v_size[n_keys])) == 0) {
                    n_keys++;
                    v_size[n_keys] = -1*max_attr;
                    val[n_keys] = NULL;
                }
            }

            //** Print the record
            info_printf(lio_ifd, 0, new_obj_fmt, fname);
            if (n_keys > 0) {
                info_printf(lio_ifd, 0, attr_fmt, key[0], val[0]);
                for (i=1; i<n_keys; i++) {
                    info_printf(lio_ifd, 0, "%s", attr_sep);
                    info_printf(lio_ifd, 0, attr_fmt, key[i], val[i]);
                }
            }
            info_printf(lio_ifd, 0, end_obj_fmt, fname);

            //** Free the space
            free(fname);
            for (i=0; i<n_keys; i++) {
                if (val[i] != NULL) free(val[i]);
                if (attr_regex != NULL) free(key[i]);
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

    if (ftype < 0) {
        fprintf(stderr, "ERROR getting the next object!\n");
        return_code = EIO;
    }

    log_printf(15, "after main loop\n");

    if (attr_regex != NULL) {
        lio_os_regex_table_destroy(attr_regex);
    }

    free(new_obj_fmt);
    free(end_obj_fmt);
    free(attr_fmt);
    free(attr_sep);
    tbx_stdinarray_iter_destroy(it_args);

    lio_shutdown();

    return(return_code);
}


