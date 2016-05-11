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

#include <assert.h>
#include <tbx/assert_result.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <tbx/iniparse.h>
#include "lio.h"
#include <tbx/type_malloc.h>
#include "object_service_abstract.h"


typedef struct {
    lio_path_tuple_t src_tuple;
} ls_file_t;

typedef struct {
    lio_path_tuple_t src_tuple;
    os_regex_table_t *regex;
    int recurse_depth;
    int dest_type;
} ls_path_t;


//**********************************************************************************
// Performs walk and prints the output
//**********************************************************************************
void run_ls(char *path, char *regex_path, char *regex_object, int obj_types, int recurse_depth)
{
    unified_object_iter_t *it;
    os_regex_table_t *rp, *ro;
    lio_path_tuple_t tuple;
    int prefix_len, ftype;
    char *fname = NULL;

    rp = ro = NULL;

    tuple = lio_path_resolve(lio_gc->auto_translate, path);
    if (path != NULL) {
        lio_path_wildcard_auto_append(&tuple);
        rp = os_path_glob2regex(tuple.path);
    } else {
        rp = os_regex2table(regex_path);
        ro = os_regex2table(regex_object);
    }

    it = unified_create_object_iter(tuple, rp, ro, obj_types, recurse_depth);
    if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation! path=%s regex_path=%s regex_object=%s\n", path, regex_path, regex_object);
        exit(3);
    }

    while ((ftype = unified_next_object(it, &fname, &prefix_len)) > 0) {
        char *ftype_string = NULL;
        if ((ftype & OS_OBJECT_VIRTUAL) == 32) {
            ftype_string = "VIRTUAL";
        } else if ((ftype & OS_OBJECT_BROKEN_LINK) == 16) {
            ftype_string = "BROKEN_LINK";
        } else if ((ftype & OS_OBJECT_HARDLINK) == 8) {
            ftype_string = "HARDLINK";
        } else if ((ftype & OS_OBJECT_SYMLINK) == 4) {
            ftype_string = "SYMLINK";
        } else if ((ftype & OS_OBJECT_DIR) == 2) {
            ftype_string = "DIR";
        } else if ((ftype & OS_OBJECT_FILE) == 1) {
            ftype_string = "FILE";
        } else {
            ftype_string = "UNKNOWN";
        }
        printf("%-13s\t%s\n", ftype_string, fname);
        free(fname);
    }

    unified_destroy_object_iter(it);

    if (rp != NULL) os_regex_table_destroy(rp);
    if (ro != NULL) os_regex_table_destroy(ro);

    lio_path_release(&tuple);
}


void process_tag_file(char *tag_file, char *tag_name)
{

    char *name = NULL;
    char *path = NULL;
    char *regex_path = NULL;
    char *regex_object = NULL;
    int recurse_depth, obj_types;
    tbx_inip_file_t *ini_fd;
    tbx_inip_group_t *ini_g;
    tbx_inip_element_t *ele;
    char *key, *value;

    /*** Check for tag file existence and read permission ***/
    if (((access (tag_file, F_OK)) == -1) || ((access(tag_file, R_OK)) == -1)) {
        printf("%s does not exist or you do not have read permission!\n", tag_file);
        exit(1);
    } else {
        /*** process tag file ***/
        ini_fd = tbx_inip_file_read(tag_file); assert(ini_fd);
        ini_g = tbx_inip_group_first(ini_fd);
        obj_types = OS_OBJECT_ANY;
        while (ini_g != NULL) {
            if (strcmp(tbx_inip_group_get(ini_g), "TAG") == 0) {
                ele = tbx_inip_ele_first(ini_g);
                while (ele != NULL) {
                    key = tbx_inip_ele_key_get(ele);
                    value = tbx_inip_ele_value_get(ele);
                    if (strcmp(key, "name") == 0) {
                        name = value;
                    } else if (strcmp(key, "path") == 0) {
                        path = value;
                    } else if (strcmp(key, "regex_path") == 0) {
                        regex_path = value;
                    } else if (strcmp(key, "regex_object") == 0) {
                        regex_object = value;
                    } else if (strcmp(key, "recurse_depth") == 0) {
                        recurse_depth = atoi(value);
                    } else if (strcmp(key, "object_types") == 0) {
                        obj_types = atoi(value);
                    }
                    ele = tbx_inip_ele_next(ele);
                }
                if ((tag_name == NULL) || (strcmp(tag_name, name) == 0)) {
                    run_ls(path, regex_path, regex_object, obj_types, recurse_depth);
                }
            }
            ini_g = tbx_inip_group_next(ini_g);
        }
        /*** proper cleanup ***/
        tbx_inip_destroy(ini_fd);
    }
}


void print_usage()
{
    printf("\nUsage: arc_tag_ls [-t tag file] [-n tag name]\n");
    printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
    printf("\t\n\ttag name to list (if none, all tags will be printed)");
    printf("\nExamples to come soon\n");
    exit(0);
}


int main(int argc, char **argv)
{

    int i = 1, start_option = 0;
    char *tag_file = NULL;
    char *tag_name = NULL;

    lio_init(&argc, &argv);

    /*** Parse the args ***/
    if (argc > 3) {
        print_usage();
    } else if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-h") == 0) {
                print_usage();
            } else if (strcmp(argv[i], "-t") == 0) {
                i++;
                tag_file = argv[i];
                i++;
            } else if (strcmp(argv[i], "-n") == 0) {
                i++;
                tag_name = argv[i];
                i++;
            }
        } while ((start_option < i) && (i < argc));
    }
    /*** If no tag file was specified, set to the default ***/
    if (tag_file == NULL) {
        char *homedir = getenv("HOME");
        tag_file = strcat(homedir, "/.arc_tag_file.txt");
    }
    process_tag_file(tag_file, tag_name);

    lio_shutdown();

    return(0);
}
