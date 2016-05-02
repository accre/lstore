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
#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "iniparse.h"
#include "lio.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "apr_time.h"
#include "archive.h"


int restore_path(char *path, char *tape_id)
{
    int res = EXIT_SUCCESS;
    //should not be hardcoded but this is temporary
    char *script = "/tibs/bin/restore.sh";
    char server[256];
    char *backup_path;

    strcpy(server, tape_id);

    strtok_r(server, " ", &backup_path);

    int len = strlen(script) + strlen(server) + strlen(backup_path) + 10;
    char cmd[len];
    sprintf(cmd, "%s %s %s", script, server, backup_path);
    res = system(cmd);
    int exit_status = WEXITSTATUS(res);
    if (exit_status == 10) {
        printf("ERROR: Invalid arguments passed to archive service!\n");
    } else if (exit_status == 11) {
        printf("ERROR: Failed to retrieve data!\n");
    } else if (exit_status == 1) {
        printf("ERROR: Generic failure running tape restore\n");
    }
    return(res);
}

int run_lstore_copy(char *spath, char *dpath)
{
    int res = EXIT_SUCCESS;
    lio_path_tuple_t stuple;
    os_regex_table_t *rp, *ro;
    lio_cp_path_t *flist;
    int dtype, buffer_size, max_spawn;
    op_status_t status;
    int recurse_depth;
    int obj_types = OS_OBJECT_ANY;

    //TODO add this as an CLI option
    //** Store the buffer size
    buffer_size = 1024 * 1024 * 20;


    type_malloc_clear(flist, lio_cp_path_t, 1);
    max_spawn = lio_parallel_task_count;

    // target in lio to download
    char *lio_src = concat("@:", spath);
    stuple = lio_path_resolve(lio_gc->auto_translate, lio_src);

    if (spath != NULL) {
        // target to download to on the local system
        flist[0].dest_tuple = lio_path_resolve(lio_gc->auto_translate, dpath);
    } else {
        // TODO: deal with regex if use
        // Get the dest filetype/exists
        printf("ERROR:  Should not be here!\n");
    }
    if (flist[0].dest_tuple.is_lio == 1) {
        dtype = lioc_exists(flist[0].dest_tuple.lc, flist[0].dest_tuple.creds, flist[0].dest_tuple.path);
    } else {

        dtype = os_local_filetype(flist[0].dest_tuple.path);
    }

    flist[0].src_tuple.creds = lio_gc->creds;
    //rp = os_regex2table(regex_path);
    //ro = os_regex2table(regex_object);
    flist[0].src_tuple = stuple;
    flist[0].dest_type = dtype;
    flist[0].path_regex = os_path_glob2regex(flist[0].src_tuple.path);
    flist[0].recurse_depth = recurse_depth;
    flist[0].obj_types = obj_types;
    flist[0].max_spawn = max_spawn;
    flist[0].bufsize = buffer_size;

    status = lio_cp_path_fn(&(flist[0]), 0);
    if (status.op_status != OP_STATE_SUCCESS) {
        printf("ERROR: with copy src=%s  dest=%s\n", flist[0].src_tuple.path, flist[0].dest_tuple.path);
        res = 5;
        goto finally;
    }

finally:
    lio_path_release(&stuple);
    lio_path_release(&(flist[0].dest_tuple));
    os_regex_table_destroy(flist[0].path_regex);
    free(flist);
    return(res);
}

void process_restore(char *spath, char *dpath)
{
    int res = EXIT_SUCCESS;
    char *tape_id, *dir;
    int attr_size = -lio_gc->max_attr;
    tape_id = NULL;

    //check tape attribute
    lioc_get_attr(lio_gc, lio_gc->creds, spath, NULL, ARCHIVE_TAPE_ATTRIBUTE, (void**) &tape_id, &attr_size);
    if (tape_id == NULL) {
        printf("ERROR: Could not find tape ID attribute for %s\n", spath);
    } else {
        //run restore
        res = restore_path(spath, tape_id);
    }
    // copy files from L-Store
    run_lstore_copy(spath, dpath);
}

void print_usage()
{
    printf("\nUsage: arc_restore -s SOURCE_PATH -d DESTINATION_PATH\n");
    printf("\t-s\tL-Store path to restore\n");
    printf("\t-l\tLocal path to restore files to");
    printf("\nExamples to come soon\n");
    exit(0);
}

/*
 *
 */
int main(int argc, char** argv)
{
    int i = 1, start_option = 0;
    char *spath = NULL;
    char *dpath = NULL;

    lio_init(&argc, &argv);

    /*** Parse the args ***/
    if (argc > 5) {
        print_usage();
    } else if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-h") == 0) {
                print_usage();
            } else if (strcmp(argv[i], "-s") == 0) {
                i++;
                spath = argv[i];
                i++;
            } else if (strcmp(argv[i], "-l") == 0) {
                i++;
                dpath = argv[i];
                i++;
            }
        } while ((start_option < i) && (i < argc));
    }
    process_restore(spath, dpath);
    lio_shutdown();

    return(EXIT_SUCCESS);
}

