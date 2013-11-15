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

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "iniparse.h"
#include "lio.h"
#include "archive.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "apr_time.h"




//**********************************************************************************
// Prepares the L-Store destination
//**********************************************************************************

char* prepare_lstore_destination(char *name) {
    apr_time_t now = apr_time_sec(apr_time_now());
    char *secs[80];
    char *path = NULL;
    char* arc_path = NULL;
    char *root = "/archive/";

    check_path(root);

    path = concat("/archive/", name);
    check_path(path);

    sprintf(secs, "" TT "", now);
    arc_path = path_concat(path, secs);
    check_path(arc_path);

    //clean up
    free(path);
    return arc_path;
}

// Check if path exists...if not, creates it

void check_path(char *path) {
    if (lioc_exists(lio_gc, lio_gc->creds, path) == 0) {
        if (gop_sync_exec(lioc_create_object(lio_gc, lio_gc->creds, path, OS_OBJECT_DIR, NULL, NULL)) != OP_STATE_SUCCESS) {
            printf("ERROR: Failed to create %s!\n", path);
            exit(4);
        }
    }
}


//**********************************************************************************
// Copies the local data into the L-Store system for staging
//**********************************************************************************

int run_lstore_copy(char *dest, char *path, char *regex_path, char *regex_object, int obj_types, int recurse_depth) {
    int res = EXIT_SUCCESS;
    lio_path_tuple_t dtuple;
    os_regex_table_t *rp, *ro;
    lio_cp_path_t *flist;
    int dtype, buffer_size, max_spawn;
    op_status_t status;


    //TODO add this as an CLI option 
    //** Store the buffer size
    buffer_size = 1024 * 1024 * 20;
    char *lio_dest = concat("@:", dest);
    dtuple = lio_path_resolve(lio_gc->auto_translate, lio_dest);

    //** Get the dest filetype/exists
    if (dtuple.is_lio == 1) {
        dtype = lioc_exists(dtuple.lc, dtuple.creds, dtuple.path);
    } else {
        dtype = os_local_filetype(dtuple.path);
    }

    type_malloc_clear(flist, lio_cp_path_t, 1);
    max_spawn = lio_parallel_task_count;
    if (path != NULL) {
        flist[0].src_tuple = lio_path_resolve(lio_gc->auto_translate, path);
    } else {
        // TODO: deal with regex if use
    }
    flist[0].src_tuple.creds = lio_gc->creds;
    //rp = os_regex2table(regex_path);
    //ro = os_regex2table(regex_object);
    flist[0].dest_tuple = dtuple;
    flist[0].dest_type = dtype;
    flist[0].path_regex = os_path_glob2regex(flist[0].src_tuple.path);
    flist[0].recurse_depth = recurse_depth;
    flist[0].obj_types = obj_types;
    flist[0].max_spawn = max_spawn;
    flist[0].bufsize = buffer_size;

    status = lio_cp_path_fn(&(flist[0]), 0);
    if (status.op_status != OP_STATE_SUCCESS) {
        printf("ERROR: with copy src=%s  dest=%s\n", flist[0].src_tuple.path, dtuple.path);
        res = 5;
        goto finally;
    }

finally:
    lio_path_release(&dtuple);
    lio_path_release(&(flist[0].src_tuple));
    os_regex_table_destroy(flist[0].path_regex);
    free(flist);
    return (res);
}


//**********************************************************************************
// Copies the data from L-Store to tape.
//**********************************************************************************

int run_tape_copy(char *server, char *dest) {

    /* this is a temporary hack and eventually this will be a direct integration with 
     * TiBS once they have completed their API.
     */

    //should not be hardcoded but this is temporary
    char *script = "/tibs/bin/archive.sh";
    int len = strlen(script) + strlen(server) + strlen(dest) + 10;
    char cmd[len];
    sprintf(cmd, "%s %s %s", script, server, dest);
    int res = system(cmd);
    int exit_status = WEXITSTATUS(res);
    if (exit_status == 10) {
        printf("ERROR: Invalid arguments passed to archive service!\n");
    } else if (exit_status == 11) {
        printf("ERROR: Failed to archive data!\n");
    } else if (exit_status == 1) {
        printf("ERROR: Generic failure running tape copy\n");
    }
    return (exit_status);
}


//**********************************************************************************
// Sets the user.tapeid attribute on the files archived
//**********************************************************************************

int set_tapeid_attr(char *dest, char *server) {
    int err, res = EXIT_SUCCESS;
    lio_path_tuple_t dtuple;
    os_object_iter_t *it;
    os_regex_table_t *rp_single, *ro_single = NULL;
    int len = strlen(server) + strlen(dest) + 10;
    char attr[len];
    sprintf(attr, "%s %s", server, dest);
    err = lioc_set_attr(lio_gc, lio_gc->creds, dest, NULL, ARCHIVE_TAPE_ATTRIBUTE, (char *) attr, strlen(attr));
    if (err != OP_STATE_SUCCESS) {
        res = 1;
        printf("ERROR: Failed to set tape id attribute on parent!:  %s\n", dest);
    } else {
        char *lio_dest = concat("@:", dest);
        dtuple = lio_path_resolve(lio_gc->auto_translate, lio_dest);
        rp_single = os_path_glob2regex(dtuple.path);
        it = os_create_object_iter(dtuple.lc->os, dtuple.creds, rp_single, ro_single, OS_OBJECT_ANY, NULL, 10000, NULL, 0);
        if (it == NULL) {
            res = 1;
            printf("ERROR: Failed to create iterator!\n");
        } else {
            int ftype, prefix_len;
            char *fname;
            os_fd_t *fd;

            while ((ftype = os_next_object(dtuple.lc->os, it, &fname, &prefix_len)) > 0) {
                // Open the file
                err = gop_sync_exec(os_open_object(dtuple.lc->os, dtuple.creds, fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, 30));
                if (err != OP_STATE_SUCCESS) {
                    printf("ERROR: Failed to open file: %s.  Skipping.\n", fname);
                } else {
                    // Create the symlink
                    if (strcmp(dest, fname) != 0) {
                        //printf("Creating symlink,  src: %s  attr: %s  val: %s\n", dest, ARCHIVE_TAPE_ATTRIBUTE, fname);
                        err = gop_sync_exec(os_symlink_attr(dtuple.lc->os, dtuple.creds, dest, ARCHIVE_TAPE_ATTRIBUTE, fd, ARCHIVE_TAPE_ATTRIBUTE));
                        if (err != OP_STATE_SUCCESS) {
                            printf("ERROR: Failed to link file: %s\n", fname);
                        } else {
                            // if object is a file, truncate it                         
                            if ((ftype & OS_OBJECT_FILE) == 1) {
                                lio_path_tuple_t tuple;
                                tuple = lio_path_resolve(lio_gc->auto_translate, fname);
                                err = gop_sync_exec(lioc_truncate(&tuple, 0));
                                if (err != OP_STATE_SUCCESS) {
                                    printf("Failed to truncate %s\n", fname);
                                }
                            }
                        }
                    }
                    // Close the file
                    err = gop_sync_exec(os_close_object(dtuple.lc->os, fd));
                    if (err != OP_STATE_SUCCESS) {
                        printf("ERROR: Failed to close file: %s\n", fname);
                    }
                }

                free(fname);
            }
            os_destroy_object_iter(dtuple.lc->os, it);
        }
        lio_path_release(&dtuple);
        if (rp_single != NULL) {
            os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
    }
    return (res);
}

//**********************************************************************************
// Processes the ini file and archives the appropriate data
//**********************************************************************************

int process_tag_file(char *tag_file, char *tag_name) {
    int res = EXIT_SUCCESS;
    char *name = NULL;
    char *path = NULL;
    char *regex_path = NULL;
    char *regex_object = NULL;
    char *dest = NULL;
    int recurse_depth, obj_types;
    inip_file_t *ini_fd;
    inip_group_t *ini_g;
    inip_element_t *ele;
    char *key, *value, *arc_server;

    /*** Check for tag file existence and read permission ***/
    if (((access(tag_file, F_OK)) == -1) || ((access(tag_file, R_OK)) == -1)) {
        printf("%s does not exist or you do not have read permission!\n", tag_file);
        exit(1);
    } else {
        /*** process tag file ***/
        assert(ini_fd = inip_read(tag_file));
        ini_g = inip_first_group(ini_fd);
        obj_types = OS_OBJECT_ANY;
        while (ini_g != NULL) {
            if (strcmp(inip_get_group(ini_g), "TAG") == 0) {
                ele = inip_first_element(ini_g);
                while (ele != NULL) {
                    key = inip_get_element_key(ele);
                    value = inip_get_element_value(ele);
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
                    } else if (strcmp(key, "arc_server") == 0) {
                        arc_server = value;
                    }
                    ele = inip_next_element(ele);
                }
                if ((tag_name == NULL) || (strcmp(tag_name, name) == 0)) {
                    // create destination
                    dest = prepare_lstore_destination(name);
                    // copy the data into L-Store
                    res = run_lstore_copy(dest, path, regex_path, regex_object, obj_types, recurse_depth);
                    if (res != 0) {
                        printf("ERROR: Data ingestion has failed...skipping %s\n", dest);
                        continue;
                    }
                    // Write the data to tape via TiBS
                    res = run_tape_copy(arc_server, dest);
                    if (res != 0) {
                        printf("ERROR: %d: Writing to tape has failed for %s\n", res, dest);
                    } else {
                        // set the tape id attribute for restores
                        res = set_tapeid_attr(dest, arc_server);
                        if (res != 0) {
                            printf("Failed to set tape id attribute for %s\n", dest);
                        }
                    }
                }
            }
            ini_g = inip_next_group(ini_g);
        }
        /*** proper cleanup ***/
        inip_destroy(ini_fd);
    }
}

void print_usage() {
    printf("\nUsage: arc_create [-t tag file] [-n tag name]\n");
    printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
    printf("\t\n\ttag name to archive (if none, all tags will be archived)");
    printf("\nExamples to come soon\n");
    exit(0);
}

int main(int argc, char **argv) {

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
        tag_file = concat(homedir, "/.arc_tag_file.txt");
    }
    process_tag_file(tag_file, tag_name);

    lio_shutdown();

    return (EXIT_SUCCESS);
}
