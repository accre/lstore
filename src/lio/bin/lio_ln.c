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

#define _log_module_index 192

#include <gop/gop.h>
#include <gop/mq.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/list.h>
#include <tbx/log.h>

#include <lio/blacklist.h>
#include <lio/cache.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>

char *exnode_data = NULL;


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int ftype, err, symlink, i, force, start_option;
    char *src_fname, *dest_fname;
    char *dir, *file;
    lio_path_tuple_t stuple, dtuple;
    char fullname[OS_PATH_MAX];

    if (argc < 3) {
        printf("\n");
        printf("lio_ln LIO_COMMON_OPTIONS [-s] [-f] source_file linked_dest_file\n");
        lio_print_options(stdout);
        printf("    -s               - Make symbolic links instead of hard links\n");
        printf("    -f               - Force the link creation even if source doesn't exist. Only used for symlinks\n");

        return(1);
    }

    lio_init(&argc, &argv);

    if (argc < 3) {
        printf("\n");
        printf("lio_ln LIO_COMMON_OPTIONS source_file linked_dest_file\n");
        lio_print_options(stdout);
        printf("    -s               - Make symbolic links instead of hard links\n");
        printf("    -f               - Force the link creation even if source doesn't exist. Only used for symlinks\n");
        return(1);
    }

    symlink = 0;
    force = 0;
    i = 1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-s") == 0) {
            symlink = 1;
            i++;
        } else if (strcmp(argv[i], "-f") == 0) {
            force = 1;
            i++;
        }
    } while ((start_option < i) && (i<argc));

    src_fname = argv[i];
    i++;
    stuple = lio_path_resolve(lio_gc->auto_translate, src_fname);
    if (stuple.is_lio < 0) {
        fprintf(stderr, "Unable to parse path: %s\n", src_fname);
        return(EINVAL);
    }

    dest_fname = argv[i];
    i++;
    dtuple = lio_path_resolve(lio_gc->auto_translate, dest_fname);
    if (dtuple.is_lio < 0) {
        fprintf(stderr, "Unable to parse path: %s\n", dest_fname);
        return(EINVAL);
    }

    //** Make sure we're linking in the same system
    if (strcmp(stuple.lc->section_name, dtuple.lc->section_name) != 0) {
        printf("Source and destination objects must exist in the same system!\n");
        printf("Source: %s   Dest: %s\n", stuple.lc->section_name, dtuple.lc->section_name);
        return(1);
    }

    if (symlink == 0) force = 0;

    if (stuple.path[0] == '/') { //** Absolute path
        ftype = lio_exists(stuple.lc, stuple.creds, stuple.path);
    } else {  //** Relative path
        lio_os_path_split(dtuple.path, &dir, &file);
        snprintf(fullname, OS_PATH_MAX, "%s/%s", dir, stuple.path);
        ftype = lio_exists(stuple.lc, stuple.creds, fullname);
        free(dir);
        free(file);
    }

    //** Check on the source file
    if (force == 0) {
        if (ftype == 0) { //** The file doesn't exists
            printf("ERROR source file doesn't exist: %s\n", stuple.path);
            err = 1;
            goto finished;
        }
    }

    if (((ftype & OS_OBJECT_DIR_FLAG)>0) && (symlink == 0))  { //** Can only symlink a file
        printf("ERROR Can't hard link directories!  Source: %s\n", stuple.path);
        err = 1;
        goto finished;
    }

    //** Check on the dest file
    ftype = lio_exists(dtuple.lc, dtuple.creds, dtuple.path);
    if (ftype != 0) { //** The file doesn't exists
        printf("ERROR destination file exists: %s\n", dtuple.path);
        err = 1;
        goto finished;
    }

    //** Now create the link
    err = gop_sync_exec(lio_link_gop(dtuple.lc, dtuple.creds, symlink, stuple.path, dtuple.path, NULL));
    if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "ERROR linking file!\n");
        err = 1;
        goto finished;
    }

    err = 0;

finished:
    lio_path_release(&stuple);
    lio_path_release(&dtuple);

    lio_shutdown();

    return(err);
}
