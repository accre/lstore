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

#define _log_module_index 206

#include <gop/gop.h>
#include <stdio.h>
#include <tbx/log.h>

#include <lio/ex3.h>
#include <lio/ex3_types.h>
#include <lio/lio.h>
#include <lio/os.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 10*1024;
    char buffer[bufsize];
    int err, ftype, start_index, used;
    char *ex_data;
    exnode_t *ex;
    exnode_exchange_t *exp;
    segment_t *seg;
    int v_size;
    lio_path_tuple_t tuple;

    if (argc < 2) {
        printf("\n");
        printf("lio_signature LIO_COMMON_OPTIONS file\n");
        lio_print_options(stdout);
        printf("    file          - File to examine\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //** This is the 1st dir to remove
    start_index = 1;
    if (argv[start_index] == NULL) {
        info_printf(lio_ifd, 0, "Missing Source!\n");
        return(2);
    }

    //** Get the source
    tuple = lio_path_resolve(lio_gc->auto_translate, argv[start_index]);

    //** Check if it exists
    ftype = lio_exists(tuple.lc, tuple.creds, tuple.path);

    if ((ftype & OS_OBJECT_FILE) == 0) { //** Doesn't exist or is a dir
        info_printf(lio_ifd, 1, "ERROR source file(%s) doesn't exist or is a dir ftype=%d!\n", tuple.path, ftype);
        goto finished;
    }

    //** Get the exnode
    v_size = -tuple.lc->max_attr;
    err = lio_getattr(tuple.lc, tuple.creds, tuple.path, NULL, "system.exnode", (void **)&ex_data, &v_size);
    if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "Failed retrieving exnode! err=%d path=%s\n", err, tuple.path);
        goto finished;
    }

    //** Load it
    exp = lio_exnode_exchange_text_parse(ex_data);
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, tuple.lc->ess) != 0) {
        info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
        goto finished;
    }

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
        goto finished;
    }


    used = 0;
    segment_signature(seg, buffer, &used, bufsize);

    info_printf(lio_ifd, 0, "%s", buffer);

    lio_exnode_destroy(ex);
    lio_exnode_exchange_destroy(exp);

finished:
    lio_path_release(&tuple);

    lio_shutdown();

    return(0);
}


