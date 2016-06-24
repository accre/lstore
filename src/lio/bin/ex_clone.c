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

#define _log_module_index 169

#include <tbx/log.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <lio/ex3.h>
#include <lio/ex3_types.h>
#include <lio/lio.h>



//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    int mode;
    char *clone_arg = NULL;
    char *sfname = NULL;
    char *cfname = NULL;
    exnode_t *ex, *cex;
    exnode_exchange_t *exp, *exp_out;
    op_generic_t *gop;
    op_status_t status;
    FILE *fd;

    if (argc < 3) {
        printf("\n");
        printf("ex_clone LIO_COMMON_OPTIONS [-structure|-data] [-a clone_attr] source_file.ex3 clone_file.ex3\n");
        lio_print_options(stdout);
        printf("    -structure      - Clone the structure only [default mode]\n");
        printf("    -data           - Clone the structure and copy the data\n");
        printf("    -a clone_attr   - Segment specific attribute passed to the clone routine. Not used for all Segment types.\n");
        printf("    source_file.ex3 - File to clone\n");
        printf("    clone_file.ex3  - DEstination cloned file\n");
        return(1);
    }

    lio_init(&argc, &argv);

    mode = CLONE_STRUCTURE;

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-structure") == 0) { //** Clone the structure only
            i++;
            mode = CLONE_STRUCTURE;
        } else if (strcmp(argv[i], "-data") == 0) { //** Clone the structure and the data
            i++;
            mode = CLONE_STRUCT_AND_DATA;
        } else if (strcmp(argv[i], "-a") == 0) { //** Alternate query attribute
            i++;
            clone_arg = argv[i];
            i++;
        }

    } while (start_option < i);

    //** This is the source file
    sfname = argv[i];
    i++;
    if (sfname == NULL) {
        printf("Missing source file!\n");
        return(2);
    }

    //** This is the cloned file
    cfname = argv[i];
    i++;
    if (cfname == NULL) {
        printf("Missing cloned file!\n");
        return(2);
    }

    //** Load the source
    exp = lio_exnode_exchange_load_file(sfname);
    ex = lio_exnode_create();
    lio_exnode_deserialize(ex, exp, lio_gc->ess);

    //** Execute the clone operation
    gop = lio_exnode_clone(lio_gc->tpc_unlimited, ex, lio_gc->da, &cex, (void *)clone_arg, mode, lio_gc->timeout);

    gop_waitany(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    if (status.op_status != OP_STATE_SUCCESS) {
        printf("Error with clone! source=%s mode=%d\n", sfname, mode);
        abort();
    }

    //** Store the updated exnode back to disk
    exp_out = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(cex, exp_out);

    fd = fopen(cfname, "w");
    fprintf(fd, "%s", exp_out->text.text);
    fclose(fd);
    lio_exnode_exchange_destroy(exp_out);

    //** Clean up
    lio_exnode_exchange_destroy(exp);

    lio_exnode_destroy(ex);
    lio_exnode_destroy(cex);

    lio_shutdown();

    return(0);
}


