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

#include <assert.h>
#include <tbx/assert_result.h>
#include "exnode.h"
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "thread_pool.h"
#include "lio.h"

#define n_inspect 10
char *inspect_opts[] = { "DUMMY", "inspect_quick_check",  "inspect_scan_check",  "inspect_full_check",
                         "inspect_quick_repair", "inspect_scan_repair", "inspect_full_repair",
                         "inspect_soft_errors",  "inspect_hard_errors", "inspect_migrate"
                       };


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, start_option, whattodo;
    int force_repair;
    char *fname = NULL;
    exnode_t *ex;
    exnode_exchange_t *exp, *exp_out;
    segment_t *seg;
    op_generic_t *gop;
    ex_off_t bufsize = 100*1024*1024;
    ex_off_t n;
    op_status_t status;
    FILE *fd;

    if (argc < 2) {
        printf("\n");
        printf("ex_inspect LIO_COMMON_OPTIONS [-bufsize n] [-force] inspect_opt file.ex3\n");
        printf("    file.ex3 - File to inspect\n");
        lio_print_options(stdout);
        n = bufsize / 1024 / 1024;
        printf("    -bufsize n    - Buffer size to use.  Defaults to " XOT "MB\n", n);
        printf("    -force        - Forces data replacement even if it would result in data loss\n");
        printf("    inspect_opt   - Inspection option.  One of the following:\n");
        for (i=1; i<n_inspect; i++) {
            printf("                 %s\n", inspect_opts[i]);
        }
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    force_repair = 0;

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-force") == 0) { //** Force repair
            i++;
            force_repair = INSPECT_FORCE_REPAIR;
        } else if (strcmp(argv[i], "-bufsize") == 0) { //** Change the buffer size
            i++;
            bufsize = atoi(argv[i])*1024*1024;
            i++;
            if (bufsize <= 0) {
                n = bufsize / 1024 / 1024;
                printf("Invalid buffer size=" XOT "\n", n);
                abort();
            }
        }

    } while (start_option < i);

    //** Get the inspect_opt;
    whattodo = -1;
    for(j=1; j<n_inspect; j++) {
        if (strcasecmp(inspect_opts[j], argv[i]) == 0) {
            whattodo = j;
            break;
        }
    }
    if (whattodo == -1) {
        printf("Invalid inspect option:  %s\n", argv[i]);
        abort();
    }
    if ((whattodo == INSPECT_QUICK_REPAIR) || (whattodo == INSPECT_SCAN_REPAIR) || (whattodo == INSPECT_FULL_REPAIR)) whattodo |= force_repair;
    i++;

    //** This is the file to inspect
    fname = argv[i];
    i++;
    if (fname == NULL) {
        printf("Missing remote file!\n");
        return(2);
    }

    //** Load it
    exp = lio_exnode_exchange_load_file(fname);
    ex = lio_exnode_create();
    lio_exnode_deserialize(ex, exp, lio_gc->ess);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    printf("whattodo=%d\n", whattodo);
    //** Execute the inspection operation
    gop = segment_inspect(seg, lio_gc->da, lio_ifd, whattodo, bufsize, NULL, lio_gc->timeout);
    tbx_log_flush();
    gop_waitany(gop);
    tbx_log_flush();
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    //** Print out the results
    whattodo = whattodo & INSPECT_COMMAND_BITS;
    switch(whattodo) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
    case (INSPECT_MIGRATE):
        if (status.op_status == OP_STATE_SUCCESS) {
            printf("Success!\n");
        } else {
            printf("Error!  status=%d error_code=%d\n", status.op_status, status.error_code);
        }
        break;
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        if (status.op_status == OP_STATE_SUCCESS) {
            printf("Success! error_cont=%d\n", status.error_code);
        } else {
            printf("Error!  status=%d error_code=%d\n", status.op_status, status.error_code);
        }
        break;
    }


    //** Store the updated exnode back to disk
    exp_out = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(ex, exp_out);

    fd = fopen(fname, "w");
    fprintf(fd, "%s", exp_out->text.text);
    fclose(fd);
    lio_exnode_exchange_destroy(exp_out);


    //** Clean up
    lio_exnode_exchange_destroy(exp);

    lio_exnode_destroy(ex);

    lio_shutdown();

    return(0);
}


