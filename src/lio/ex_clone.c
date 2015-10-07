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

#define _log_module_index 169

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"


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

//printf("argc=%d\n", argc);
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
    exp = exnode_exchange_load_file(sfname);
    ex = exnode_create();
    exnode_deserialize(ex, exp, lio_gc->ess);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


    //** Execute the clone operation
    gop = exnode_clone(lio_gc->tpc_unlimited, ex, lio_gc->da, &cex, (void *)clone_arg, mode, lio_gc->timeout);

    gop_waitany(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    if (status.op_status != OP_STATE_SUCCESS) {
        printf("Error with clone! source=%s mode=%d\n", sfname, mode);
        abort();
    }

    //** Store the updated exnode back to disk
    exp_out = exnode_exchange_create(EX_TEXT);
    exnode_serialize(cex, exp_out);
//  printf("Updated remote: %s\n", fname);
//  printf("-----------------------------------------------------\n");
//  printf("%s", exp_out->text);
//  printf("-----------------------------------------------------\n");

    fd = fopen(cfname, "w");
    fprintf(fd, "%s", exp_out->text.text);
    fclose(fd);
    exnode_exchange_destroy(exp_out);

    //** Clean up
    exnode_exchange_destroy(exp);

    exnode_destroy(ex);
    exnode_destroy(cex);

    lio_shutdown();

    return(0);
}


