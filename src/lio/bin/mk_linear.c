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

#define _log_module_index 173

#include <gop/gop.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/rs.h>
#include <lio/segment.h>
#include <lio/service_manager.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    int n_rid;
    char *query_text = NULL;
    rs_query_t *rq;
    ex_off_t block_size, total_size;
    lio_exnode_t *ex;
    lio_segment_create_fn_t *screate;
    char *fname_out = NULL;
    lio_exnode_exchange_t *exp;
    lio_segment_t *seg = NULL;
    gop_op_generic_t *gop;

    if (argc < 5) {
        printf("\n");
        printf("mk_linear LIO_COMMON_OPTIONS -q rs_query_string n_rid block_size total_size file.ex3\n");
        lio_print_options(stdout);
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-q") == 0) { //** Load the query
            i++;
            query_text = argv[i];
            i++;
        }

    } while (start_option - i < 0);

    //** Load the fixed options
    n_rid = atoi(argv[i]);
    i++;
    block_size = atoi(argv[i]);
    i++;
    total_size = atoi(argv[i]);
    i++;
    fname_out = argv[i];
    i++;

    //** Do some simple sanity checks
    //** Make sure we loaded a simple res service
    if (fname_out == NULL) {
        printf("Missing output filename!\n");
        return(2);
    }
    //** Create an empty linear segment
    screate = lio_lookup_service(lio_gc->ess, SEG_SM_CREATE, SEGMENT_TYPE_LINEAR);
    seg = (*screate)(lio_gc->ess);

    //** Parse the query
    rq = rs_query_parse(lio_gc->rs, query_text);
    if (rq == NULL) {
        printf("Error parsing RS query: %s\n", query_text);
        printf("Exiting!\n");
        exit(1);
    }

    //** Make the actual segment
    gop = lio_segment_linear_make(seg, NULL, rq, n_rid, block_size, total_size, lio_gc->timeout);
    i = gop_waitall(gop);
    if (i != 0) {
        printf("ERROR making segment! nerr=%d\n", i);
        return(-1);
    }
    gop_free(gop, OP_DESTROY);

    //** Make an empty exnode
    ex = lio_exnode_create();

    //** and insert it
    lio_view_insert(ex, seg);


    //** Print it
    exp = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(ex, exp);
    printf("%s", exp->text.text);

    //** and Save if back to disk
    FILE *fd = fopen(fname_out, "w");
    fprintf(fd, "%s", exp->text.text);
    fclose(fd);
    lio_exnode_exchange_destroy(exp);


    //** Clean up
    lio_exnode_destroy(ex);

    rs_query_destroy(lio_gc->rs, rq);

    lio_shutdown();

    return(0);
}


