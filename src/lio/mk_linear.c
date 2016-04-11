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

#define _log_module_index 173

#include "exnode.h"
#include "log.h"
#include "lio.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    int n_rid;
    char *query_text;
    rs_query_t *rq;
    ex_off_t block_size, total_size;
    exnode_t *ex;
    segment_create_t *screate;
    char *fname_out = NULL;
    exnode_exchange_t *exp;
    segment_t *seg = NULL;
    op_generic_t *gop;

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

    } while (start_option < i);

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
    screate = lookup_service(lio_gc->ess, SEG_SM_CREATE, SEGMENT_TYPE_LINEAR);
    seg = (*screate)(lio_gc->ess);

    //** Parse the query
    rq = rs_query_parse(lio_gc->rs, query_text);
//  rs_query_add(rs, &rq, RSQ_BASE_OP_AND, "lun", RSQ_BASE_KV_EXACT, "", RSQ_BASE_KV_ANY);
    if (rq == NULL) {
        printf("Error parsing RS query: %s\n", query_text);
        printf("Exiting!\n");
        exit(1);
    }

    //** Make the actual segment
    gop = segment_linear_make(seg, NULL, rq, n_rid, block_size, total_size, lio_gc->timeout);
    i = gop_waitall(gop);
    if (i != 0) {
        printf("ERROR making segment! nerr=%d\n", i);
        return(-1);
    }
    gop_free(gop, OP_DESTROY);

    //** Make an empty exnode
    ex = exnode_create();

    //** and insert it
    view_insert(ex, seg);


    //** Print it
    exp = exnode_exchange_create(EX_TEXT);
    exnode_serialize(ex, exp);
    printf("%s", exp->text.text);

    //** and Save if back to disk
    FILE *fd = fopen(fname_out, "w");
    fprintf(fd, "%s", exp->text.text);
    fclose(fd);
    exnode_exchange_destroy(exp);


    //** Clean up
    exnode_destroy(ex);

    rs_query_destroy(lio_gc->rs, rq);

    lio_shutdown();

    return(0);
}


