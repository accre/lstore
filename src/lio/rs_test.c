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

#define _log_module_index 182

#include <assert.h>
#include "assert_result.h"
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
    int i, start_option, err;
    int n_alloc = 0;
    rs_query_t *rsq;
    op_generic_t *gop;
    opque_t *q;
    op_status_t status;
    char *qstr;
    data_cap_set_t **cap_list;
    rs_request_t *req_list;


//printf("argc=%d\n", argc);
    if (argc < 3) {
        printf("\n");
        printf("rs_test LIO_COMMON_OPTIONS -q query_string -n n_alloc\n");
        lio_print_options(stdout);
        printf("    -q query_string - Resource service Query string\n");
        printf("    -n n_alloc      - Number of allocations to request\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-n") == 0) { //** Allocation count
            i++;
            n_alloc = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-q") == 0) { //** Query string
            i++;
            qstr = argv[i];
            i++;
        }

    } while ((start_option < i) && (i<argc));


    //** Parse the query
    rsq = rs_query_parse(lio_gc->rs, qstr);

    //** Generate the data request
    type_malloc_clear(req_list, rs_request_t, n_alloc);
    type_malloc_clear(cap_list, data_cap_set_t *, n_alloc);

    for (i=0; i<n_alloc; i++) {
        cap_list[i] = ds_cap_set_create(lio_gc->ds);
        req_list[i].rid_index = i;
        req_list[i].size = 1000;  //** Don't really care how big it is for testing
        req_list[i].rid_key = NULL;  //** This will let me know if I got success as well as checking the cap
    }

    gop = rs_data_request(lio_gc->rs, lio_gc->da, rsq, cap_list, req_list, n_alloc, NULL, 0, n_alloc, 0, lio_gc->timeout);


    //** Wait for it to complete
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    if (status.op_status != OP_STATE_SUCCESS) {
        printf("Error with data request! err_code=%d\n", status.error_code);
        abort();
    }


    //** Print the caps
    printf("Query: %s  n_alloc: %d\n", qstr, n_alloc);
    printf("\n");
    for (i=0; i<n_alloc; i++) {
        printf("%d.\tRID key: %s\n", i, req_list[i].rid_key);
        printf("\tRead  : %s\n", (char *)ds_get_cap(lio_gc->ds, cap_list[i], DS_CAP_READ));
        printf("\tWrite : %s\n", (char *)ds_get_cap(lio_gc->ds, cap_list[i], DS_CAP_WRITE));
        printf("\tManage: %s\n", (char *)ds_get_cap(lio_gc->ds, cap_list[i], DS_CAP_MANAGE));
    }
    printf("\n");

    //** Now destroy the allocation I just created
    q = new_opque();
    for (i=0; i<n_alloc; i++) {
        gop = ds_remove(lio_gc->ds, lio_gc->da, ds_get_cap(lio_gc->ds, cap_list[i], DS_CAP_MANAGE), lio_gc->timeout);
        opque_add(q, gop);
    }

    //** Wait for it to complete
    err = opque_waitall(q);
    opque_free(q, OP_DESTROY);

    if (err != OP_STATE_SUCCESS) {
        printf("Error removing allocations!\n");
    }

    //** Destroy the caps
    for (i=0; i<n_alloc; i++) {
        ds_cap_set_destroy(lio_gc->ds, cap_list[i], 1);
    }
    free(cap_list);
    free(req_list);

    //** Clean up
    rs_query_destroy(lio_gc->rs, rsq);

    lio_shutdown();

    return(0);
}


