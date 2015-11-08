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

#define _log_module_index 172

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "type_malloc.h"
#include "lio.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i;
    char *fname = NULL;
    exnode_t *ex;
    exnode_exchange_t *exp;
    exnode_exchange_t *exp_in;

    if (argc < 4) {
        printf("\n");
        printf("ex_load LIO_COMMON_OPTIONS file.ex3\n");
        lio_print_options(stdout);
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //** Load the fixed options
    i = 1;
    fname = argv[i];
    i++;

    if (fname == NULL) {
        printf("Missing input filename!\n");
        return(2);
    }


    //** Create a blank exnode
    ex = exnode_create();

    //** Load it
    exp_in = exnode_exchange_load_file(fname);

    printf("Initial exnode=====================================\n");
    printf("%s", exp_in->text.text);
    printf("===================================================\n");

    exnode_deserialize(ex, exp_in, lio_gc->ess);

    //** Print it
    exp = exnode_exchange_create(EX_TEXT);
    exnode_serialize(ex, exp);

    printf("Loaded exnode=====================================\n");
    printf("%s", exp->text.text);
    printf("===================================================\n");

    exnode_exchange_destroy(exp_in);
    exnode_exchange_destroy(exp);

    exnode_destroy(ex);

    lio_shutdown();

    return(0);
}


