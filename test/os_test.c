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

#define _log_module_index 187

#include <assert.h>
#include <tbx/list.h>
#include <lio/authn.h>
#include <lio/ds.h>
#include <tbx/skiplist.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <tbx/log.h>

//*************************************************************************
//  Object service test program
//*************************************************************************

void print_help()
{
    printf("\n");
    printf("os_test LIO_COMMON_OPTIONS path\n");
    lio_print_options(stdout);
    printf("    path  - Path prefix to use\n");
    printf("\n");
}

int main(int argc, char **argv)
{
    int nfailed;
    lio_path_tuple_t tuple;

    if (argc < 2) {
        print_help();
        return(1);
    }

    lio_init(&argc, &argv);

    if (argc < 1) {
        printf("Missing PATH!\n");
        print_help();
    }
    tuple = lio_path_resolve(lio_gc->auto_translate, argv[1]);

    log_printf(0, "--------------------------------------------------------------------\n");
    log_printf(0, "Using prefix=%s\n", tuple.path);
    log_printf(0, "--------------------------------------------------------------------\n");
    tbx_log_flush();

    nfailed = os_create_remove_tests(tuple.path);
    if (nfailed > 0) goto oops;

    nfailed = os_attribute_tests(tuple.path);
    if (nfailed > 0) goto oops;

    nfailed = os_locking_tests(tuple.path);
    if (nfailed > 0) goto oops;

oops:
    log_printf(0, "--------------------------------------------------------------------\n");
    log_printf(0, "Tasks failed: %d\n", nfailed);
    log_printf(0, "--------------------------------------------------------------------\n");

    lio_path_release(&tuple);

    lio_shutdown();

    return(nfailed);
}


