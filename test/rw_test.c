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

#define _log_module_index 171

#include <assert.h>
#include <tbx/assert_result.h>
#include <math.h>
#include <apr_time.h>

//#include "exnode.h"
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/random.h>
#include <gop/opque.h>
#include <lio/lio.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option, rw_mode, errors;
    char *section = "rw_params";

//printf("argc=%d\n", argc);
    if (argc < 2) {
        printf("\n");
        printf("ex_rw_test LIO_COMMON_OPTIONS [-ex|-aio|-tq|-local] [-s section]\n");
        lio_print_options(stdout);
        printf("     -ex        Use the exnode driver\n");
        printf("     -aio       Use LIO Asynchrounous I/O\n");
        printf("     -wq        Use the LIO Work Queue\n");
        printf("     -local     Use a local file\n");

        printf("     -s          section Section in the config file to usse.  Defaults to %s.\n", section);
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    rw_mode = -1;
    i=1;
    if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-ex") == 0) { //** Use the segment driver
                i++;
                rw_mode = 0;
            } else if (strcmp(argv[i], "-aio") == 0) { //** Normal LIO Async I/O
                i++;
                rw_mode = 1;
            } else if (strcmp(argv[i], "-wq") == 0) { //** LIO Task Queue
                i++;
                rw_mode = 2;
            } else if (strcmp(argv[i], "-local") == 0) { //** Local File
                i++;
                rw_mode = 3;
            } else if (strcmp(argv[i], "-s") == 0) { //** Change the default section to use
                i++;
                section = argv[i];
                i++;
            }
        } while ((start_option < i) && (i<argc));
    }

    errors = lio_rw_test_exec(rw_mode, section);

    lio_shutdown();

    return(errors);
}


