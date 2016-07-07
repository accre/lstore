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

#define _log_module_index 172

#include <stdio.h>

#include <lio/ex3.h>
#include <lio/lio.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i;
    char *fname = NULL;
    lio_exnode_t *ex;
    lio_exnode_exchange_t *exp;
    lio_exnode_exchange_t *exp_in;

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
    ex = lio_exnode_create();

    //** Load it
    exp_in = lio_exnode_exchange_load_file(fname);

    printf("Initial exnode=====================================\n");
    printf("%s", exp_in->text.text);
    printf("===================================================\n");

    lio_exnode_deserialize(ex, exp_in, lio_gc->ess);

    //** Print it
    exp = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(ex, exp);

    printf("Loaded exnode=====================================\n");
    printf("%s", exp->text.text);
    printf("===================================================\n");

    lio_exnode_exchange_destroy(exp_in);
    lio_exnode_exchange_destroy(exp);

    lio_exnode_destroy(ex);

    lio_shutdown();

    return(0);
}


