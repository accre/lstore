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

//***************************************************************
//***************************************************************

#define _log_module_index 156

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "log.h"
#include "phoebus.h"
#include "errno.h"
#include "string_token.h"
#include "append_printf.h"
#include "iniparse.h"

phoebus_t *global_phoebus = NULL;

#ifndef _ENABLE_PHOEBUS       //** Dummy phoebus routines
void phoebus_init(void) { };
void phoebus_destroy(void) { };
int phoebus_print(char *buffer, int *used, int nbytes)
{
    return(0);
}
void phoebus_load_config(inip_file_t *kf) { };
void phoebus_path_set(phoebus_t *p, const char *path) { };
void phoebus_path_destroy(phoebus_t *p) { };
void phoebus_path_to_string(char *string, int max_size, phoebus_t *p)
{
    string[0] = '\0';
};
char *phoebus_get_key(phoebus_t *p)
{
    return("");
}

#else                         //** Actual Phoebus routines

//***************************************************************
//  phoebus_path_set - Sets the phoebus data structure
//***************************************************************

void phoebus_path_set(phoebus_t *p, const char *path)
{
    char *hop, *bstate;
    char *stage[100];
    int finished;

    if (path == NULL) {    //** If NULL set to defaults and return
        p->path_string = NULL;
        p->path = NULL;
        p->p_count = 0;
        return;
    }

    //** Parse the path **
    p->p_count = 0;
    p->path_string = strdup(path);

    p->key = strdup(path);

    hop = string_token(p->path_string, ",", &bstate, &finished);
    while (finished == 0) {
        stage[p->p_count] = hop;
        p->p_count++;
        hop = string_token(NULL, ",", &bstate, &finished);
    }

    //** Copy the path to the final location
    p->path = (char **)malloc(sizeof(char *) * p->p_count);
    assert(p->path != NULL);
    memcpy(p->path, stage, sizeof(char *) * p->p_count);

    return;
}

//***************************************************************
//  phoebus_path_destroy - Frees the internal phoebus data structure
//***************************************************************

void phoebus_path_destroy(phoebus_t *p)
{
    free(p->path_string);
    free(p->path);
    free(p->key);
}

//***************************************************************
// phoebus_path_to_string - Converts a pheobus path a character string
//***************************************************************

void phoebus_path_to_string(char *string, int max_size, phoebus_t *p)
{
    int n, i, nleft;
    n = p->p_count-1;
    nleft = max_size-1;
    string[0] = '\0';
    for (i=0; i<n; i++) {
        strncat(string, p->path[i], nleft);
        nleft = nleft - strlen(p->path[i]);
        strncat(string, ",", nleft);
        nleft--;
    }
    strncat(string, p->path[n], nleft);
}

//***************************************************************
// phoebus_get_key - Get's the unique Phoebus key for the path
//***************************************************************

char *phoebus_get_key(phoebus_t *p)
{
    if (p != NULL) return(p->key);
    if (global_phoebus != NULL) return(global_phoebus->key);

    return("");
}


//***************************************************************
// phoebus_init - Phoebus initialization routine
//***************************************************************

void phoebus_init(void)
{
    if (global_phoebus != NULL) return;

    global_phoebus = (phoebus_t *)malloc(sizeof(phoebus_t));
    if (global_phoebus == NULL) {
        log_printf(0, "phoebus_init:  Aborting programm!! Malloc failed!\n");
        abort();
    }

    phoebus_path_set(global_phoebus, NULL);

    if (liblsl_init() < 0) {
        perror("liblsl_init(): failed");
        exit(errno);
    }

    if (getenv("PHOEBUS_PATH") != NULL) {
        phoebus_path_set(global_phoebus, getenv("PHOEBUS_PATH"));
        if (!global_phoebus->path) {
            log_printf(0, "phoebus_init: Parsing of variable PHOEBUS_PATH failed.  It needs to be a comma separated list of depot IDs\n");
            global_phoebus->p_count = 0;
        }
        log_printf(10, "phoebus_init: Using the gateway specified in environmental variable PHOEBUS_PATH: \"%s\"\n", getenv("PHOEBUS_PATH"));
    } else if (getenv("PHOEBUS_GW") != NULL) {
        phoebus_path_set(global_phoebus, getenv("PHOEBUS_GW"));
        log_printf(10, "phoebus_init: Using the gateway specified in environmental variable PHOEBUS_GW: \"%s\"\n", getenv("PHOEBUS_GW"));
    }
}

//***************************************************************
// phoebus_destroy - Phoebus shutdown routine
//***************************************************************

void phoebus_destroy(void)
{
    phoebus_path_destroy(global_phoebus);
    free(global_phoebus);
}

//***************************************************************
// phoebus_print - Prints phoebus config
//***************************************************************

int phoebus_print(char *buffer, int *used, int nbytes)
{
    int i, n;

    append_printf(buffer, used, nbytes, "[phoebus]\n");
    if (global_phoebus->p_count <= 0) return(0);

    append_printf(buffer, used, nbytes, "gateway = ");
    n = global_phoebus->p_count-1;
    for (i=0; i<n; i++) {
        append_printf(buffer, used, nbytes, "%s,", global_phoebus->path[i]);
    }
    i = append_printf(buffer, used, nbytes, "%s\n", global_phoebus->path[n]);

    return(i);
}

//***************************************************************
// phoebus_load_config - Prints phoebus config
//***************************************************************

void phoebus_load_config(inip_file_t *kf)
{
    if (global_phoebus == NULL) phoebus_init();

    char *gateway = inip_get_string(kf, "phoebus", "gateway", NULL);

    if (gateway != NULL) {
        phoebus_path_set(global_phoebus, gateway);
        log_printf(10, "phoebus_init: Using the gateway specified in local config: %s\n", gateway);
        free(gateway);
    } else if (!global_phoebus->path) {
        log_printf(10, "phoebus_init: Error, no valid Phoebus Gateway specified!\n");
        abort();
    }
}

#endif


