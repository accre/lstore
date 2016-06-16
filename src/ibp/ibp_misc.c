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

#define _log_module_index 131

#include <signal.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/signal.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/string_token.h>

#include "ibp_op.h"
#include "ibp_types.h"


//*************************************************************
// ibppc_form_host forms the host string for hte portal context
//    based on the IC connection mode
//*************************************************************

void ibppc_form_host(ibp_context_t *ic, char *hoststr, int n_host, char *host, rid_t rid)
{
    int i, j, n;
    char rr[16];

    //** Everybody gets the host copied
    for (i=0 ; (i<n_host) && (host[i] != '\0') ; i++) hoststr[i] = host[i];
    if (i>=n_host-2) { //** Space isn't big enough so truncate and return
        if (i<n_host) hoststr[i] = '\0';
        return;
    }

    //** If we make it here we have enough space for at least "# ? NULL"

    switch (ic->connection_mode) {
    case IBP_CMODE_RID:     //** Add the "#RID"
        hoststr[i] = '#';
        i++;
        for (j=0; (i<n_host) && (rid.name[j] != '\0'); i++, j++) hoststr[i] = rid.name[j];
        break;
    case IBP_CMODE_ROUND_ROBIN:
        hoststr[i] = '#';
        i++;
        n = tbx_atomic_inc(ic->rr_count);
        n = n % ic->rr_size;
        snprintf(rr, sizeof(rr), "%d", n);
        for (j=0; (i<n_host) && (rr[j] != '\0'); i++, j++) {
            hoststr[i] = rr[j];
        }
        break;
    }

    if (i<n_host) {
        hoststr[i] = '\0';
    } else {
        hoststr[n_host-1] = '\0';
    }

    return;
}


//*************************************************************
// parse_cap - Parses the provided capability
//    NOTE:  host, key, and typekey should have at least 256 char
//*************************************************************

int parse_cap(ibp_context_t *ic, ibp_cap_t *cap, char *host, int *port, char *key, char *typekey)
{
    char *bstate;
    int finished = 0;
    int i, j, n, m;
    char rr[16];

    host[MAX_HOST_SIZE-1] = '\0';
    host[0] = '\0';
    key[MAX_KEY_SIZE-1] = '\0';
    key[0] = '\0';
    typekey[MAX_KEY_SIZE-1] = '\0';
    typekey[0] = '\0';
    *port = -1;

    if (cap == NULL) return(1);

    char *temp = strdup(cap);
    char *ptr;
    tbx_stk_string_token(temp, "/", &bstate, &finished); //** gets the ibp:/
    ptr = tbx_stk_string_token(NULL, ":", &bstate, &finished); //** This should be the hostname
    ptr = &(ptr[1]);  //** Skip the extra "/"
    sscanf(tbx_stk_string_token(NULL, "/", &bstate, &finished), "%d", port);
    strncpy(host,  ptr, MAX_HOST_SIZE-1); //** This should be the host name

    strncpy(key, tbx_stk_string_token(NULL, "/", &bstate, &finished), 255);
    strncpy(typekey, tbx_stk_string_token(NULL, "/", &bstate, &finished), 255);

    switch (ic->connection_mode) {
    case IBP_CMODE_RID:
        n = strlen(host);
        host[n] = '#';
        n++;
        m = strlen(key);
        m = ((m+n) > MAX_HOST_SIZE-1) ? MAX_HOST_SIZE-1 - n : m;
        for (i=0; i<m; i++) {
            if (key[i] == '#') {
                host[i+n] = 0;
                break;
            }
            host[i+n] = key[i];
        }
        break;
    case IBP_CMODE_ROUND_ROBIN:
        n = tbx_atomic_inc(ic->rr_count);
        n = n % ic->rr_size;
        snprintf(rr, sizeof(rr), "%d", n);
        n = strlen(host);
        host[n] = '#';
        n++;
        for (j=0, i=n; (i<MAX_HOST_SIZE) && (rr[j] != '\0'); i++, j++) host[i] = rr[j];
        if (i<1024) {
            host[i] = '\0';
        } else {
            host[MAX_HOST_SIZE-1] = '\0';
        }
    }

    free(temp);

    log_printf(14, "parse_cap: CAP=%s * parsed=[%s]:%d/%s/%s\n", cap, host, *port, key, typekey);

    if (finished == 1) log_printf(0, "parse_cap:  Error parsing cap %s\n", cap);

    return(finished);
}

//*************************************************************
// parse_cmpstr - Parses the cmpstr for an IO_op_t.
//     cmpstr format: host:port: size
//*************************************************************

int parse_cmpstr(char *str, char *host, int *port, int *size)
{
    return(sscanf(str, "%s:%d:%d\n", host, port, size));
}

//*****************************************************************************
// ibp_configure_signals - Configures the signals
//*****************************************************************************

void ibp_configure_signals()
{
#ifdef SIGPIPE
    //** Ignore SIGPIPE
    struct sigaction action;
    action.sa_handler = SIG_IGN;
    sigemptyset(&(action.sa_mask));
    action.sa_flags = 0;
    sigaction(SIGPIPE, &action, NULL);
#endif
}


