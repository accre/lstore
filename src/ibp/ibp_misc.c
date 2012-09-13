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

#define _log_module_index 131

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <apr_signal.h>
#include "ibp.h"
#include "log.h"
#include "string_token.h"


//*************************************************************
// ibppc_form_host forms the host string for hte portal context
//    based on the IC connection mode
//*************************************************************

void ibppc_form_host(ibp_context_t *ic, char *hoststr, int n_host, char *host, rid_t rid)
{
  int i, j, n;
  char rr[16];

//  log_printf(15, "HOST host=%s rid=%s cmode=%d\n", host, rid.name, ic->connection_mode);

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
        n = atomic_inc(ic->rr_count);
        n = n % ic->rr_size;
        snprintf(rr, sizeof(rr), "%d", n);
//log_printf(0, "HOST rr=%s i=%d\n", rr, i);
//        for (j=0; (i<n_host) && (rr[j] != '\0'); i++, j++) { printf("[%c,%d]", rr[j], j); hoststr[i] = rr[j]; }
        for (j=0; (i<n_host) && (rr[j] != '\0'); i++, j++) { hoststr[i] = rr[j]; }
//printf("\n i=%d\n", i);
        break;
  }

  if (i<n_host) {
     hoststr[i] = '\0';
  } else {
     hoststr[n_host-1] = '\0';
  }

//  log_printf(15, "HOST hoststr=%s host=%s rid=%s cmode=%d\n", hoststr, host, rid.name, ic->connection_mode);
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
  key[MAX_KEY_SIZE-1] = '\0';
  typekey[MAX_KEY_SIZE-1] = '\0';

  if (cap == NULL) {
     host[0] = '\0'; key[0] = '\0'; typekey[0] = '\0';
     *port = -1;
     return(1);
  }

  char *temp = strdup(cap);
  char *ptr;
  ptr = string_token(temp, "/", &bstate, &finished); //** gets the ibp:/
//log_printf(15, "1 ptr=%s\n", ptr);
  ptr = string_token(NULL, ":", &bstate, &finished); //** This should be the hostname
//log_printf(15, "2 ptr=%s\n", ptr);
  ptr = &(ptr[1]);  //** Skip the extra "/"
//log_printf(15, "3 ptr=%s\n", ptr);
  sscanf(string_token(NULL, "/", &bstate, &finished), "%d", port);
  strncpy(host,  ptr, MAX_HOST_SIZE-1); //** This should be the host name

//log_printf(15, "ptr=%s host=%s ccmode=%d\n", ptr, host, ic->connection_mode);

  strncpy(key, string_token(NULL, "/", &bstate, &finished), 255);
  strncpy(typekey, string_token(NULL, "/", &bstate, &finished), 255);

  switch (ic->connection_mode) {
    case IBP_CMODE_RID:
       n = strlen(host);
       host[n] = '#'; n++;
       m = strlen(key);
       m = ((m+n) > MAX_HOST_SIZE-1) ? MAX_HOST_SIZE-1 - n : m;
       for (i=0; i<m; i++) {
          if (key[i] == '#') { host[i+n] = 0; break; }
          host[i+n] = key[i];
       }
       break;
    case IBP_CMODE_ROUND_ROBIN:
        n = atomic_inc(ic->rr_count);
        n = n % ic->rr_size;
        snprintf(rr, sizeof(rr), "%d", n);
        n = strlen(host);
        host[n] = '#'; n++;
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
//  apr_signal_block(SIGPIPE);

  //** Ignore SIGPIPE
  struct sigaction action;
  action.sa_handler = SIG_IGN;
  sigemptyset(&(action.sa_mask));
  action.sa_flags = 0;
  sigaction(SIGPIPE, &action, NULL);

#endif
}


