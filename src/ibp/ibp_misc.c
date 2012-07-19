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
// parse_cap - Parses the provided capability
//    NOTE:  host, key, and typekey should have at least 256 char
//*************************************************************

int parse_cap(ibp_cap_t *cap, char *host, int *port, char *key, char *typekey)
{
  char *bstate;
  int finished = 0;

  host[255] = '\0';
  key[255] = '\0';
  typekey[255] = '\0';

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
  strncpy(host,  ptr, 255);; //** This should be the host name
  sscanf(string_token(NULL, "/", &bstate, &finished), "%d", port);  

  strncpy(key, string_token(NULL, "/", &bstate, &finished), 255);
  strncpy(typekey, string_token(NULL, "/", &bstate, &finished), 255);

  free(temp);

//  log_printf(15, "parse_cap: CAP=%s * parsed=%s:%d/%s/%s\n", cap, host, *port, key, typekey);

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


