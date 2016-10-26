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

#include "envelope.h"
#include <tbx/network.h>
#include <tbx/log.h>
#include <ibp-server/ibp_server.h>

//*********************************************************************
// envelope_send - Sends an envelope
//*********************************************************************

int envelope_send(tbx_ns_t *ns, envelope_t *env)
{
  char buffer[ENVELOPE_SIZE];
  int err;

  envelope_encode(env, (unsigned char *)buffer);
  apr_time_t to = apr_time_now() + apr_time_make(30, 0);
  err = server_ns_write_block(ns, to, buffer, ENVELOPE_SIZE);
  if (err != NS_OK) {
     log_printf(0, "envelope_send: server_ns_write_block failed sending envelope header! err=%d ns=%d\n",err, tbx_ns_getid(ns));
  }

  return(err);
}

//*********************************************************************
// envelope_recv - Receives an envelope over the wire
//*********************************************************************

int envelope_recv(tbx_ns_t *ns, envelope_t *env)
{
  char buffer[ENVELOPE_SIZE];
  int err;

  apr_time_t to = apr_time_now() + apr_time_make(30, 0);
  err = server_ns_read_block(ns, to, buffer, ENVELOPE_SIZE);
  if (err != NS_OK) {
     log_printf(0, "envelope_recv: server_ns_read_block failed reading envelope header! err=%d ns=%d\n",err, tbx_ns_getid(ns));
  }

  return(envelope_parse(env, (unsigned char *)buffer));
}

//*********************************************************************
// envelope_simple_send - Sends a envelope with size 0 with only the cmd set
//*********************************************************************

int envelope_simple_send(tbx_ns_t *ns, uint32_t cmd)
{
  envelope_t env;
  env_command_t ecmd;

  set_env_command_uint32(&ecmd, cmd);
  envelope_set(&env, ecmd, 0);

  return(envelope_send(ns, &env));
}

//*********************************************************************
// envelope_simple_recv - Receives an empty envelope and reports the cmd
//*********************************************************************

int envelope_simple_recv(tbx_ns_t *ns, uint32_t *cmd)
{
  envelope_t env;
  env_command_t ecmd;
  int err;
  uint32_t size;

  err = envelope_recv(ns, &env);
  if (err != 0) return(err);

  envelope_get(&env, &ecmd, &size);
  err = size;
  log_printf(15, "envelope_simple_recv: cmd=%hhu %hhu %hhu %hhu size=%d\n", 
        ecmd.byte[0], ecmd.byte[1], ecmd.byte[2], ecmd.byte[3], err); 
  if (size != 0) return(size);

  *cmd = get_env_command_uint32(&ecmd);

  return(0);
}


