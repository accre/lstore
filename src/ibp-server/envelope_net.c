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


#include "envelope.h"
#include <tbx/network.h>
#include <tbx/log.h>
#include "ibp_server.h"

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


