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

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "envelope.h"

unsigned char _env_bitmask[8] = {128, 192, 224, 240, 248, 252, 254, 255};

//****************************************************************
// envelope_clear - Clears/initializes an envelope
//****************************************************************

void envelope_clear(envelope_t *env)
{
  memset(env, 0, sizeof(envelope_t));
}

//****************************************************************
// envelope_parse - Parses an envelope
//****************************************************************

int envelope_parse(envelope_t *env, const unsigned char *buffer)
{
   memcpy(&(env->cmd), &(buffer[0]), 4);
   env->size = buffer[4] + 256*buffer[5] + 65536*buffer[6] + 16777216*buffer[7];

   return(0);
}

//****************************************************************
// envelope_encode - Encodes an envelope
//****************************************************************

void envelope_encode(const envelope_t *env, unsigned char *buffer)
{
   uint32_t val;

   memcpy(&(buffer[0]), &(env->cmd), 4);
  
   val = env->size;
   buffer[4] = val % 256; val = val >> 8;
   buffer[5] = val % 256; val = val >> 8;
   buffer[6] = val % 256; val = val >> 8;
   buffer[7] = val;
}


//****************************************************************
// envelope manipulation functions
//****************************************************************

uint32_t envelope_get_size(envelope_t *env) { return(env->size); }
void envelope_set_size(envelope_t *env, uint32_t size) { env->size = size; }

env_command_t envelope_get_command(envelope_t *env) { return(env->cmd); }
void envelope_set_command(envelope_t *env, env_command_t cmd) { env->cmd = cmd; }
 
void envelope_set(envelope_t *env, const env_command_t cmd, uint32_t size) 
{
  env->cmd = cmd;
  env->size = size;
}

void envelope_get(envelope_t *env, env_command_t *cmd, uint32_t *size) 
{
  *cmd = env->cmd;
  *size = env->size;
}
 
void set_env_command(env_command_t *cmd, int b0, int b1, int b2, int b3)
{
  cmd->byte[0] = b0;
  cmd->byte[1] = b1;
  cmd->byte[2] = b2;
  cmd->byte[3] = b3;
}

void set_env_command_uint32(env_command_t *cmd, uint32_t val) 
{ 
  cmd->byte[0] = val % 256; val = val >> 8;
  cmd->byte[1] = val % 256; val = val >> 8;
  cmd->byte[2] = val % 256; val = val >> 8;
  cmd->byte[3] = val;
}

void get_env_command(env_command_t *cmd, int *b0, int *b1, int *b2, int *b3)
{
  *b0 = cmd->byte[0];
  *b1 = cmd->byte[1];
  *b2 = cmd->byte[2];
  *b3 = cmd->byte[3];
}

uint32_t get_env_command_uint32(env_command_t *cmd) 
{ 
 return(cmd->byte[0] + 256*cmd->byte[1] + 65536*cmd->byte[2] + 16777216*cmd->byte[3]);
}


//****************************************************************
//  env_cmd_within_subnet - Verifies if the command is within
//    the provided subnet range.  REturns 0 if true 
//****************************************************************

int env_cmd_within_subnet(env_command_t *cmd, env_command_t *cmd_net, int bits)
{
  unsigned char c, d;
  int i, n;

  //** Verify the full bytes
  n = bits / 8;
  for (i=0; i<(n-1); i++) {
    if (cmd->byte[i] != cmd_net->byte[i]) return(1);
  }

  //** Now check the partial byte
  i = bits % 8;
  c = cmd->byte[n] & _env_bitmask[i];
  d = cmd_net->byte[n] & _env_bitmask[i];
  if (c != d) return(1);

  return(0);
}

//****************************************************************
//  env_cmd_compare - similar to strcmp but for commands.  
//      Returns -1 if cmd1 < cmd2 0 if cmd1 == cmd2 
//      and +1 if cmd1 > cmd2.
//****************************************************************

int env_cmd_compare(env_command_t *cmd1, env_command_t *cmd2)
{
  return(memcmp(cmd1->byte, cmd2->byte, 4));
}

