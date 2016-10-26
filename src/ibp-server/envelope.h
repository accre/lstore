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

#ifndef __ENVELOPE_H_
#define __ENVELOPE_H_

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ENVELOPE_SIZE 8

typedef struct {
  union {
    unsigned char byte[4];
    uint32_t n;
  };
} env_command_t;

typedef struct {
  env_command_t cmd;
  uint32_t size;
} envelope_t;

void envelope_clear(envelope_t *env);
int envelope_parse(envelope_t *env, const unsigned char *buffer);
void envelope_encode(const envelope_t *env, unsigned char *buffer);
uint32_t envelope_get_size(envelope_t *env);
void envelope_set_size(envelope_t *env, uint32_t size);
env_command_t envelope_get_command(envelope_t *env);
void envelope_set_command(envelope_t *env, const env_command_t cmd);
void envelope_set(envelope_t *env, const env_command_t cmd, uint32_t size);
void envelope_get(envelope_t *env, env_command_t *cmd, uint32_t *size);
void set_env_command(env_command_t *cmd, int b0, int b1, int b2, int b3);
void set_env_command_uint32(env_command_t *cmd, uint32_t n);
uint32_t get_env_command_uint32(env_command_t *cmd);
int env_cmd_within_subnet(env_command_t *cmd, env_command_t *cmd_net, int bits);
int env_cmd_compare(env_command_t *cmd1, env_command_t *cmd2);

#ifdef __cplusplus
}
#endif

#endif


