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


