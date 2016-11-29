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

#ifndef __ERASURE_TOOLS_H_
#define __ERASURE_TOOLS_H_

#include <lio/erasure_tools.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif


struct lio_erasure_plan_t;

extern int _debug;
#define debug_printf(...) \
   if (_debug > 0) { \
      printf(__VA_ARGS__); \
   }


#define REED_SOL_VAN    0
#define REED_SOL_R6_OP  1
#define CAUCHY_ORIG     2
#define CAUCHY_GOOD     3
#define BLAUM_ROTH      4
#define LIBERATION      5
#define LIBER8TION      6
#define RAID4           7
#define N_JE_METHODS    8

extern const char *JE_method[N_JE_METHODS];


struct lio_erasure_plan_t {    //** Contains the erasure parameters
    long long int strip_size;   //** Size of each data strip
    int method;                 //** Encoding/Decoding method used
    int data_strips;            //** Number of data devices
    int parity_strips;          //** Number of coding or parity devices
    int w;                      //** Word size
    int packet_size;            //** Chunk size for operations
    int base_unit;              //** Typically the register size in bytes
    int *encode_matrix;         //** Encoding Matrix
    int *encode_bitmatrix;      //** Encoding bit Matrix
    int **encode_schedule;      //** Encoding Schedule
    int (*form_encoding_matrix)(lio_erasure_plan_t *plan);  //**Routine to form encoding matrix
    int (*form_decoding_matrix)(lio_erasure_plan_t *plan);  //**Routine to form encoding matrix
    void (*encode_block)(lio_erasure_plan_t *plan, char **ptr, int block_size);  //**Routine for encoding the block
    int (*decode_block)(lio_erasure_plan_t *plan, char **ptr, int block_size, int *erasures);  //**Routine for decoding the block
};

int nearest_prime(int w, int which);
int et_method_type(char *meth);
lio_erasure_plan_t *et_new_plan(int method, long long int strip_size,
                            int data_strips, int parity_strips, int w, int packet_size, int base_unit);
lio_erasure_plan_t *et_generate_plan(long long int file_size, int method,
                                 int data_strips, int parity_strips, int w, int packet_low, int packet_high);
void et_destroy_plan(lio_erasure_plan_t *plan);
int et_encode(lio_erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size);
int et_decode(lio_erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures);

#ifdef __cplusplus
}
#endif


#endif
