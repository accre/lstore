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

#ifndef __ERASURE_TOOLS_H_
#define __ERASURE_TOOLS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

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


typedef struct erasure_plan_s erasure_plan_t;

struct erasure_plan_s {    //** Contains the erasure parameters
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
    int (*form_encoding_matrix)(erasure_plan_t *plan);  //**Routine to form encoding matrix
    int (*form_decoding_matrix)(erasure_plan_t *plan);  //**Routine to form encoding matrix
    void (*encode_block)(erasure_plan_t *plan, char **ptr, int block_size);  //**Routine for encoding the block
    int (*decode_block)(erasure_plan_t *plan, char **ptr, int block_size, int *erasures);  //**Routine for decoding the block
};

int nearest_prime(int w, int force_larger);
int et_method_type(char *meth);
erasure_plan_t *et_new_plan(int method, long long int strip_size,
                            int data_strips, int parity_strips, int w, int packet_size, int base_unit);
erasure_plan_t *et_generate_plan(long long int file_size, int method,
                                 int data_strips, int parity_strips, int w, int packet_low, int packet_high);
void et_destroy_plan(erasure_plan_t *plan);
int et_encode(erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffersize);
int et_decode(erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffersize, int *erasures);


#ifdef __cplusplus
}
#endif


#endif



