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

//*************************************************************************
//*************************************************************************

#ifndef __CHKSUM_H_
#define __CHKSUM_H_

#include "transfer_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CHKSUM_STATE_SIZE  1024
#define CHKSUM_MAX_SIZE    257

//*** How to return the chksum
#define CHKSUM_DIGEST_HEX   0
#define CHKSUM_DIGEST_BIN   1

//*** Define the different algorithms **
#define CHKSUM_DEFAULT -1
#define CHKSUM_NONE    0
#define CHKSUM_SHA256  1
#define CHKSUM_SHA512  2
#define CHKSUM_SHA1    3
#define CHKSUM_MD5     4

#define CHKSUM_TYPE_SIZE  (CHKSUM_MD5+1)

#define CHKSUM_MAX_TYPE 5

typedef struct {    //** Generic Checksum container
    char state[CHKSUM_STATE_SIZE];  //** Used to store state information as an overlay record
    int type;                    //** Chksum type
    char *name;                  //** Pointer to the string version of the chksum type
    int (*reset)(void *state);   //** Resets chksum to initial value
    int (*size)(void *state, int type);    //** Size of chksum in bytes
    int (*get)(void *state, int type, char *value);  //** Returns the chksum string
    int (*add)(void *state, int size, tbuffer_t *data, int doff);    //** Adds the data to the check sum
} chksum_t;

//** Provide usage shortcuts
#define chksum_name(cs) (cs)->name
#define chksum_type(cs) (cs)->type
#define chksum_reset(cs) (cs)->reset((cs)->state)
#define chksum_size(cs, type) (cs)->size((cs)->state, type)
#define chksum_get(cs, type, value)  (cs)->get((cs)->state, type, value)
#define chksum_add(cs, size, data, doff)  (cs)->add((cs)->state, size, data, doff)
#define chksum_clear(cs)  blank_chksum_set(cs)

int convert_bin2hex(int in_size, const unsigned char *in, char *out);
int chksum_valid_type(int type);
int chksum_set(chksum_t *cs, int chksum_type);
int chksum_name_type(const char *name);
int blank_chksum_set(chksum_t *cs);

#ifdef __cplusplus
}
#endif

#endif


