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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "assert_result.h"

//******************************************************************************
//  xor_block - XOR's a block of data
//******************************************************************************

void xor_block(char *data, char *parity, int nbytes)
{
    int i;

    for (i=0; i<nbytes; i++) parity[i] = parity[i] ^ data[i];

    return;
}


//******************************************************************************
//  raid4_encode - Encodes the given data blocks
//******************************************************************************

void raid4_encode(int data_strips, char **data, char **parity, int block_size)
{
    int i;

    memcpy(parity[0], data[0], block_size);

    for (i=1; i<data_strips; i++) xor_block(data[i], parity[0], block_size);

    return;
}

//******************************************************************************
//  raid4_decode - Decodes the data
//******************************************************************************

int raid4_decode(int data_strips, int *erasures, char **data, char **parity, int block_size)
{
    int i, k;

    if (erasures[1] != -1) return(-1);  //** Too many missing blocks to recover from
    if (erasures[0] >= data_strips) return(0);  //** Lost parity only so return

    k = erasures[0];
    memcpy(data[k], parity[0], block_size);

    for (i=0; i<data_strips; i++) {
        if (i != k) {
            xor_block(data[i], data[k], block_size);
        }
    }

    return(0);
}


