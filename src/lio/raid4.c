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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

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

  
