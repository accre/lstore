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

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>

#define I64T "%" PRId64    //int64_t
#define U64T "%" PRIu64    //uint64_t


//*******************************************************************************
//  varint_encode - Encodes an integer using base 128 variants.
//     The number of bytes used are returned.  The buffer shoulbe be at least 10 bytes
//*******************************************************************************

int varint_encode(uint64_t value, uint8_t *buffer)
{
  int i;

  for (i=0; i<10; i++) {
    buffer[i] = value | 0x80;

    value >>= 7;
    if (value == 0) {  //** Last byte so unset bit 8.
       buffer[i] ^= 0x80;
//printf("ve: b[%d]=%u value=" U64T"\n", i, buffer[i], value);
       return(i+1);
    }
//printf("ve: b[%d]=%u value=" U64T"\n", i, buffer[i], value);
  }

  return(-1);
}

//*******************************************************************************
//  varint_decode - Decodes an integer using base 128 variants.
//     The number of bytes used from the buffer are returned.
//*******************************************************************************

int varint_decode(uint8_t *buffer, int bufsize, uint64_t *value)
{
  int i, bits;

  *value = 0;
  for (i=0, bits = 0; i<bufsize; i++, bits += 7) {
    *value += (buffer[i] & 0x7F) << bits;
//printf("vd: b[%d]=%u value=" U64T "\n", i, buffer[i], *value);
    if ((buffer[i] & 0x80) == 0) {  //** Last byte
       return(i+1);
    }
  }

  return(-1);
}

//*******************************************************************************
//  zigzag_encode - Uses zigzag encoding to store the integer
//*******************************************************************************

int zigzag_encode(int64_t value, uint8_t *buffer)
{
  uint64_t zz;

  zz = (value < 0) ? 2*(-value) - 1 : 2*value;

  return(varint_encode(zz, buffer));
}

//*******************************************************************************
// zigzag_decode - Decodes the zigzaged number
//*******************************************************************************

int zigzag_decode(uint8_t *buffer, int bufsize, int64_t *value)
{
   uint64_t zz;
   int n;

   n = varint_decode(buffer, bufsize, &zz);

   *value = (zz & 1) ? -(zz >> 1) - 1 : zz >> 1;

   return(n);
}


//*******************************************************************************
// varint_test - Test routine
//*******************************************************************************

int varint_test()
{
  int i, j, bytes, dbytes;
  int asize = 11;
  uint64_t varray[] = {0, 1, 127, 128, 129, 16383, 16384, 16385, 2097151, 2097152, 2097153};
  int      nbytes[] = {1, 1,   1,   2,   2,     2,     3,     3,       3,       4,       4};

  uint64_t result;
  int64_t zz_value, zz_result;
  unsigned char buffer[20];

  for (i=0; i< asize; i++) {
     memset(buffer, 0, sizeof(buffer));
     bytes = varint_encode(varray[i], buffer);
     if (bytes != nbytes[i]) {
       printf("VARINT ENCODE Bytes used mismatch value=" U64T " used=%d should be=%d\n", varray[i], bytes, nbytes[i]);
       abort();
     }

     bytes = varint_decode(buffer, 20, &result);
//printf("v=" U64T " r=" U64T "\n", varray[i], result);
     if (bytes != nbytes[i]) {
       printf("VARINT DECODE Bytes used mismatch value=" U64T " used=%d should be=%d\n", varray[i], bytes, nbytes[i]);
       abort();
     }

     if (result != varray[i]) {
       printf("VARINT DECODE incorrect value=" U64T " should be=" U64T "\n", result, varray[i]);
       abort();
     }

     for (j=-1; j<2; j += 2) {
        memset(buffer, 0, sizeof(buffer));
        zz_value = j * varray[i];
        bytes = zigzag_encode(zz_value, buffer);

        dbytes = zigzag_decode(buffer, 20, &zz_result);
printf("v=" I64T " r=" I64T "\n", zz_value, zz_result);
        if (bytes != dbytes) {
           printf("ZIGZAG DECODE Bytes used mismatch value=" I64T " used=%d should be=%d\n", zz_value, bytes, dbytes);
          abort();
        }

        if (zz_result != zz_value) {
           printf("ZIGZAG DECODE incorrect value=" I64T " should be=" I64T "\n", zz_result, zz_value);
           abort();
        }
     }
  }

  return(0);
}

