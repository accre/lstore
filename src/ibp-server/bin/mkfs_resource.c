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

#include "allocation.h"
#include "resource.h"
#include <stdio.h>
#include <assert.h>

//***********************************************************************************
//***********************************************************************************
//***********************************************************************************

int main(int argc, const char **argv)
{
   if (argc < 5) {
      printf("mkfs.resource RID type device db_location [max_mbytes]\n");
      printf("\n");
      printf("RID    - Resource ID (integer)\n");
      printf("type   - Type or resource. Currently only 'dir' is supported\n");
      printf("device - Device to be used for the resource.\n");
      printf("db_location - Base directory to use for storing the DBes for the resource.\n");
      printf("max_mbytes  - Max number of MB to use.  If missing it defaults to the entire disk.\n");
      printf("\n");      
      return(1);
   }

   int err = 0;
   rid_t rid;
   ibp_off_t nbytes = 0;

   if (argc > 5) nbytes = 1024*1024 * atoll(argv[5]);

   assert(apr_initialize() == APR_SUCCESS);

   if (ibp_str2rid((char *)argv[1], &rid) != 0) {
     printf("Invalid RID format!  RID=%s\n", argv[1]);
   } else {
     err = mkfs_resource(rid, (char *)argv[2], (char *)argv[3], (char *)argv[4], nbytes);
   }

   apr_terminate();

   return(err);
}

