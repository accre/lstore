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

#ifndef _SUBNET_H_
#define _SUBNET_H_

#include <ibp-server/visibility.h>

#define SUBNET_OPEN  0   //** Anybody can connect and execute the command
#define SUBNET_LOCAL 1   //** Command can only be issued from localhost
#define SUBNET_LIST  2   //** Use the specified subnet list


typedef struct {  //** Subnet type
   int type;         //** Type of subnet AF_INET or AF_INET6
   int bits;         //** "Class" bits
   char mask[16];    //** Subnet/address mask
} subnet_t;

typedef struct {  //** Subnet list
   int n;            //** Number of subnets in list
   int mode;         //** Default mode SUBNET_OPEN|SUBNET_LOCAL|SUBNET_LIST
   subnet_t *range;    //** Array of subnets
} subnet_list_t;

IBPS_API int ipdecstr2address(char *src, char *dest);
IBPS_API void address2ipdecstr(char *dest, char *byteaddress, int family);
IBPS_API subnet_list_t *new_subnet_list(char *list);
IBPS_API void destroy_subnet_list(subnet_list_t *sn);
IBPS_API int subnet_list_validate(subnet_list_t *sl, char *address);
IBPS_API void init_subnet_list(char *hostname);

#endif

