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

#ifndef _SUBNET_H_
#define _SUBNET_H_

#include "visibility.h"

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

