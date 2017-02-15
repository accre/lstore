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

//**************************************************************************
//  Returns an unsigned 64-bit random number
//**************************************************************************

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include <stdint.h>
#include <stdio.h>
#include "random.h"

int main(int argc, char *argv[])
{
  uint64_t llu;
 
  tbx_random_startup();

  tbx_random_get_bytes(&llu, sizeof(llu));

  printf("%lu\n", llu);  

  char addr[16], name[256];

  tbx_random_get_bytes(addr, sizeof(addr));
  
  name[0] = '\0';
  inet_ntop(AF_INET6, addr, name, 256);
  printf("ipv6:%s\n", name);
}

