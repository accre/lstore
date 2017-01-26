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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "subnet.h"
#include <tbx/dns_cache.h>
#include <tbx/string_token.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

unsigned char bitmask[8] = {128, 192, 224, 240, 248, 252, 254, 255};

char local_address[16];

//*************************************************************************
// ipdecstr2address - Converts a "." string representation to a byte version
//    This routine is for internal use and so doesn't worry with
//    host/network format.
//    The adress type is returned AF_INET or AF_INET6 or -1 for an error
//*************************************************************************

int ipdecstr2address(char *src, char *dest)
{
  int j, fin;
  char *number, *bstate;
  memset(dest, 0, 16);

  char *srcdup = strdup(src);

  j = 0;   //** Parse the address and determine the address type
  number = tbx_stk_string_token(srcdup, ".", &bstate, &fin);
  while ((number != NULL) && (j<16)) {
     dest[j] = atoi(number);
     j++;
     number = tbx_stk_string_token(NULL, ".", &bstate, &fin);
  }

  fin = AF_INET;
  if (j > 3) fin = AF_INET;

  free(srcdup);

  return(fin);
}

//*************************************************************************
// address2ipdecstr - Converts a byte version to a string representation
//    This routine is for internal use and so doesn't worry with
//    host/network format.
//    The adress type is returned AF_INET or AF_INET6 or -1 for an error
//*************************************************************************

void address2ipdecstr(char *dest, char *byteaddress, int family)
{
  int i,n, val;
  char dec[8];
  n = 4;
  if (family == AF_INET6) n = 16;

  dest[0] = '\0';
  for (i=0; i<n-1; i++) {
     val = (unsigned char)byteaddress[i];
     sprintf(dec, "%d.", val);
     strcat(dest, dec);
  } 

  val = (unsigned char)byteaddress[n-1];
  sprintf(dec, "%d", val);
  strcat(dest, dec);  
}

//*************************************************************************
// apply_subnet_mask - Applies a given subnet mask
//*************************************************************************

void apply_subnet_mask(int bits, char *address, char *masked_address)
{
  int j, n;

  if (masked_address != address) memcpy(masked_address, address, 16);

  n = bits / 8;
  for (j=n+1; j<16; j++) masked_address[j] = 0;  // Blank the lower bits as needed
  j = bits % 8;
  masked_address[n] = masked_address[n] & bitmask[j];   
}

//*************************************************************************
// new_subnet_list - Creates a new subnet list
//*************************************************************************

subnet_list_t *new_subnet_list(char *list_string)
{
   int n, i, fin;
   subnet_t *s;
   subnet_list_t *sn;
   char acl_text[128];
   char *mask, *mstate;
   char *list_str = strdup(list_string);

   tbx_type_malloc_clear(sn, subnet_list_t, 1);

   //** Determin the list size
   char *list[100];
   n = 0; 
   list[n] = tbx_stk_string_token(list_str, ";", &mstate, &fin);
   while (strcmp(list[n], "") != 0) {
     n++; 
     list[n] = tbx_stk_string_token(NULL, ";", &mstate, &fin);
   }
   
   sn->n = 0;
   sn->mode = SUBNET_LOCAL;
   sn->range = NULL;

   if (strcmp(list[0], "open") == 0) {  //** Accept any connections
      sn->mode = SUBNET_OPEN;
   } else if (strcmp(list[0], "local") == 0) {  //** Only local connections are allowed 
      sn->mode = SUBNET_LOCAL;
   } else {       //** Process the mask
      sn->mode = SUBNET_LIST;
      sn->n = n;
      tbx_type_malloc_clear(sn->range, subnet_t, n);

      for (i=0; i<sn->n; i++) {  //** Process each element
         s = &(sn->range[i]);
         strncpy(acl_text, list[i], sizeof(acl_text)); acl_text[sizeof(acl_text)-1] = '\0';
         log_printf(15, "new_subnet_list: acl_text=%s\n", acl_text);
         mask = tbx_stk_string_token(acl_text, "/", &mstate, &fin);
         s->type = ipdecstr2address(mask, s->mask);
         n = 4;
         if (s->type == AF_INET6) {
            n = 16;
         }

         //** Now get the "class"
         s->bits = atoi(tbx_stk_string_token(NULL, "/", &mstate, &fin));

         //** and apply it
         apply_subnet_mask(s->bits, s->mask, s->mask); 
      }
   }

   free(list_str);
   return(sn);
}

//*************************************************************************
// destroy_subnet_list - Destroys a subnet list structure
//*************************************************************************

void destroy_subnet_list(subnet_list_t *sn) {
  free(sn->range);
  free(sn);
}

//*************************************************************************
// subnet_validate - Checks if the address is in the specified subnet
//       and returns 1 if Ok and 0 otherwise.
//*************************************************************************

int subnet_validate(subnet_t *sn, char *address) 
{
   int result;
   subnet_t madd;

   apply_subnet_mask(sn->bits, address, madd.mask);
 
   result = memcmp(madd.mask, sn->mask, sizeof(sn->mask));
   if (result == 0) { 
      result = 1;
   } else {
      result = 0;
   }

   return(result);
}


//*************************************************************************
// subnet_list_validate - Checks if the address is in one of the specified 
//       subnets and returns 1 if Ok and 0 otherwise.
//*************************************************************************

int subnet_list_validate(subnet_list_t *sl, char *address) 
{
  int result, i;
  subnet_t sadd;
  char *add;

  result = 0;

  switch (sl->mode) {
     case SUBNET_OPEN : 
          return(1); break;
     case SUBNET_LIST :
     case SUBNET_LOCAL :
          add = strdup(address);
          ipdecstr2address(add, sadd.mask);
          free(add);

          for (i=0; i<sl->n; i++) {
             if (subnet_validate(&(sl->range[i]), sadd.mask) == 1) {
                return(1);
             }
          }   

          //** check if they are connecting locally
          i = memcmp(local_address, sadd.mask, 16);
          if (i == 0) result = 1;
          break; 
  }

  return(result);
}

//*************************************************************************
// init_subnet_list - Destroys a subnet list structure
//*************************************************************************

void init_subnet_list(char *hostname) {
  char in_addr[16];
  char ip_address[512];

  if (tbx_dnsc_lookup(hostname, in_addr, NULL) != 0) {
     log_printf(0, "init_subnet_list: tbx_dnsc_lookup failed.  Hostname: %s\n", hostname);
     abort();
  }

  if (inet_ntop(AF_INET, in_addr, ip_address, 511) == NULL) {
     log_printf(0, "init_subnet_list: inet_ntop failed.  Hostname: %s\n", hostname);
     abort();
  }

  ipdecstr2address(ip_address, local_address);  
}
