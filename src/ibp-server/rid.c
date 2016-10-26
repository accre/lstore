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
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "rid.h"
#include <tbx/log.h>

//*****************************************************************

char *ibp_rid2str(rid_t rid, char *buffer)
{
  strncpy(buffer, rid.name, RID_LEN);

  return(buffer);
}

//*****************************************************************

int ibp_str2rid(char *rid_str, rid_t *rid)
{
  strncpy(rid->name, rid_str, RID_LEN);

  return(0);
}

//*****************************************************************

void ibp_empty_rid(rid_t *rid)
{
  sprintf(rid->name, "0");
}

//*****************************************************************

int ibp_rid_is_empty(rid_t rid)
{
  if (strcmp(rid.name, "0") == 0) {
     return(1);
  }

  return(0);
}

//*****************************************************************

int ibp_compare_rid(rid_t rid1, rid_t rid2)
{
  return(strncmp(rid1.name, rid2.name, RID_LEN));
}




