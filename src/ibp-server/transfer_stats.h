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

//************************************************************
// Definition for gathering depot statistics
//************************************************************

#define IP_LEN 24  //Just store the decimal form

#define DIR_IN  0
#define DIR_OUT 1
#define DEPOT_COPY_OUT 2

typedef struct {
  int          id;                 //** Command id
  int          start;              //** Command start time
  int          end;                //** Command start time
  int          nbytes;             //** Amount of data transfered
  int          dir;               //** Direction of traffic
  char         address[IP_LEN];    //** IP address of command
} Transfer_stat_t;


