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

#include "envelope.h"
#include <tbx/network.h>

int envelope_send(tbx_ns_t *ns, envelope_t *env);
int envelope_recv(tbx_ns_t *ns, envelope_t *env);
int envelope_simple_send(tbx_ns_t *ns, uint32_t cmd);
int envelope_simple_recv(tbx_ns_t *ns, uint32_t *cmd);


