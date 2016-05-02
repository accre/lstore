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

//*********************************************************************
//*********************************************************************

#define _log_module_index 117

#include "network.h"
#include "net_sock.h"

//@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//   NOTE: Not fully implemented!!!! Currently it just wraps to using standard
//   sockets
//@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@


//*********************************************************************
// ns_config_2_sock - Configure the connection to use a single SSL socket
//*********************************************************************

void ns_config_2_ssl(tbx_ns_t *ns, int fd, int tcpsize)
{
    ns_config_sock(ns, tcpsize);
}

