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

#include "ibp_server.h"

//**************Global control Variables*****************
apr_thread_mutex_t *shutdown_lock = NULL;
apr_thread_cond_t *shutdown_cond = NULL;
apr_pool_t         *global_pool = NULL;
int shutdown_now;
Config_t *global_config;
ibp_task_t *global_task;

