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

#define _log_module_index 148

#include "ex3_abstract.h"

data_service_fn_t *_ds_default = NULL;
resource_service_fn_t *_rs_default = NULL;
object_service_fn_t *_os_default = NULL;


int ex3_set_default_ds(data_service_fn_t *ds)
{
    _ds_default = ds;
    return(0);
}
data_service_fn_t *ex3_get_default_ds()
{
    return(_ds_default);
}
int ex3_set_default_rs(resource_service_fn_t *rs)
{
    _rs_default = rs;
    return(0);
}
resource_service_fn_t *ex3_get_default_rs()
{
    return(_rs_default);
}
int ex3_set_default_os(object_service_fn_t *os)
{
    _os_default = os;
    return(0);
}
object_service_fn_t *ex3_get_default_os()
{
    return(_os_default);
}


