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


