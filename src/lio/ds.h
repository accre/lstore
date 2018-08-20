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

//***********************************************************************
// Generic data service
//***********************************************************************

#ifndef _DATA_SERVICE_H_
#define _DATA_SERVICE_H_

#include <gop/opque.h>
#include <lio/ds.h>
#include <tbx/transfer_buffer.h>

#include "ex3/types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DS_SM_AVAILABLE "ds_available"
#define DS_SM_RUNNING   "ds_running"

#define DS_ATTR_DURATION 1

#define DS_CAP_READ   1
#define DS_CAP_WRITE  2
#define DS_CAP_MANAGE 3

#define DS_MODE_INCR  1
#define DS_MODE_DECR  2

#define DS_PUSH 4
#define DS_PULL 8

#define DS_PROBE_DURATION    1
#define DS_PROBE_READ_COUNT  2
#define DS_PROBE_WRITE_COUNT 3
#define DS_PROBE_CURR_SIZE   4
#define DS_PROBE_MAX_SIZE    5

#define DS_INQUIRE_USED  1
#define DS_INQUIRE_FREE  2
#define DS_INQUIRE_TOTAL 3

#define ds_type(ds) (ds)->type
#define ds_print_running_config(ds, fd, psh) (ds)->print_running_config(ds, fd, psh)
#define ds_destroy_service(ds) (ds)->destroy_service(ds)
#define ds_cap_auto_warm(ds, cs) (ds)->cap_auto_warm(ds, cs)
#define ds_cap_stop_warm(ds, cs) (ds)->cap_stop_warm(ds, cs)
#define ds_cap_set_create(ds) (ds)->new_cap_set(ds)
#define ds_cap_set_create(ds) (ds)->new_cap_set(ds)
#define ds_cap_set_destroy(ds, cs, free_cap) (ds)->destroy_cap_set(ds, cs, free_cap)
#define ds_get_cap(ds, cs, key) (ds)->get_cap(ds, cs, key)
#define ds_set_cap(ds, cs, key, cap) (ds)->set_cap(ds, cs, key, cap)
#define ds_translate_cap_set(ds, rid_key, ds_key, cs) (ds)->translate_cap_set(ds, rid_key, ds_key, cs)
#define ds_attr_create(ds) (ds)->new_attr(ds)
#define ds_attr_destroy(ds, da) (ds)->destroy_attr(ds, da)
#define ds_get_attr(ds, attr, key, val, size) (ds)->get_attr(ds, attr, key, val, size)
#define ds_set_attr(ds, attr, key, val) (ds)->set_attr(ds, attr, key, val)
#define ds_get_default_attr(ds, attr) (ds)->get_attr(ds, attr)
#define ds_set_default_attr(ds, attr) (ds)->set_attr(ds, attr)
#define ds_probe_create(ds) (ds)->new_probe(ds)
#define ds_probe_destroy(ds, p) (ds)->destroy_probe(ds, p)
#define ds_get_probe(ds, p, key, val, size) (ds)->get_probe(ds, p, key, val, size)
#define ds_res2rid(ds, res) (ds)->res2rid(ds, res)
#define ds_inquire_create(ds) (ds)->new_inquire(ds)
#define ds_inquire_destroy(ds, space) (ds)->destroy_inquire(ds, space)
#define ds_res_inquire(ds, res, attr, space, to) (ds)->res_inquire(ds, res, attr, space, to)
#define ds_res_inquire_get(ds, type, space) (ds)->res_inquire_get(ds, type, space)
#define ds_allocate(ds, res, attr, size, cs, to) (ds)->allocate(ds, res, attr,size, cs, to)
#define ds_remove(ds, attr, mcap, to) (ds)->remove(ds, attr, mcap, to)
#define ds_truncate(ds, attr, mcap, new_size, to) (ds)->truncate(ds, attr, mcap, new_size, to)
#define ds_probe(ds, attr, mcap, p, to) (ds)->probe(ds, attr, mcap, p, to)
#define ds_modify_count(ds, attr, mcap, mode, captype, to) (ds)->modify_count(ds, attr, mcap, mode, captype, to)
#define ds_write(ds, attr, wcap, off, writefn, boff, len, to) (ds)->write(ds, attr, wcap, off, writefn, boff, len, to)
#define ds_readv(ds, attr, rcap, n_vec, iov, readfn, boff, len, to) (ds)->readv(ds, attr, rcap, n_vec, iov, readfn, boff, len, to)
#define ds_writev(ds, attr, wcap, n_vec, iov, writefn, boff, len, to) (ds)->writev(ds, attr, wcap, n_vec, iov, writefn, boff, len, to)
#define ds_append(ds, attr, wcap, writefn, boff, len, to) (ds)->append(ds, attr, wcap, writefn, boff, len, to)
#define ds_copy(ds, attr, mode, ns_type, ppath, src_cap, src_off, dest_cap, dest_off, len, to) \
              (ds)->copy(ds, attr, mode, ns_type, ppath, src_cap, src_off, dest_cap, dest_off, len, to)


#ifdef __cplusplus
}
#endif

#endif
