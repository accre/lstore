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
#include <tbx/transfer_buffer.h>

#include "ex3_types.h"

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

typedef int64_t ds_int_t;
typedef void data_attr_t;
typedef void data_cap_set_t;
typedef void data_cap_t;
typedef void data_probe_t;
typedef void data_inquire_t;

#define ds_type(ds) (ds)->type
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
#define ds_read(ds, attr, rcap, off, readfn, boff, len, to) (ds)->read(ds, attr, rcap, off, readfn, boff, len, to)
#define ds_write(ds, attr, wcap, off, writefn, boff, len, to) (ds)->write(ds, attr, wcap, off, writefn, boff, len, to)
#define ds_readv(ds, attr, rcap, n_vec, iov, readfn, boff, len, to) (ds)->readv(ds, attr, rcap, n_vec, iov, readfn, boff, len, to)
#define ds_writev(ds, attr, wcap, n_vec, iov, writefn, boff, len, to) (ds)->writev(ds, attr, wcap, n_vec, iov, writefn, boff, len, to)
#define ds_append(ds, attr, wcap, writefn, boff, len, to) (ds)->append(ds, attr, wcap, writefn, boff, len, to)
#define ds_copy(ds, attr, mode, ns_type, ppath, src_cap, src_off, dest_cap, dest_off, len, to) \
              (ds)->copy(ds, attr, mode, ns_type, ppath, src_cap, src_off, dest_cap, dest_off, len, to)

typedef struct data_service_fn_t data_service_fn_t;
struct data_service_fn_t {
    void *priv;
    char *type;
    void (*destroy_service)(data_service_fn_t *);
    void (*translate_cap_set)(data_service_fn_t *ds, char *rid_key, char *ds_key, data_cap_set_t *cs);
    data_cap_set_t *(*new_cap_set)(data_service_fn_t *);
    void *(*cap_auto_warm)(data_service_fn_t *, data_cap_set_t *dcs);
    void (*cap_stop_warm)(data_service_fn_t *, void *warm);
    data_cap_t *(*get_cap)(data_service_fn_t *, data_cap_set_t *cs, int key);
    int (*set_cap)(data_service_fn_t *, data_cap_set_t *cs, int key, data_cap_t *cap);
    void (*destroy_cap_set)(data_service_fn_t *, data_cap_set_t *caps, int free_cap);
    data_attr_t *(*new_attr)(data_service_fn_t *);
    int (*get_attr)(data_service_fn_t *, data_attr_t *attr, int key, void *val, int size);
    int (*set_attr)(data_service_fn_t *, data_attr_t *attr, int key, void *val);
    void (*destroy_attr)(data_service_fn_t *, data_attr_t *attr);
    int (*get_default_attr)(data_service_fn_t *, data_attr_t *attr);
    int (*set_default_attr)(data_service_fn_t *, data_attr_t *attr);
    data_probe_t *(*new_probe)(data_service_fn_t *);
    int (*get_probe)(data_service_fn_t *, data_probe_t *probe, int key, void *val, int size);
    void (*destroy_probe)(data_service_fn_t *, data_probe_t *probe);
    char *(*res2rid)(data_service_fn_t *ds, char *ds_key);
    data_inquire_t *(*new_inquire)(data_service_fn_t *ds);
    void (*destroy_inquire)(data_service_fn_t *ds, data_inquire_t *space);
    ds_int_t (*res_inquire_get)(data_service_fn_t *ds, int type, data_inquire_t *space);
    op_generic_t *(*res_inquire)(data_service_fn_t *, char *res, data_attr_t *attr, data_inquire_t *space, int timeout);
    op_generic_t *(*allocate)(data_service_fn_t *, char *res, data_attr_t *attr, ds_int_t size, data_cap_set_t *caps, int timeout);
    op_generic_t *(*remove)(data_service_fn_t *, data_attr_t *dattr, data_cap_t *mcap, int timeout);
    op_generic_t *(*truncate)(data_service_fn_t *, data_attr_t *dattr, data_cap_t *mcap, ex_off_t new_size, int timeout);
    op_generic_t *(*probe)(data_service_fn_t *, data_attr_t *dattr, data_cap_t *mcap, data_probe_t *probe, int timeout);
    op_generic_t *(*modify_count)(data_service_fn_t *, data_attr_t *dattr, data_cap_t *mcap, int mode, int captype, int timeout);
    op_generic_t *(*read)(data_service_fn_t *, data_attr_t *attr, data_cap_t *rcap, ds_int_t off, tbx_tbuf_t *read, ex_off_t boff, ex_off_t len, int timeout);
    op_generic_t *(*write)(data_service_fn_t *, data_attr_t *attr, data_cap_t *wcap, ds_int_t off, tbx_tbuf_t *write, ex_off_t boff, ex_off_t len, int timeout);
    op_generic_t *(*readv)(data_service_fn_t *, data_attr_t *attr, data_cap_t *rcap, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *read, ex_off_t boff, ex_off_t len, int timeout);
    op_generic_t *(*writev)(data_service_fn_t *, data_attr_t *attr, data_cap_t *wcap, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *write, ex_off_t boff, ex_off_t len, int timeout);
    op_generic_t *(*append)(data_service_fn_t *, data_attr_t *attr, data_cap_t *wcap, tbx_tbuf_t *write, ex_off_t boff, ex_off_t len, int timeout);
    op_generic_t *(*copy)(data_service_fn_t *, data_attr_t *attr, int mode, int ns_type, char *ppath, data_cap_t *src_cap, ds_int_t src_off,
                          data_cap_t *dest_cap, ds_int_t dest_off, ds_int_t len, int timeout);
};


#ifdef __cplusplus
}
#endif

#endif

