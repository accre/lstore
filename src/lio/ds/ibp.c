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
// IBP based data service
//***********************************************************************

#define _log_module_index 146

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <ibp/op.h>
#include <ibp/protocol.h>
#include <ibp/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/chksum.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "ds.h"
#include "ds/ibp.h"
#include "ex3/types.h"

int _ds_ibp_do_init = 1;

//***********************************************************************
// ds_ibp_destroy_* - Simple destroy routines
//***********************************************************************

void ds_ibp_destroy_probe(lio_data_service_fn_t *arg, data_probe_t *probe)
{
    free(probe);
}

void ds_ibp_destroy_attr(lio_data_service_fn_t *arg, data_attr_t *attr)
{
    free(attr);
}


//***********************************************************************
// ds_ibp_new_cap_set - Creates a new capability set
//***********************************************************************

data_cap_set_t *ds_ibp_new_cap_set(lio_data_service_fn_t *arg)
{
    return((data_cap_set_t *)ibp_capset_new());
}

//***********************************************************************
// ds_ibp_destroy_cap_set - Destroys a cap set
//***********************************************************************

void ds_ibp_destroy_cap_set(lio_data_service_fn_t *arg, data_cap_set_t *dcs, int free_caps)
{
    ibp_capset_t *cs = (ibp_capset_t *)dcs;

    if (free_caps > 0) {
        ibp_cap_destroyset(cs);
    } else {
        free(cs);
    }
}

//***********************************************************************
// ds_ibp_cap_auto_warm - Adds the cap to the auto warming list
//***********************************************************************

void *ds_ibp_cap_auto_warm(lio_data_service_fn_t *arg, data_cap_set_t *dcs)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)arg->priv;
    ibp_capset_t *cs = (ibp_capset_t *)dcs;
    ibp_capset_t *w;

    log_printf(15, "Adding to auto warm cap: %s\n", cs->manageCap);

    //** Make the new cap
    w = ibp_capset_new();FATAL_UNLESS(w != NULL);
    if (cs->readCap) w->readCap = strdup(cs->readCap);
    if (cs->writeCap) w->writeCap = strdup(cs->writeCap);
    if (cs->manageCap) w->manageCap = strdup(cs->manageCap);

    //** Add it to the warming list
    apr_thread_mutex_lock(ds->lock);
    apr_hash_set(ds->warm_table, w->manageCap, APR_HASH_KEY_STRING, w);
    apr_thread_mutex_unlock(ds->lock);

    return(w);
}

//***********************************************************************
// ds_cap_stop_warm - Disables the cap from being warmed
//***********************************************************************

void ds_ibp_cap_stop_warm(lio_data_service_fn_t *arg, void *dcs)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)arg->priv;
    ibp_capset_t *cs = (ibp_capset_t *)dcs;

    if (cs == NULL) return;

    //** Remove it from the list
    apr_thread_mutex_lock(ds->lock);
    apr_hash_set(ds->warm_table, cs->manageCap, APR_HASH_KEY_STRING, NULL);

    log_printf(15, "Removing from auto warm: nkeys=%ud  cap: %s\n", apr_hash_count(ds->warm_table), cs->manageCap);
    apr_thread_mutex_unlock(ds->lock);


    //** Destroy the cap
    ds_ibp_destroy_cap_set(arg, (data_cap_set_t *)dcs, 1);
}

//***********************************************************************
// ds_ibp_get_cap - Returns a specific cap from the set
//***********************************************************************

data_cap_t *ds_ibp_get_cap(lio_data_service_fn_t *arg, data_cap_set_t *dcs, int key)
{
    ibp_capset_t *cs = (ibp_capset_t *)dcs;
    ibp_cap_t *cap = NULL;

    switch (key) {
    case DS_CAP_READ:
        cap = cs->readCap;
        break;
    case DS_CAP_WRITE:
        cap = cs->writeCap;
        break;
    case DS_CAP_MANAGE:
        cap = cs->manageCap;
        break;
    }

    return((void *)cap);
}

//***********************************************************************
// ds_ibp_set_cap - Sets a particular cap
//***********************************************************************

int ds_ibp_set_cap(lio_data_service_fn_t *arg, data_cap_set_t *dcs, int key, data_cap_t *cap)
{
    ibp_capset_t *cs = (ibp_capset_t *)dcs;
    int err = 0;

    switch (key) {
    case DS_CAP_READ:
        cs->readCap = cap;
        break;
    case DS_CAP_WRITE:
        cs->writeCap = cap;
        break;
    case DS_CAP_MANAGE:
        cs->manageCap = cap;
        break;
    default:
        err = 1;
    }

    return(err);
}

//***********************************************************************
//  ds_ibp_translate_cap_set - Translates the capability if needed
//***********************************************************************

void ds_ibp_translate_cap_set(lio_data_service_fn_t *ds, char *rid_key, char *ds_key, data_cap_set_t *dcs)
{
    ibp_capset_t *cs = (ibp_capset_t *)dcs;
    char ds_new[1024], new_cap[2048];
    char *str;
    int i, start, rid_start, end, len;

    log_printf(20, "START rcap=%s\n", cs->readCap);

    //** Make sure we have a cap to work with
    str = ((cs->readCap == NULL) ? ((cs->writeCap == NULL) ? cs->manageCap : cs->writeCap) : cs->readCap);
    if (str == NULL) return;

    //** Check if we need to translate anything
    //** Skip the preamble
    start = rid_start = end = -1;
    for (i=0; str[i] != '\0'; i++) {
        if (str[i] == ':') {
            if (str[i+1] == '/') {
                if (str[i+2] == '/') start = i+3;
            }
            break;
        }
    }

    log_printf(20, "cap=%s start=%d\n", str, start);

    if ( start == -1) return;

    //** Read the hostname:port/rid#
    for (i=start; str[i] != '\0'; i++) {
        if (str[i] == '#') {
            end = i;
            break;
        }
    }

    log_printf(20, "cap=%s end=%d\n", str, end);

    if (end == -1) return;

    len = end - start;
    log_printf(20, "cap=%s ds_key=%s strncmp=%d len=%d strlen(ds_key)=%lu\n", str, ds_key, strncmp(ds_key, &(str[start]), len), len, strlen(ds_key));
    if ((strncmp(ds_key, &(str[start]), len) == 0) && ((int)strlen(ds_key) == len)) return;

    //** If we made it hear we need to do a translation
    //** Make the new prefix
    snprintf(ds_new, sizeof(ds_new), "ibp://%s", ds_key);

    log_printf(20, "cap=%s prefix=%s\n", str, ds_new);

    //** Now do the actual translation
    if (cs->readCap != NULL) {
        snprintf(new_cap, sizeof(new_cap), "ibp://%s%s", ds_key, &(cs->readCap[end]));
        log_printf(20, "rcap_old=%s new=%s\n", cs->readCap, new_cap);
        free(cs->readCap);
        cs->readCap = strdup(new_cap);

    }

    if (cs->writeCap != NULL) {
        snprintf(new_cap, sizeof(new_cap), "ibp://%s%s", ds_key, &(cs->writeCap[end]));
        free(cs->writeCap);
        cs->writeCap = strdup(new_cap);
    }

    if (cs->manageCap != NULL) {
        snprintf(new_cap, sizeof(new_cap), "ibp://%s%s", ds_key, &(cs->manageCap[end]));
        free(cs->manageCap);
        cs->manageCap = strdup(new_cap);
    }
}


//***********************************************************************
// ds_ibp_new_attr - Creates a new attributes structure
//***********************************************************************

data_attr_t *ds_ibp_new_attr(lio_data_service_fn_t *arg)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)arg->priv;

    lio_ds_ibp_attr_t *a;

    tbx_type_malloc(a, lio_ds_ibp_attr_t, 1);

    *a = ds->attr_default;

    return((data_attr_t *)a);
}

//***********************************************************************
// ds_ibp_get_attr - Returns a specific attribute
//***********************************************************************

int ds_ibp_get_attr(lio_data_service_fn_t *arg, data_attr_t *dsa, int key, void *val, int size)
{
    lio_ds_ibp_attr_t *a = (lio_ds_ibp_attr_t *)dsa;
    ds_int_t *n = (ds_int_t *)val;
    ibp_depot_t *depot = (ibp_depot_t *)val;
    ibp_connect_context_t *cc = (ibp_connect_context_t *)val;
    tbx_ns_chksum_t *ncs = (tbx_ns_chksum_t *)val;
    int err = 0;

    switch (key) {
    case DS_ATTR_DURATION:
        if (size < (int)sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
        } else {
            *n = a->attr.duration;
        }
        break;
    case DS_IBP_ATTR_RELIABILITY:
        if (size < (int)sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
        } else {
            *n = a->attr.reliability;
        }
        break;
    case DS_IBP_ATTR_TYPE:
        if (size < (int)sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
        } else {
            *n = a->attr.type;
        }
        break;
    case DS_IBP_ATTR_DEPOT:
        if (size < (int)sizeof(ibp_depot_t)) {
            err = sizeof(ibp_depot_t);
        } else {
            *depot = a->depot;
        }
        break;
    case DS_IBP_ATTR_CC :
        if (size < (int)sizeof(ibp_connect_context_t)) {
            err = sizeof(ibp_connect_context_t);
        } else {
            *cc = a->cc;
        }
        break;
    case DS_IBP_ATTR_NET_CKSUM:
        if (size < (int)sizeof(tbx_ns_chksum_t)) {
            err = sizeof(tbx_ns_chksum_t);
        } else {
            *ncs = a->ncs;
        }
        break;
    case DS_IBP_ATTR_DISK_CHKSUM_TYPE:
        if (size < (int)sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
        } else {
            *n = a->disk_cs_type;
        }
        break;
    case DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE:
        if (size < (int)sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
        } else {
            *n = a->disk_cs_blocksize;
        }
        break;
    default:
        err = -1;
    }

    return(err);
}

//***********************************************************************
// ds_ibp_set_attr - Sets a specific attribute
//***********************************************************************

int ds_ibp_set_attr(lio_data_service_fn_t *arg, data_attr_t *dsa, int key, void *val)
{
    lio_ds_ibp_attr_t *a = (lio_ds_ibp_attr_t *)dsa;
    ds_int_t *n = (ds_int_t *)val;
    int err = 0;

    switch (key) {
    case DS_ATTR_DURATION:
        a->attr.duration = *n;
        break;
    case DS_IBP_ATTR_RELIABILITY:
        a->attr.reliability = *n;
        break;
    case DS_IBP_ATTR_TYPE:
        a->attr.type = *n;
        break;
    case DS_IBP_ATTR_CC :
        a->cc = *((ibp_connect_context_t *)val);
        break;
    case DS_IBP_ATTR_NET_CKSUM:
        a->ncs = *((tbx_ns_chksum_t *)val);
        break;
    case DS_IBP_ATTR_DISK_CHKSUM_TYPE:
        a->disk_cs_type = *((int *)val);
        break;
    case DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE:
        a->disk_cs_blocksize = *((int *)val);
        break;
    case DS_IBP_ATTR_DEPOT:
        a->depot = *((ibp_depot_t *)val);
        break;
    default:
        err = -1;
    }

    return(err);
}


//***********************************************************************
// ds_ibp_new_probe - Creates a new probe structure
//***********************************************************************

data_cap_set_t *ds_ibp_new_probe(lio_data_service_fn_t *arg)
{
    void *p = malloc(sizeof(ibp_capstatus_t));

    memset(p, 0, sizeof(ibp_capstatus_t));

    return(p);
}

//***********************************************************************
// ds_ibp_get_probe - Returns a specific probably value
//***********************************************************************

int ds_ibp_get_probe(lio_data_service_fn_t *arg, data_attr_t *dsa, int key, void *val, int size)
{
    ibp_capstatus_t *p = (ibp_capstatus_t *)dsa;
    ds_int_t *n = (ds_int_t *)val;
    int err = 0;

    if (size < (int)sizeof(ds_int_t))  return(sizeof(ds_int_t));

    switch (key) {
    case DS_PROBE_DURATION:
        *n = p->attrib.duration;
        break;
    case DS_PROBE_READ_COUNT:
        *n = p->readRefCount;
        break;
    case DS_PROBE_WRITE_COUNT:
        *n = p->writeRefCount;
        break;
    case DS_PROBE_CURR_SIZE:
        *n = p->currentSize;
        break;
    case DS_PROBE_MAX_SIZE:
        *n = p->maxSize;
        break;
    default:
        err = -1;
    }

    return(err);
}

//***********************************************************************
// res2ibp Converts a Resource string to an IBP depot
//  string format: host:port:rid
//***********************************************************************

int res2ibp(char *res, ibp_depot_t *depot)
{
    char *str = strdup(res);
    char *bstate, *tmp;
    int fin;

    strncpy(depot->host, tbx_stk_string_token(str, ":", &bstate, &fin), sizeof(depot->host)-1);
    depot->host[sizeof(depot->host)-1] = '\0';
    tmp = tbx_stk_string_token(NULL, "/", &bstate, &fin);
    depot->port = atoi(tmp);
    strncpy(depot->rid.name, tbx_stk_string_token(NULL, ":/", &bstate, &fin), sizeof(depot->rid.name)-1);
    depot->rid.name[sizeof(depot->rid.name)-1] = '\0';

    log_printf(15, "ds_key=%s host=%s port=%d rid=%s\n", res, depot->host, depot->port, depot->rid.name);
    free(str);

    return(0);
}

//***********************************************************************
//  ds_ibp_res2rid - Extracts the RID info from the resource
//***********************************************************************

char *ds_ibp_res2rid(lio_data_service_fn_t *dsf, char *res)
{
    ibp_depot_t depot;

    res2ibp(res, &depot);

    return(strdup(depot.rid.name));
}

//***********************************************************************
// ds_ibp_new_inquire - Creates a new inquire structure
//***********************************************************************

data_inquire_t *ds_ibp_new_inquire(lio_data_service_fn_t *arg)
{
    ibp_depotinfo_t *d;

    tbx_type_malloc_clear(d, ibp_depotinfo_t, 1);

    return(d);
}

//***********************************************************************
// ds_ibp_destroy_inquire - Destroys an inquire structure
//***********************************************************************

void ds_ibp_destroy_inquire(lio_data_service_fn_t *arg, data_inquire_t *di)
{
    free(di);
}


//***********************************************************************
//  ds_ibp_res2rid - Extracts the RID info from the resource
//***********************************************************************

ds_int_t ds_ibp_res_inquire_get(lio_data_service_fn_t *dsf, int type, data_inquire_t *space)
{
    ds_int_t value;
    ibp_depotinfo_t *d = (data_inquire_t *)space;

    value = -1;
    switch(type) {
    case DS_INQUIRE_USED:
        value = d->TotalUsed;
        break;
    case DS_INQUIRE_FREE:
        value = d->SoftAllocable;
        break;
    case DS_INQUIRE_TOTAL:
        value = d->TotalServed;
        break;
    }

    return(value);
}

//***********************************************************************
//  ds_ibp_op_create - Creates a new CB for the opque list
//***********************************************************************

lio_ds_ibp_op_t *ds_ibp_op_create(lio_ds_ibp_priv_t *ds, lio_ds_ibp_attr_t *attr)
{
//  lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    lio_ds_ibp_op_t *iop;

    tbx_type_malloc_clear(iop, lio_ds_ibp_op_t, 1);

    iop->attr = (attr == NULL) ? &(ds->attr_default) : attr;
    return(iop);
}

//***********************************************************************
// _ds_ibp_op_free - Frees the calling structure
//***********************************************************************

void _ds_ibp_op_free(gop_op_generic_t *gop, int mode)
{
    lio_ds_ibp_op_t *iop = gop->free_ptr;

//log_printf(0, "Freeing gid=%d\n", gop_id(gop));
//tbx_log_flush();

    //** Call the original cleanup routine
    gop->free_ptr = iop->free_ptr;
    iop->free(gop, mode);

    if (mode == OP_DESTROY) free(iop);
}

//***********************************************************************
//  ds_ibp_setup_finish - Finishes iop setup
//***********************************************************************

void ds_ibp_setup_finish(lio_ds_ibp_op_t *iop)
{
    if (ibp_cc_type(&(iop->attr->cc)) != NS_TYPE_UNKNOWN) ibp_op_cc_set(iop->gop, &(iop->attr->cc));

    iop->free = iop->gop->base.free;
    iop->free_ptr = iop->gop->free_ptr;

    iop->gop->base.free = _ds_ibp_op_free;
    iop->gop->free_ptr = iop;
}

//***********************************************************************
// ds_ibp_res_inqure - Generates a resource inquiry operation
//***********************************************************************

gop_op_generic_t *ds_ibp_res_inquire(lio_data_service_fn_t *dsf, char *res, data_attr_t *dattr, data_inquire_t *space, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;
    lio_ds_ibp_alloc_op_t *cmd;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
    cmd = &(iop->ops.alloc);

    //** Fill in the depot structure
    res2ibp(res, &(cmd->depot));

    //** Create the op
    iop->gop = ibp_depot_inq_op(ds->ic, &(cmd->depot), "ibp", space, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_allocate - Makes an IBP allocation operation
//***********************************************************************

gop_op_generic_t *ds_ibp_allocate(lio_data_service_fn_t *dsf, char *res, data_attr_t *dattr, ds_int_t size, data_cap_set_t *caps, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;
    lio_ds_ibp_alloc_op_t *cmd;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
    cmd = &(iop->ops.alloc);

    //** Fill in the depot structure
    res2ibp(res, &(cmd->depot));

    //** Create the op
    iop->gop = ibp_alloc_op(ds->ic, caps, size, &(cmd->depot), &(iop->attr->attr), iop->attr->disk_cs_type, iop->attr->disk_cs_blocksize, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}


//***********************************************************************
// ds_ibp_remove - Decrements the allocations ref count
//***********************************************************************

gop_op_generic_t *ds_ibp_remove(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *cap, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_remove_op(ds->ic, cap, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_truncate - Adjusts the allocations size.
//***********************************************************************

gop_op_generic_t *ds_ibp_truncate(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, ex_off_t new_size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_truncate_op(ds->ic, mcap, new_size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_modify_count - Decrements the allocations ref count
//***********************************************************************

gop_op_generic_t *ds_ibp_modify_count(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, int mode, int captype, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;
    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
    int imode, icaptype;

    switch (mode) {
    case (DS_MODE_INCR) :
        imode = IBP_INCR;
        break;
    case (DS_MODE_DECR) :
        imode = IBP_DECR;
        break;
    default:
        log_printf(0, "ds_ibp_modify_count: invalid mode! mode=%d\n", mode);
        return(NULL);
    }

    switch (captype) {
    case (DS_CAP_READ) :
        icaptype = IBP_READCAP;
        break;
    case (DS_CAP_WRITE) :
        icaptype = IBP_WRITECAP;
        break;
    default:
        log_printf(0, "ds_ibp_modify_count: invalid captype! captype=%d\n", captype);
        return(NULL);
    }

    //** Create the op
    iop->gop = ibp_modify_count_op(ds->ic, mcap, imode, icaptype, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_probe - Generates an allocation probe operation
//***********************************************************************

gop_op_generic_t *ds_ibp_probe(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, data_probe_t *probe, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_probe_op(ds->ic, mcap, (ibp_capstatus_t *)probe, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_read - Generates a read operation
//***********************************************************************

gop_op_generic_t *ds_ibp_read(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *rcap, ds_int_t off, tbx_tbuf_t *dread, ds_int_t droff, ds_int_t size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_read_op(ds->ic, rcap, off, dread, droff, size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_write - Generates a write operation
//***********************************************************************

gop_op_generic_t *ds_ibp_write(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *wcap, ds_int_t off, tbx_tbuf_t *dwrite, ds_int_t boff, ds_int_t size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_write_op(ds->ic, wcap, off, dwrite, boff, size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_readv - Generates a vec read operation
//***********************************************************************

gop_op_generic_t *ds_ibp_readv(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *rcap, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *dread, ds_int_t droff, ds_int_t size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_vec_read_op(ds->ic, rcap, n_iov, iov, dread, droff, size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_writev - Generates a Vec write operation
//***********************************************************************

gop_op_generic_t *ds_ibp_writev(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *wcap, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *dwrite, ds_int_t boff, ds_int_t size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_vec_write_op(ds->ic, wcap, n_iov, iov, dwrite, boff, size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_append - Generates a write append operation
//***********************************************************************

gop_op_generic_t *ds_ibp_append(lio_data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *wcap, tbx_tbuf_t *dwrite, ds_int_t boff, ds_int_t size, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;

    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

    //** Create the op
    iop->gop = ibp_append_op(ds->ic, wcap, dwrite, boff, size, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}

//***********************************************************************
// ds_ibp_copy - Generates a depot-depot copy operation
//***********************************************************************

gop_op_generic_t *ds_ibp_copy(lio_data_service_fn_t *dsf, data_attr_t *dattr, int mode, int ns_type, char *ppath, data_cap_t *src_cap, ds_int_t src_off,
                          data_cap_t *dest_cap, ds_int_t dest_off, ds_int_t len, int timeout)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)(dsf->priv);
    lio_ds_ibp_attr_t *attr = (lio_ds_ibp_attr_t *)dattr;
    lio_ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
    int dir;

    //** Create the op
    dir = ((mode & DS_PULL) > 0) ? IBP_PULL : IBP_PUSH;
    iop->gop = ibp_copy_op(ds->ic, dir, ns_type, ppath, src_cap, dest_cap, src_off, dest_off, len, timeout, timeout, timeout);

    ds_ibp_setup_finish(iop);

    return(iop->gop);
}


//***********************************************************************
// ds_ibp_set_default_attr - Configures the default attributes for the ds
//***********************************************************************

int ds_ibp_set_default_attr(lio_data_service_fn_t *dsf, data_attr_t *da)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    lio_ds_ibp_attr_t *ida = (lio_ds_ibp_attr_t *)da;

    ds->attr_default = *ida;

    return(0);
}

//***********************************************************************
//  ds_ibp_get_default - Retreive the pointer to the global default attributes
//***********************************************************************

int ds_ibp_get_default_attr(lio_data_service_fn_t *dsf, data_attr_t *da)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    lio_ds_ibp_attr_t *ida = (lio_ds_ibp_attr_t *)da;

    *ida = ds->attr_default;

    return(0);
}

//***********************************************************************
// ds_ibp_warm_thread - IBP warmer thread for active files
//***********************************************************************

void *ds_ibp_warm_thread(apr_thread_t *th, void *data)
{
    lio_data_service_fn_t *dsf = (lio_data_service_fn_t *)data;
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    apr_time_t max_wait;
    char *mcap;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    ibp_capset_t *w;
    gop_opque_t *q;
    int dt, err;

    dt = 60;
    max_wait = apr_time_make(ds->warm_interval, 0);
    apr_thread_mutex_lock(ds->lock);
    while (ds->warm_stop == 0) {
        //** Generate all the tasks
        log_printf(10, "Starting auto-warming run\n");

        q = gop_opque_new();
        for (hi=apr_hash_first(NULL, ds->warm_table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&mcap, &hlen, (void **)&w);
            gop_opque_add(q, ibp_modify_alloc_op(ds->ic, mcap, -1, ds->warm_duration, -1, dt));
            log_printf(15, " warming: %s\n", mcap);

        }

        //** Wait until they all complete
        err = opque_waitall(q);
        log_printf(10, "opque_waitall=%d\n", err);

        gop_opque_free(q, OP_DESTROY);  //** Clean up.  Don;t care if we are successfull or not:)

        //** Sleep until the next time or we get an exit request
        apr_thread_cond_timedwait(ds->cond, ds->lock, max_wait);
    }
    apr_thread_mutex_unlock(ds->lock);

    log_printf(10, "EXITING auto-warm thread\n");

    return(NULL);
}

//***********************************************************************
//  ds_ibp_destroy - Creates the IBP data service
//***********************************************************************

void ds_ibp_destroy(lio_data_service_fn_t *dsf)
{
    lio_ds_ibp_priv_t *ds = (lio_ds_ibp_priv_t *)dsf->priv;
    apr_status_t value;

    //** Wait for the warmer thread to complete
    apr_thread_mutex_lock(ds->lock);
    ds->warm_stop = 1;
    apr_thread_cond_signal(ds->cond);
    apr_thread_mutex_unlock(ds->lock);
    apr_thread_join(&value, ds->thread);  //** Wait for it to complete

    //** Now we can clean up
    apr_thread_mutex_destroy(ds->lock);
    apr_thread_cond_destroy(ds->cond);
    apr_pool_destroy(ds->pool);

    ibp_context_destroy(ds->ic);

    free(ds);
    free(dsf);
}


//***********************************************************************
//  ds_ibp_create - Creates the IBP data service
//***********************************************************************

lio_data_service_fn_t *ds_ibp_create(void *arg, tbx_inip_file_t *ifd, char *section)
{
    int cs_type;
    lio_data_service_fn_t *dsf;
    lio_ds_ibp_priv_t *ds;
    ibp_context_t *ic;

    tbx_type_malloc_clear(dsf, lio_data_service_fn_t, 1);
    tbx_type_malloc_clear(ds, lio_ds_ibp_priv_t , 1);

    //** Set the default attributes
    memset(&(ds->attr_default), 0, sizeof(lio_ds_ibp_attr_t));
    ds->attr_default.attr.duration = tbx_inip_get_integer(ifd, section, "duration", 3600);

    ds->warm_duration = ds->attr_default.attr.duration;
    ds->warm_interval = 0.33 * ds->warm_duration;
    ds->warm_interval = tbx_inip_get_integer(ifd, section, "warm_interval", ds->warm_interval);
    ds->warm_duration = tbx_inip_get_integer(ifd, section, "warm_duration", ds->warm_duration);

    cs_type = tbx_inip_get_integer(ifd, section, "chksum_type", CHKSUM_DEFAULT);
    if ( ! ((tbx_chksum_type_valid(cs_type) == 0) || (cs_type == CHKSUM_DEFAULT) || (cs_type == CHKSUM_NONE)))  {
        log_printf(0, "Invalid chksum type=%d resetting to CHKSUM_DEFAULT(%d)\n", cs_type, CHKSUM_DEFAULT);
        cs_type = CHKSUM_DEFAULT;
    }
    ds->attr_default.disk_cs_type = cs_type;

    ds->attr_default.disk_cs_blocksize = tbx_inip_get_integer(ifd, section, "chksum_blocksize", 64*1024);
    if (ds->attr_default.disk_cs_blocksize <= 0) {
        log_printf(0, "Invalid chksum blocksize=%d resetting to %d\n", ds->attr_default.disk_cs_blocksize, 64*1024);
        ds->attr_default.disk_cs_blocksize = 64 *1024;
    }
    ds->attr_default.attr.reliability = IBP_HARD;
    ds->attr_default.attr.type = IBP_BYTEARRAY;


    ic = ibp_context_create();
    ibp_config_load(ic, ifd, section);
    ds->ic = ic;
    dsf->type = DS_TYPE_IBP;

    dsf->priv = (void *)ds;
    dsf->destroy_service = ds_ibp_destroy;
    dsf->new_cap_set = ds_ibp_new_cap_set;
    dsf->cap_auto_warm = ds_ibp_cap_auto_warm;
    dsf->cap_stop_warm = ds_ibp_cap_stop_warm;
    dsf->get_cap = ds_ibp_get_cap;
    dsf->set_cap = ds_ibp_set_cap;
    dsf->translate_cap_set = ds_ibp_translate_cap_set;
    dsf->destroy_cap_set = ds_ibp_destroy_cap_set;
    dsf->new_probe = ds_ibp_new_probe;
    dsf->destroy_probe = ds_ibp_destroy_probe;
    dsf->get_probe = ds_ibp_get_probe;
    dsf->new_attr = ds_ibp_new_attr;
    dsf->destroy_attr = ds_ibp_destroy_attr;
    dsf->set_attr = ds_ibp_set_attr;
    dsf->get_attr = ds_ibp_get_attr;
    dsf->set_default_attr = ds_ibp_set_default_attr;
    dsf->get_default_attr = ds_ibp_get_default_attr;
    dsf->res2rid = ds_ibp_res2rid;
    dsf->new_inquire = ds_ibp_new_inquire;
    dsf->destroy_inquire = ds_ibp_destroy_inquire;
    dsf->res_inquire_get = ds_ibp_res_inquire_get;
    dsf->res_inquire = ds_ibp_res_inquire;
    dsf->allocate = ds_ibp_allocate;
    dsf->remove = ds_ibp_remove;
    dsf->modify_count = ds_ibp_modify_count;
    dsf->read = ds_ibp_read;
    dsf->write = ds_ibp_write;
    dsf->readv = ds_ibp_readv;
    dsf->writev = ds_ibp_writev;
    dsf->append = ds_ibp_append;
    dsf->copy = ds_ibp_copy;
    dsf->probe = ds_ibp_probe;
    dsf->truncate = ds_ibp_truncate;

    //** Launch the warmer
    assert_result(apr_pool_create(&(ds->pool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(ds->lock), APR_THREAD_MUTEX_DEFAULT, ds->pool);
    apr_thread_cond_create(&(ds->cond), ds->pool);
    ds->warm_table = apr_hash_make(ds->pool);
    tbx_thread_create_assert(&(ds->thread), NULL, ds_ibp_warm_thread, (void *)dsf, ds->pool);

    return(dsf);
}


