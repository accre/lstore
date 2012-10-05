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

//***********************************************************************
// IBP based data service
//***********************************************************************

#define _log_module_index 146

#include "type_malloc.h"
#include "log.h"
#include "data_service_abstract.h"
#include "ds_ibp_priv.h"
#include "ibp.h"
#include "opque.h"
#include "string_token.h"
#include "type_malloc.h"

int _ds_ibp_do_init = 1;

//***********************************************************************
// ds_ibp_destroy_* - Simple destroy routines
//***********************************************************************

void ds_ibp_destroy_probe(data_service_fn_t *arg, data_probe_t *probe)
{
  free(probe);
}

void ds_ibp_destroy_attr(data_service_fn_t *arg, data_attr_t *attr)
{
  free(attr);
}


//***********************************************************************
// ds_ibp_new_cap_set - Creates a new capability set
//***********************************************************************

data_cap_set_t *ds_ibp_new_cap_set(data_service_fn_t *arg)
{
  return((data_cap_set_t *)new_ibp_capset());
}

//***********************************************************************
// ds_ibp_destroy_cap_set - Destroys a cap set
//***********************************************************************

void ds_ibp_destroy_cap_set(data_service_fn_t *arg, data_cap_set_t *dcs, int free_caps)
{
  ibp_capset_t *cs = (ibp_capset_t *)dcs;

  if (free_caps > 0) {
    destroy_ibp_capset(cs);
  } else {
    free(cs);
  }
}

//***********************************************************************
// ds_ibp_get_cap - Returns a specific cap from the set
//***********************************************************************

data_cap_t *ds_ibp_get_cap(data_service_fn_t *arg, data_cap_set_t *dcs, int key)
{
  ibp_capset_t *cs = (ibp_capset_t *)dcs;
  ibp_cap_t *cap = NULL;

  switch (key) {
    case DS_CAP_READ: cap = cs->readCap;  break;
    case DS_CAP_WRITE: cap = cs->writeCap;  break;
    case DS_CAP_MANAGE: cap = cs->manageCap;  break;
  }

  return((void *)cap);
}

//***********************************************************************
// ds_ibp_set_cap - Sets a particular cap
//***********************************************************************

int ds_ibp_set_cap(data_service_fn_t *arg, data_cap_set_t *dcs, int key, data_cap_t *cap)
{
  ibp_capset_t *cs = (ibp_capset_t *)dcs;
  int err = 0;

  switch (key) {
    case DS_CAP_READ: cs->readCap = cap;  break;
    case DS_CAP_WRITE: cs->writeCap = cap;  break;
    case DS_CAP_MANAGE: cs->manageCap = cap;  break;
    default: err = 1;
  }

  return(err);
}

//***********************************************************************
// ds_ibp_new_attr - Creates a new attributes structure
//***********************************************************************

data_attr_t *ds_ibp_new_attr(data_service_fn_t *arg)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)arg->priv;
  
  ds_ibp_attr_t *a;

  type_malloc(a, ds_ibp_attr_t, 1);
  
  *a = ds->attr_default;
  
  return((data_attr_t *)a);
}

//***********************************************************************
// ds_ibp_get_attr - Returns a specific attribute
//***********************************************************************

int ds_ibp_get_attr(data_service_fn_t *arg, data_attr_t *dsa, int key, void *val, int size)
{
  ds_ibp_attr_t *a = (ds_ibp_attr_t *)dsa;
  ds_int_t *n = (ds_int_t *)val;
  ibp_depot_t *depot = (ibp_depot_t *)val;
  ibp_connect_context_t *cc = (ibp_connect_context_t *)val;
  ns_chksum_t *ncs = (ns_chksum_t *)val;  
  int err = 0;

  switch (key) {
    case DS_ATTR_DURATION: 
         if (size < sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
         } else {
            *n = a->attr.duration;
         }
         break;
    case DS_IBP_ATTR_RELIABILITY: 
         if (size < sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
         } else {
            *n = a->attr.reliability;
         }
         break;
    case DS_IBP_ATTR_TYPE: 
         if (size < sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
         } else {
            *n = a->attr.type;  
         }
         break;
    case DS_IBP_ATTR_DEPOT: 
         if (size < sizeof(ibp_depot_t)) {
            err = sizeof(ibp_depot_t);
         } else {
            *depot = a->depot;
         }
         break;
    case DS_IBP_ATTR_CC : 
         if (size < sizeof(ibp_connect_context_t)) {
            err = sizeof(ibp_connect_context_t);
         } else {
           *cc = a->cc;
         }
         break;
    case DS_IBP_ATTR_NET_CKSUM: 
         if (size < sizeof(ns_chksum_t)) {
            err = sizeof(ns_chksum_t);
         } else {
           *ncs = a->ncs;
         }
         break;
    case DS_IBP_ATTR_DISK_CHKSUM_TYPE: 
         if (size < sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
         } else {
            *n = a->disk_cs_type;  
         }
         break;
    case DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE: 
         if (size < sizeof(ds_int_t)) {
            err = sizeof(ds_int_t);
         } else {
            *n = a->disk_cs_blocksize;  
         }
         break;
    default: err = -1;
  }

  return(err);
}

//***********************************************************************
// ds_ibp_set_attr - Sets a specific attribute
//***********************************************************************

int ds_ibp_set_attr(data_service_fn_t *arg, data_attr_t *dsa, int key, void *val)
{
  ds_ibp_attr_t *a = (ds_ibp_attr_t *)dsa;
  ds_int_t *n = (ds_int_t *)val;
  int err = 0;

  switch (key) {
    case DS_ATTR_DURATION: a->attr.duration = *n;  break;
    case DS_IBP_ATTR_RELIABILITY: a->attr.reliability = *n;  break;
    case DS_IBP_ATTR_TYPE: a->attr.type = *n;  break;
    case DS_IBP_ATTR_CC : a->cc = *((ibp_connect_context_t *)val);  break;
    case DS_IBP_ATTR_NET_CKSUM: a->ncs = *((ns_chksum_t *)val);  break;
    case DS_IBP_ATTR_DISK_CHKSUM_TYPE: a->disk_cs_type = *((int *)val);  break;
    case DS_IBP_ATTR_DISK_CHKSUM_BLOCKSIZE: a->disk_cs_blocksize = *((int *)val);  break;
    case DS_IBP_ATTR_DEPOT: a->depot = *((ibp_depot_t *)val);  break;
    default: err = -1;
  }

  return(err);
}


//***********************************************************************
// ds_ibp_new_probe - Creates a new probe structure
//***********************************************************************

data_cap_set_t *ds_ibp_new_probe(data_service_fn_t *arg)
{
  void *p = malloc(sizeof(ibp_capstatus_t));
  
  memset(p, 0, sizeof(ibp_capstatus_t));
  
  return(p);
}

//***********************************************************************
// ds_ibp_get_probe - Returns a specific probably value
//***********************************************************************

int ds_ibp_get_probe(data_service_fn_t *arg, data_attr_t *dsa, int key, void *val, int size)
{
  ibp_capstatus_t *p = (ibp_capstatus_t *)dsa;
  ds_int_t *n = (ds_int_t *)val;
  int err = 0;

  if (size < sizeof(ds_int_t))  return(sizeof(ds_int_t));

  switch (key) {
    case DS_PROBE_DURATION: *n = p->attrib.duration;  break;
    case DS_PROBE_READ_COUNT: *n = p->readRefCount;  break;
    case DS_PROBE_WRITE_COUNT: *n = p->writeRefCount;  break;
    case DS_PROBE_CURR_SIZE: *n = p->currentSize;  break;
    case DS_PROBE_MAX_SIZE: *n = p->maxSize;  break;
    default: err = -1;
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

  strncpy(depot->host, string_token(str, ":", &bstate, &fin), sizeof(depot->host)-1); depot->host[sizeof(depot->host)-1] = '\0';
  tmp = string_token(NULL, ":", &bstate, &fin);
  depot->port = atoi(tmp);
  strncpy(depot->rid.name, string_token(NULL, ":", &bstate, &fin), sizeof(depot->rid.name)-1); depot->rid.name[sizeof(depot->rid.name)-1] = '\0';

  free(str);

  return(0);  
}


//***********************************************************************
//  ds_ibp_op_create - Creates a new CB for the opque list
//***********************************************************************

ds_ibp_op_t *ds_ibp_op_create(ds_ibp_priv_t *ds, ds_ibp_attr_t *attr)
{
//  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)dsf->priv;
  ds_ibp_op_t *iop;

  type_malloc_clear(iop, ds_ibp_op_t, 1);

  iop->attr = (attr == NULL) ? &(ds->attr_default) : attr;
  return(iop);
}

//***********************************************************************
// _ds_ibp_op_free - Frees the calling structure
//***********************************************************************

void _ds_ibp_op_free(op_generic_t *gop, int mode)
{
  ds_ibp_op_t *iop = gop->free_ptr;

//log_printf(0, "Freeing gid=%d\n", gop_id(gop));
//flush_log();

  //** Call the original cleanup routine
  gop->free_ptr = iop->free_ptr;
  iop->free(gop, mode);

  if (mode == OP_DESTROY) free(iop);
}

//***********************************************************************
//  ds_ibp_setup_finish - Finishes iop setup
//***********************************************************************

void ds_ibp_setup_finish(ds_ibp_op_t *iop)
{
  if (ibp_cc_type(&(iop->attr->cc)) != NS_TYPE_UNKNOWN) ibp_op_set_cc(iop->gop, &(iop->attr->cc));

  iop->free = iop->gop->base.free;
  iop->free_ptr = iop->gop->free_ptr;

  iop->gop->base.free = _ds_ibp_op_free;
  iop->gop->free_ptr = iop;
}

//***********************************************************************
// ds_ibp_allocate - Makes an IBP allocation operation 
//***********************************************************************

op_generic_t *ds_ibp_allocate(data_service_fn_t *dsf, char *res, data_attr_t *dattr, ds_int_t size, data_cap_set_t *caps, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)dsf->priv;
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;
  ds_ibp_alloc_op_t *cmd;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
  cmd = &(iop->alloc);

  //** Fill in the depot structure
  res2ibp(res, &(cmd->depot));

  //** Create the op
  iop->gop = new_ibp_alloc_op(ds->ic, caps, size, &(cmd->depot), &(iop->attr->attr), iop->attr->disk_cs_type, iop->attr->disk_cs_blocksize, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}


//***********************************************************************
// ds_ibp_remove - Decrements the allocations ref count
//***********************************************************************

op_generic_t *ds_ibp_remove(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *cap, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_remove_op(ds->ic, cap, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_truncate - Adjusts the allocations size.
//***********************************************************************

op_generic_t *ds_ibp_truncate(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, ex_off_t new_size, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_truncate_op(ds->ic, mcap, new_size, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_modify_count - Decrements the allocations ref count
//***********************************************************************

op_generic_t *ds_ibp_modify_count(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, int mode, int captype, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;
  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
  int imode, icaptype;

  switch (mode) {
     case (DS_MODE_INCR) : imode = IBP_INCR; break;
     case (DS_MODE_DECR) : imode = IBP_DECR; break;
     default:
         log_printf(0, "ds_ibp_modify_count: invalid mode! mode=%d\n", mode);
         return(NULL);
  }

  switch (captype) {
     case (DS_CAP_READ) : icaptype = IBP_READCAP; break;
     case (DS_CAP_WRITE) : icaptype = IBP_WRITECAP; break;
     default:
         log_printf(0, "ds_ibp_modify_count: invalid captype! captype=%d\n", captype);
         return(NULL);
  }

  //** Create the op
  iop->gop = new_ibp_modify_count_op(ds->ic, mcap, imode, icaptype, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_probe - Generates an allocation probe operation
//***********************************************************************

op_generic_t *ds_ibp_probe(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *mcap, data_probe_t *probe, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_probe_op(ds->ic, mcap, (ibp_capstatus_t *)probe, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_read - Generates a read operation
//***********************************************************************

op_generic_t *ds_ibp_read(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *rcap, ds_int_t off, tbuffer_t *dread, ds_int_t droff, ds_int_t size, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_read_op(ds->ic, rcap, off, dread, droff, size, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_write - Generates a write operation
//***********************************************************************

op_generic_t *ds_ibp_write(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *wcap, ds_int_t off, tbuffer_t *dwrite, ds_int_t boff, ds_int_t size, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_write_op(ds->ic, wcap, off, dwrite, boff, size, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_append - Generates a write append operation
//***********************************************************************

op_generic_t *ds_ibp_append(data_service_fn_t *dsf, data_attr_t *dattr, data_cap_t *wcap, tbuffer_t *dwrite, ds_int_t boff, ds_int_t size, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;

  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);

  //** Create the op
  iop->gop = new_ibp_append_op(ds->ic, wcap, dwrite, boff, size, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}

//***********************************************************************
// ds_ibp_copy - Generates a depot-depot copy operation
//***********************************************************************

op_generic_t *ds_ibp_copy(data_service_fn_t *dsf, data_attr_t *dattr, int mode, int ns_type, char *ppath, data_cap_t *src_cap, ds_int_t src_off,  
          data_cap_t *dest_cap, ds_int_t dest_off, ds_int_t len, int timeout)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)(dsf->priv);
  ds_ibp_attr_t *attr = (ds_ibp_attr_t *)dattr;
  ds_ibp_op_t *iop = ds_ibp_op_create(ds, attr);
  int dir;

  //** Create the op
  dir = ((mode & DS_PULL) > 0) ? IBP_PULL : IBP_PUSH;
  iop->gop = new_ibp_copy_op(ds->ic, dir, ns_type, ppath, src_cap, dest_cap, src_off, dest_off, len, timeout, timeout, timeout);

  ds_ibp_setup_finish(iop);

  return(iop->gop);
}


//***********************************************************************
// ds_ibp_set_default_attr - Configures the default attributes for the ds
//***********************************************************************

int ds_ibp_set_default_attr(data_service_fn_t *dsf, data_attr_t *da)
{
   ds_ibp_priv_t *ds = (ds_ibp_priv_t *)dsf->priv;
   ds_ibp_attr_t *ida = (ds_ibp_attr_t *)da;

   ds->attr_default = *ida;

   return(0);
}

//***********************************************************************
//  ds_ibp_get_default - Retreive the pointer to the global default attributes
//***********************************************************************

int ds_ibp_get_default_attr(data_service_fn_t *dsf, data_attr_t *da)
{
   ds_ibp_priv_t *ds = (ds_ibp_priv_t *)dsf->priv;
   ds_ibp_attr_t *ida = (ds_ibp_attr_t *)da;

   *ida = ds->attr_default;

   return(0);
}

//***********************************************************************
//  ds_ibp_destroy - Creates the IBP data service
//***********************************************************************

void ds_ibp_destroy(data_service_fn_t *dsf)
{
  ds_ibp_priv_t *ds = (ds_ibp_priv_t *)dsf->priv;

//  ibp_destroy_context(ds->ic);  //** Done by the callint program

  ibp_destroy_context(ds->ic);

  free(ds);
  free(dsf);
}


//***********************************************************************
//  ds_ibp_create - Creates the IBP data service
//***********************************************************************

data_service_fn_t *ds_ibp_create(void *arg, char *config_file, char *section)
{
  data_service_fn_t *dsf;
  ds_ibp_priv_t *ds;
  ibp_context_t *ic;
  inip_file_t *ifd;

  type_malloc_clear(dsf, data_service_fn_t, 1);
  type_malloc_clear(ds, ds_ibp_priv_t , 1);

  //** Set the default attributes
  ifd = inip_read(config_file);
  memset(&(ds->attr_default), 0, sizeof(ds_ibp_attr_t));
  ds->attr_default.attr.duration = inip_get_integer(ifd, section, "duration", 3600);
  ds->attr_default.attr.reliability = IBP_HARD;
  ds->attr_default.attr.type = IBP_BYTEARRAY;
  ds->attr_default.disk_cs_type = CHKSUM_DEFAULT;
  inip_destroy(ifd);

  //printf("cfg=%s sec=%s\n", config_file, section);
  ic = ibp_create_context();
  ibp_load_config(ic, config_file, section);
  ds->ic = ic;
  dsf->type = DS_TYPE_IBP;

  dsf->priv = (void *)ds;
  dsf->destroy_service = ds_ibp_destroy;
  dsf->new_cap_set = ds_ibp_new_cap_set;
  dsf->get_cap = ds_ibp_get_cap;
  dsf->set_cap = ds_ibp_set_cap;
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
  dsf->allocate = ds_ibp_allocate;
  dsf->remove = ds_ibp_remove;
  dsf->modify_count = ds_ibp_modify_count;
  dsf->read = ds_ibp_read;
  dsf->write = ds_ibp_write;
  dsf->append = ds_ibp_append;
  dsf->copy = ds_ibp_copy;
  dsf->probe = ds_ibp_probe;
  dsf->truncate = ds_ibp_truncate;

  return(dsf);
}


