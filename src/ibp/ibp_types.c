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

#define _log_module_index 134

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "ibp.h"
#include "log.h"

//*****************************************************************
//  new_ibp_depot -Creates a new ibp_depot_t structure
//*****************************************************************

ibp_depot_t *new_ibp_depot()
{
    return((ibp_depot_t *)malloc(sizeof(ibp_depot_t)));
}


//*****************************************************************
//  destroy_ibp_depot - Destrots the ibp_depot_t structure
//*****************************************************************

void destroy_ibp_depot(ibp_depot_t *d)
{
    free(d);
}


//*****************************************************************
//  set_ibp_depot - Initializes an ibp_depot_struct
//*****************************************************************

void set_ibp_depot(ibp_depot_t *d, char *host, int port, rid_t rid)
{
    strncpy(d->host, host, sizeof(d->host));
    d->host[sizeof(d->host)-1]='\0';

    d->port = port;
    d->rid = rid;
}

//===================================================================

//*****************************************************************
//  new_ibp_attributes -Creates a new ibp_attributes_t structure
//*****************************************************************

ibp_attributes_t *new_ibp_attributes()
{
    return((ibp_attributes_t *)malloc(sizeof(ibp_attributes_t)));
}


//*****************************************************************
//  destroy_ibp_attributes - Destrots the ibp_attributes_t structure
//*****************************************************************

void destroy_ibp_attributes(ibp_attributes_t *attr)
{
    free(attr);
}

//*****************************************************************
// set_ibp_attributes - Initializes the data structure
//*****************************************************************

void set_ibp_attributes(ibp_attributes_t *attr, time_t duration, int reliability, int type)
{
    attr->duration = duration;
    attr->reliability = reliability;
    attr->type = type;
}

//*****************************************************************
// get_ibp_attributes - Returns the ibp attributes
//*****************************************************************

void get_ibp_attributes(ibp_attributes_t *attr, time_t *duration, int *reliability, int *type)
{
    *duration = attr->duration;
    *reliability = attr->reliability;
    *type = attr->type;
}


//===================================================================

//*****************************************************************
//  new_ibp_timer -Creates a new ibp_timer_t structure
//*****************************************************************

ibp_timer_t *new_ibp_timer()
{
    ibp_timer_t *t = (ibp_timer_t *)malloc(sizeof(ibp_timer_t));
    if (t == NULL) return(NULL);

    t->ClientTimeout = 0;
    t->ServerSync = 0;

    return(t);
}


//*****************************************************************
//  destroy_ibp_attributes - Destrots the ibp_attributes_t structure
//*****************************************************************

void destroy_ibp_timer(ibp_timer_t *t)
{
    free(t);
}

//*****************************************************************
// set_ibp_timer - Initializes the data structure
//*****************************************************************

void set_ibp_timer(ibp_timer_t *t, int client_timeout, int server_timeout)
{
    t->ClientTimeout = client_timeout;
    t->ServerSync = server_timeout;
}

//*****************************************************************
// set_ibp_timer - Retreives the timer information
//*****************************************************************

void get_ibp_timer(ibp_timer_t *t, int *client_timeout, int *server_timeout)
{
    *client_timeout = t->ClientTimeout;
    *server_timeout = t->ServerSync;
}

//===================================================================


//*****************************************************************
//  destroy_ibp_cap
//*****************************************************************

void destroy_ibp_cap(ibp_cap_t *cap)
{
    free(cap);
}

//*****************************************************************
// dup_ibp_cap - Duplicates an IBP capability
//*****************************************************************

ibp_cap_t *dup_ibp_cap(ibp_cap_t *src)
{
    return(strdup(src));
}

//===================================================================

//*****************************************************************
//  new_ibp_capset -Creates a new ibp_capset_t structure
//*****************************************************************

ibp_capset_t *new_ibp_capset()
{
    ibp_capset_t *c = (ibp_capset_t *)malloc(sizeof(ibp_capset_t));
    if (c == NULL) return(NULL);

    c->readCap = NULL;
    c->writeCap = NULL;;
    c->manageCap = NULL;

    return(c);
}


//*****************************************************************
//  destroy_ibp_capset - Destroys the ibp_capset_t structure
//*****************************************************************

void destroy_ibp_capset(ibp_capset_t *caps)
{
    destroy_ibp_cap(caps->readCap);
    destroy_ibp_cap(caps->writeCap);
    destroy_ibp_cap(caps->manageCap);

    free(caps);
}

//*****************************************************************
// copy_ibp_cap - Copies a set of IBP capabilities
//*****************************************************************

void copy_ibp_capset(ibp_capset_t *src, ibp_capset_t *dest)
{
    dest->readCap = dup_ibp_cap(src->readCap);
    dest->writeCap = dup_ibp_cap(src->writeCap);
    dest->manageCap = dup_ibp_cap(src->manageCap);
}

//*****************************************************************
// get_ibp_cap - Returns the requested capability
//*****************************************************************

ibp_cap_t *get_ibp_cap(ibp_capset_t *caps, int ctype)
{
    ibp_cap_t *c;

    if (ctype == IBP_READCAP) {
        c = caps->readCap;
    } else if (ctype == IBP_WRITECAP) {
        c = caps->writeCap;
    } else if (ctype == IBP_MANAGECAP) {
        c = caps->manageCap;
    } else {
        c = NULL;
    }

    return(c);
}


//===================================================================

//*****************************************************************
//  new_ibp_alias_capstatus -Creates a new ibp_alias_capstatus_t structure
//*****************************************************************

ibp_alias_capstatus_t *new_ibp_alias_capstatus()
{
    ibp_alias_capstatus_t *cs = (ibp_alias_capstatus_t *)malloc(sizeof(ibp_alias_capstatus_t));
    if (cs == NULL) return(NULL);


    cs->read_refcount = -1;
    cs->write_refcount = -1;
    cs->offset = 0;
    cs->size = 0;
    cs->duration = 0;

    return(cs);
}


//*****************************************************************
//  destroy_ibp_alias_capstatus - Destroys the ibp_alias_capstatus_t structure
//*****************************************************************

void destroy_ibp_alias_capstatus(ibp_alias_capstatus_t *cs)
{
    free(cs);
}

//*****************************************************************
// copy_ibp_alias_capstatus - Copies a the alias_capstatus info
//*****************************************************************

void copy_ibp_alias_capstatus(ibp_alias_capstatus_t *src, ibp_alias_capstatus_t *dest)
{
    memcpy(dest, src, sizeof(ibp_alias_capstatus_t));
}

//*****************************************************************
// get_ibp_alias_capstatus - Returns the requested alias caps status info
//*****************************************************************

void get_ibp_alias_capstatus(ibp_alias_capstatus_t *cs, int *readcount, int *writecount,
                             ibp_off_t *offset, ibp_off_t *size, int *duration)
{
    *readcount = cs->read_refcount;
    *writecount = cs->write_refcount;
    *offset = cs->offset;
    *size = cs->size;
    *duration = cs->duration;
}

//===================================================================

//*****************************************************************
//  new_ibp_capstatus -Creates a new ibp_capstatus_t structure
//*****************************************************************

ibp_capstatus_t *new_ibp_capstatus()
{
    ibp_capstatus_t *cs = (ibp_capstatus_t *)malloc(sizeof(ibp_capstatus_t));
    if (cs == NULL) return(NULL);


    cs->readRefCount = -1;
    cs->writeRefCount = -1;
    cs->currentSize = -1;
    cs->maxSize = 0;
    set_ibp_attributes(&(cs->attrib), 0, -1, -1);

    return(cs);
}


//*****************************************************************
//  destroy_ibp_capstatus - Destroys the ibp_capstatus_t structure
//*****************************************************************

void destroy_ibp_capstatus(ibp_capstatus_t *cs)
{
    free(cs);
}

//*****************************************************************
// copy_ibp_capstatus - Copies a the capstatus info
//*****************************************************************

void copy_ibp_capstatus(ibp_capstatus_t *src, ibp_capstatus_t *dest)
{
    memcpy(dest, src, sizeof(ibp_capstatus_t));
}

//*****************************************************************
// get_ibp_capstatus - Returns the requested caps status info
//    The attrib pointer is redirected to the CapStatus's attrib
//    field.  As a result it should NOT point to "new"ed attribute
//    structure.
//*****************************************************************

void get_ibp_capstatus(ibp_capstatus_t *cs, int *readcount, int *writecount,
                       int *current_size, int *max_size, ibp_attributes_t *attrib)
{
    *readcount = cs->readRefCount;
    *writecount = cs->writeRefCount;
    *current_size = cs->currentSize;
    *max_size = cs->maxSize;
    *attrib = cs->attrib;
}

//===================================================================
//  ibp_ridlist_t manipulation routines
//===================================================================

//*****************************************************************

void ridlist_init(ibp_ridlist_t *rlist, int size)
{
    rlist->rl = (rid_t *)malloc(sizeof(rid_t)*size);
    assert(rlist->rl != NULL);

    rlist->n = size;
}

//*****************************************************************

void ridlist_destroy(ibp_ridlist_t *rlist)
{
    free(rlist->rl);
}

//*****************************************************************

int ridlist_get_size(ibp_ridlist_t *rlist)
{
    return(rlist->n);
}

//*****************************************************************

rid_t ridlist_get_element(ibp_ridlist_t *rlist, int index)
{
    if (index >= rlist->n) {
        log_printf(0, "ridlist_get_element:  Invalid index!  size=%d index=%d\n", rlist->n, index);
        rid_t rid;
        ibp_empty_rid(&rid);
        return(rid);
    }

    return(rlist->rl[index]);
}

//*****************************************************************

char *ibp_rid2str(rid_t rid, char *buffer)
{
    strncpy(buffer, rid.name, RID_LEN);

    return(buffer);
}

//*****************************************************************

rid_t ibp_str2rid(char *rid_str)
{
    rid_t rid;
    strncpy(rid.name, rid_str, RID_LEN);

    return(rid);
}

//*****************************************************************

void ibp_empty_rid(rid_t *rid)
{
    sprintf(rid->name, "0");
}

//*****************************************************************

int ibp_rid_is_empty(rid_t rid)
{
    if (strcmp(rid.name, "0") == 0) {
        return(1);
    }

    return(0);
}

//*****************************************************************

int ibp_compare_rid(rid_t rid1, rid_t rid2)
{
    return(strncmp(rid1.name, rid2.name, RID_LEN));
}




