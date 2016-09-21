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

#define _log_module_index 133

#include <stdlib.h>
#include <assert.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <ibp/ibp.h>
#include "ibp_sync.h"

//#include "host_portal.h"
//#include "opque.h"

extern ibp_context_t *ic;  //QWERT

ibp_context_t *_ibp_sync = NULL;

//**************************************************************************
//---------------  Unsupported routines are listed below -------------------
//**************************************************************************

#ifndef IBP_REMOVE_UNSUPPORTED

unsigned long int  IBP_mcopy ( IBP_cap pc_SourceCap,
                               IBP_cap pc_TargetCap[],
                               unsigned int pi_CapCnt,
                               IBP_timer ps_src_timeout,
                               IBP_timer ps_tgt_timeout,
                               unsigned long int pl_size,
                               unsigned long int pl_offset,
                               int dm_type[],
                               int dm_port[],
                               int dm_service)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;

    return(0);
}

unsigned long int  IBP_datamover (  IBP_cap pc_TargetCap,
                                    IBP_cap pc_ReadCap,
                                    IBP_timer ps_tgt_timeout,
                                    unsigned long int pl_size,
                                    unsigned long int pl_offset,
                                    int dm_type,
                                    int dm_port,
                                    int dm_service)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
    return(0);
}

int IBP_setAuthenAttribute(char *certFile, char *privateKeyFile , char *passwd)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
    return(0);
}

int IBP_freeCapSet(IBP_set_of_caps capSet)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
    return(0);
}

char* DM_Array2String(int numelems, void  *array[], int type)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
    return(NULL);
}

int IBP_setMaxOpenConn(int max)
{
    IBP_errno = IBP_E_TYPE_NOT_SUPPORTED;
    return(0);
}

#endif

//**************************************************************************
// ------------------ Supported routines are given below -------------------
//**************************************************************************


//**************************************************************************
// ibp_sync_command - Executes a command synchronously
//**************************************************************************

int ibp_sync_command(gop_op_generic_t *gop)
{
    gop_op_status_t status;
    int err;

    gop_set_exec_mode(gop, OP_EXEC_DIRECT);
    gop_start_execution(gop);

    //** and Wait for completion
    gop_waitany(gop);
    status = gop_get_status(gop);
    IBP_errno = status.error_code;
    err = status.error_code;

    if (gop->op->cmd.hostport != NULL) {
        free(gop->op->cmd.hostport);
        gop->op->cmd.hostport = NULL;
    }

    log_printf(10, "ibp_sync_command: IBP_errno=%d\n", err);

    return(err);
}

//**************************************************************************
// set_ibp_sync_context - Sets the IBP context to use
//**************************************************************************

void set_ibp_sync_context(ibp_context_t *ic)
{
    _ibp_sync = ic;
}

//**************************************************************************
// make_ibp_sync_context
//**************************************************************************

void make_ibp_sync_context()
{
    if (_ibp_sync != NULL) return;

//QWERT _ibp_sync = ic;
    _ibp_sync = ibp_context_create();
}

//**************************************************************************
// destroy_ibp_sync_context
//**************************************************************************

void destroy_ibp_sync_context()
{
    if (_ibp_sync == NULL) return;

    ibp_context_destroy(_ibp_sync);
}

//**************************************************************************
//  IBP_allocate - Creates an allocation and Returns a set of capabilities
//**************************************************************************

ibp_capset_t *IBP_allocate(ibp_depot_t  *depot, ibp_timer_t *timer, unsigned long int size, ibp_attributes_t *attr)
{
    int err;

    make_ibp_sync_context();

    ibp_capset_t *cs = (ibp_capset_t *)malloc(sizeof(ibp_capset_t));
    assert(cs != NULL);

    err = ibp_sync_command(ibp_alloc_gop(_ibp_sync, cs, size, depot, attr, CHKSUM_DEFAULT, 0, timer->ClientTimeout));
    if (err != IBP_OK) {
        free(cs);
        cs = NULL;
    }

    return(cs);
}

//**************************************************************************
//  IBP_write - Stores dataa at the given offset for an IBP allocation
//**************************************************************************

unsigned long int IBP_write(ibp_cap_t *cap, ibp_timer_t  *timer, char *data,
                            unsigned long int size, unsigned long int offset)
{
    int err;
    tbx_tbuf_t buf;

    tbx_tbuf_single(&buf, size, data);

    make_ibp_sync_context();

    err = ibp_sync_command(ibp_write_gop(_ibp_sync, cap, offset, &buf, 0, size, timer->ClientTimeout));
    if (err != IBP_OK) return(0);
    return(size);
}

//**************************************************************************
//  IBP_store - *Appends* data to an IBP allocation
//**************************************************************************

unsigned long int IBP_store(ibp_cap_t *cap, ibp_timer_t  *timer, char *data,
                            unsigned long int size)
{
    int err;
    tbx_tbuf_t buf;

    tbx_tbuf_single(&buf, size, data);

    make_ibp_sync_context();

    err = ibp_sync_command(ibp_append_gop(_ibp_sync, cap, &buf, 0, size, timer->ClientTimeout));
    if (err != IBP_OK) return(0);
    return(size);
}

//**************************************************************************
// IBP_load - Reads data from the given IBP allocation
//**************************************************************************

unsigned long int IBP_load(ibp_cap_t *cap, ibp_timer_t  *timer, char *data,
                           unsigned long int size, unsigned long int offset)
{
    int err;
    tbx_tbuf_t buf;

    tbx_tbuf_single(&buf, size, data);

    make_ibp_sync_context();

    err = ibp_sync_command(ibp_read_gop(_ibp_sync, cap, offset, &buf, 0, size, timer->ClientTimeout));
    if (err != IBP_OK) return(0);
    return(size);
}

//**************************************************************************
//  IBP_copy - Copies data between allocations. The data is *appended* to the
//      end of the destination allocation.
//**************************************************************************

unsigned long int IBP_copy(ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
                           unsigned long int size, unsigned long int offset)
{
    int err;

    make_ibp_sync_context();

    err = ibp_sync_command(ibp_copyappend_gop(_ibp_sync, NS_TYPE_SOCK, NULL, srccap, destcap, offset, size, src_timer->ClientTimeout,
                          dest_timer->ServerSync, dest_timer->ClientTimeout));
    if (err != IBP_OK) return(0);
    return(size);
}

//**************************************************************************
//  IBP_copy - Copies data between allocations. The data is *appended* to the
//      end of the destination allocation.
//**************************************************************************

unsigned long int IBP_phoebus_copy(char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
                                   ibp_off_t size, ibp_off_t offset)
{
    int err;

    make_ibp_sync_context();

    err = ibp_sync_command(ibp_copyappend_gop(_ibp_sync, NS_TYPE_PHOEBUS, path, srccap, destcap, offset, size, src_timer->ClientTimeout,
                          dest_timer->ServerSync, dest_timer->ClientTimeout));
    if (err != IBP_OK) return(0);
    return(size);
}

//**************************************************************************
// IBP_manage - Queries/modifies an IBP allocations properties
//**************************************************************************

int IBP_manage(ibp_cap_t *cap, ibp_timer_t  *timer, int cmd, int captype, ibp_capstatus_t *cs)
{
    int err;

    make_ibp_sync_context();

    log_printf(15, "IBP_manage: cmd=%d cap=%s\n", cmd, cap);
    fflush(stdout);

    err = 0;
    switch (cmd) {
    case IBP_INCR:
    case IBP_DECR:
        err = ibp_sync_command(ibp_modify_count_gop(_ibp_sync, cap, cmd, captype, timer->ClientTimeout));
        break;
    case IBP_PROBE:
        err = ibp_sync_command(ibp_probe_gop(_ibp_sync, cap, cs, timer->ClientTimeout));
        break;
    case IBP_CHNG:
        err = ibp_sync_command(ibp_modify_alloc_gop(_ibp_sync, cap, cs->maxSize, cs->attrib.duration, cs->attrib.reliability, timer->ClientTimeout));
        break;
    default:
        err = IBP_E_INVALID_PARAMETER;
        IBP_errno = IBP_E_INVALID_PARAMETER;
        log_printf(0, "IBP_manage:  Invalid command: %d\n", cmd);
    }

    log_printf(15, "AFTER IBP_manage: cmd=%d cap=%s\n", cmd, cap);
    fflush(stdout);

    if (err != IBP_OK) return(-1);
    return(0);
}

//**************************************************************************
// IBP_status - Reads data from the given IBP allocation
//**************************************************************************

ibp_depotinfo_t *IBP_status(ibp_depot_t *depot, int cmd, ibp_timer_t *timer, char *password,
                            unsigned long int  hard, unsigned long int soft, long duration)
{
    int err;
    ibp_depotinfo_t *di = NULL;

    make_ibp_sync_context();

    if (cmd == IBP_ST_INQ) {
        di = (ibp_depotinfo_t *)malloc(sizeof(ibp_depotinfo_t));
        assert(di != NULL);
        err = ibp_sync_command(ibp_depot_inq_gop(_ibp_sync, depot, password, di, timer->ClientTimeout));
    } else {
        err = ibp_sync_command(ibp_depot_modify_gop(_ibp_sync, depot, password, hard, soft, duration, timer->ClientTimeout));
    }

    if (err != IBP_OK) {
        free(di);
        return(NULL);
    }
    return(di);
}

