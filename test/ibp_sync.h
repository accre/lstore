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

/*
 * ibp_ClientLib.h
 *
 * Basic client APIs and data struct providing to IBP application users
 *
 */

/*! \file ibp_ClientLib.h
  \brief  IBP client include file

  This file includes definitions of the IBP client APIs and data structure
  used by application.
*/

# ifndef _IBP_SYNC_H
# define _IBP_SYNC_H

# ifdef STDC_HEADERS
# include <sys/types.h>
# endif
# include <ibp/ibp.h>
# include <time.h>

# include "ibp_errno.h"

# ifdef __cplusplus
extern "C" {
# endif

typedef struct ibp_attributes_t * IBP_attributes;
typedef struct ibp_capset_t * IBP_set_of_caps;
typedef struct ibp_depot_t * IBP_depot;
typedef struct ibp_timer_t * IBP_timer;
typedef ibp_cap_t * IBP_cap;
typedef struct ibp_capstatus_t * IBP_CapStatus;
typedef struct ibp_depotinfo_t * IBP_DptInfo;


/*********************************************************
 * IBP Client APIs definition
 *********************************************************/

IBP_set_of_caps  IBP_allocate(  IBP_depot  ps_depot,
                                IBP_timer   ps_timeout,
                                unsigned long int pl_size,
                                IBP_attributes ps_attr);

unsigned long int IBP_store(  IBP_cap    pc_cap,
                              IBP_timer  ps_timeout,
                              char *     pc_data,
                              unsigned long int pl_size);

unsigned long int IBP_write(  IBP_cap    pc_cap,
                              IBP_timer  ps_timeout,
                              char *     pc_data,
                              unsigned long int pl_size,
                              unsigned long int pl_offset);

unsigned long int  IBP_load ( IBP_cap  pc_source,
                              IBP_timer  ps_timeout,
                              char *  pc_buf,
                              unsigned long int  pl_size,
                              unsigned long int  pl_offset);

unsigned long int  IBP_copy(  IBP_cap  ps_source,
                              IBP_cap  ps_target,
                              IBP_timer  ps_sourceTimeout,
                              IBP_timer  ps_targetTimeout,
                              unsigned long int  pl_size,
                              unsigned long int  pl_offset);

unsigned long int  IBP_mcopy ( IBP_cap pc_SourceCap,
                               IBP_cap pc_TargetCap[],
                               unsigned int pi_CapCnt,
                               IBP_timer ps_src_timeout,
                               IBP_timer ps_tgt_timeout,
                               unsigned long int pl_size,
                               unsigned long int pl_offset,
                               int dm_type[],
                               int dm_port[],
                               int dm_service);

unsigned long int  IBP_datamover (  IBP_cap pc_TargetCap,
                                    IBP_cap pc_ReadCap,
                                    IBP_timer ps_tgt_timeout,
                                    unsigned long int pl_size,
                                    unsigned long int pl_offset,
                                    int dm_type,
                                    int dm_port,
                                    int dm_service);

int IBP_manage(   IBP_cap   pc_manCap,
                  IBP_timer ps_timeout,
                  int  pi_cmd,
                  int  pi_capType,
                  IBP_CapStatus  ps_info);

IBP_DptInfo  IBP_status(  IBP_depot ps_depot,
                          int       pi_StCmd,
                          IBP_timer ps_timeout,
                          char     *pc_password,
                          unsigned long int  pl_stableStor,
                          unsigned long int  pl_VolStor,
                          long    pl_duration);

unsigned long int IBP_phoebus_copy(char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
                                   ibp_off_t size, ibp_off_t offset);

int IBP_setAuthenAttribute(char *certFile, char *privateKeyFile , char *passwd);
int IBP_freeCapSet(IBP_set_of_caps capSet);
extern char* DM_Array2String(int numelems, void  *array[], int type);
int IBP_setMaxOpenConn(int max);

int ibp_sync_command(gop_op_generic_t *gop);
void set_ibp_sync_context(ibp_context_t *ic);

# ifdef __cplusplus
}
# endif

# endif /* _IBP_SYNC_H */
