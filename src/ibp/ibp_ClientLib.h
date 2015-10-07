/*
 *           IBP Client version 1.0:  Internet BackPlane Protocol
 *               University of Tennessee, Knoxville TN.
 *          Authors: Y. Zheng A. Bassi, W. Elwasif, J. Plank, M. Beck
 *                   (C) 1999 All Rights Reserved
 *
 *                              NOTICE
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby granted
 * provided that the above copyright notice appear in all copies and
 * that both the copyright notice and this permission notice appear in
 * supporting documentation.
 *
 * Neither the Institution (University of Tennessee) nor the Authors 
 * make any representations about the suitability of this software for 
 * any purpose. This software is provided ``as is'' without express or
 * implied warranty.
 *
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

# ifndef _IBP_CLIENTLIB_H
# define _IBP_CLIENTLIB_H

# ifdef STDC_HEADERS
# include <sys/types.h>
# endif
# include "ibp_os.h"
# include <time.h>
# include "ibp_protocol.h"
# include "ibp_errno.h"
# include "ibp_types.h"
/*# include "ibp_nfu.h"*/



# ifdef __cplusplus
extern "C" { 
# endif 

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


int IBP_setAuthenAttribute(char *certFile, char *privateKeyFile , char *passwd);
int IBP_freeCapSet(IBP_set_of_caps capSet);
extern char* DM_Array2String(int numelems, void  *array[], int type);
int IBP_setMaxOpenConn(int max);


# ifdef __cplusplus
}
# endif 

# endif /* _IBP_CLIENTLIB_H */
