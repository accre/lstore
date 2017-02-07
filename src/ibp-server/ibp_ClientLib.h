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
/*# include "ibp_nfu.h"*/



# ifdef __cpluscplus 
extern "C" { 
# endif 

/*********************************************************
 * Constant definition
 *********************************************************/

# define IBP_READCAP      1
# define IBP_WRITECAP     2
# define IBP_MANAGECAP    3

# define IBP_MAX_HOSTNAME_LEN  256
# define CMD_BUF_SIZE      1024

/*********************************************************
 * Data sturcture definition.
 *********************************************************/

/*
 * Definition of the capability attributes structure
 */
typedef struct ibp_attributes {
  time_t  duration;     /* lifetime of the capability */
  int     reliability;  /* reliability type of the capability */
  int     type;         /* capability storage type */
} *IBP_attributes;

/* 
 * Definition of the IBP depot structure 
 */
typedef struct ibp_depot {
  char  host[IBP_MAX_HOSTNAME_LEN];  /* host name of the depot */
  int   port;        /* port number */
  int   rid;         /* resource ID */
} *IBP_depot;

/*
 * Definition of the IBP depot information 
 */
typedef struct ibp_dptinfo {
  unsigned long int   StableStor;  /* size of the stable storage (in MByte) */
  unsigned long int   StableStorUsed;  /* size of the used stable storage (in MByte) */
  unsigned long int   VolStor;  /* size of the volatile storage (in MByte) */
  unsigned long int   VolStorUsed;  /* size of the used volatile storage (in MByte) */
  long                Duration;  /* How long the depot keeps up */
  float               majorVersion;   /* version (major) of IBP server */ 
  float               minorVersion;   /* version (minor) of IBP server */ 
  int                 rid;
  int                 type;
  long long           HardConfigured;
  long long           HardServed;
  long long           HardUsed;
  long long           HardAllocable;
  long long           TotalConfigured;
  long long           TotalServed;
  long long           TotalUsed;
  long long           SoftAllocable;
  int                 nDM;            /* how many data mover  types the 
                                        IBP server supports           */
  int                 *dmTypes;       /* data mover types              */ 
  int                 nNFU;           /* number of NFU ops */
  int                 *NFU;           /* NFU ops */
} *IBP_DptInfo;


/*
 * Definition of the timer structure 
 */
typedef struct ibp_timer {
  int  ClientTimeout;  /* Timeout on client side */
  int  ServerSync;  /* Timeout on server(depot) side */
} *IBP_timer;

/*
 * Definition of the capability status structure
 */
typedef struct ibp_capstatus{
  int  readRefCount;  /* number of the capability's read reference */
  int  writeRefCount;  /* number of the capability's write reference */
  int  currentSize;  /* size of data in the capability */
  unsigned long int  maxSize;  /* max size of the capability */
  struct ibp_attributes  attrib;    /* attributes of the capability */
  char  *passwd;  /* passwd of the depot */
} *IBP_CapStatus;

/*
 * Definition of the capability 
 */
typedef char* IBP_cap;

/*
 * Definition of the capability set
 */
typedef struct ibp_set_of_caps {
  IBP_cap  readCap;  /* read capability */
  IBP_cap writeCap;  /* write capability */
  IBP_cap manageCap;  /* manage capability */
} *IBP_set_of_caps;

 
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
/*
unsigned long int IBP_remote_store( IBP_cap  pc_cap,
                                    IBP_depot  ps_remoteDepot,
                                    IBP_timer  ps_timeout,
                                    unsigned long int  pl_size);
*/
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

# ifdef __cpluscplus
}
# endif 

# endif /* _IBP_CLIENTLIB_H */
