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

/*! \file ibp_nfu.h 

	This file includes definitions of the IBP NFU(networking function unit) APIs 
  and data structure.
*/

# ifndef _IBP_NFU_H
# define _IBP_NFU_H

# include "ibp_ClientLib.h"

# ifdef __cplusplus 
extern "C" { 
# endif 

typedef enum { IBP_REF_RD,IBP_REF_WR,IBP_REF_RDWR,
               IBP_VAL_IN,IBP_VAL_OUT,IBP_VAL_INOUT } IOTYPE;
/*typedef enum { DATA_CAP , DATA_BUF } DATATYPE; */

typedef struct {
  IOTYPE              ioType; /* input and/or output */
  void                *data;
  int                 offset;
  int                 len;
}PARAMETER;

int IBP_nfu_op( IBP_depot depot, int opcode, int nPara, PARAMETER *paras ,IBP_timer timeout);

# ifdef __cplusplus
}
# endif 

# endif /* _IBP_NFU_H */
