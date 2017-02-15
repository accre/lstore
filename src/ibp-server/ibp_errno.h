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
 * ibp_errno.h
 *
 * Definition of errno handling function and/or variable 
 */


# ifndef _IBP_ERRNO_H
# define _IBP_ERRNO_H

# include "ibp_os.h"

# ifdef __cplusplus 
extern "C" {
# endif
 


/*********************************************************
 * Global variable definition
 *********************************************************/

 /*
 * definition of errno code
 */
# define PTHREAD_SUPPORTED 1
#if PTHREAD_SUPPORTED 
	extern int *_IBP_errno();
    #define IBP_errno ( *_IBP_errno())
#else
	extern int	IBP_errno;
#endif


/***********************************************************
 * Subroutines definition
 ***********************************************************/

void _set_errno( int errno);
int	 _get_errno();

# ifdef __cplusplus
}
# endif

# endif /* _IBP_ERRNO_H */
