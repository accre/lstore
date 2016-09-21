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
 * ibp_errno.h
 *
 * Definition of errno handling function and/or variable
 */


# ifndef _IBP_ERRNO_H
# define _IBP_ERRNO_H

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
void ibp_errno_init();

# ifdef __cplusplus
}
# endif

# endif /* _IBP_ERRNO_H */
