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

//
// Documentation example. Inline "example" comments are delimited with double
//  slashies. First example: The following block documents the entire file.
//  Note the special comment /** */, which tells doxygen to pick up the
//  directives inside.
//

/** @file
 *      \file chksum.h
 *      Wraps platform-specific checksumming support
 */

#ifndef __CHKSUM_H_
#define __CHKSUM_H_

#include "tbx/chksum.h"
#include "tbx/toolbox_visibility.h"
#include "tbx/transfer_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

/*! Checksum reset function pointer */
typedef int (*tbx_chksum_reset_fn_t)(void *state);
/*! Checksum sizeof function pointer */
typedef int (*tbx_chksum_size_fn_t)(void *state, int type);
/*! Checksum return function pointer */
typedef int (*tbx_chksum_get_fn_t)(void *state, int type, char *value);
/*! Checksum add data function pointer */
typedef int (*tbx_chksum_add_fn_t)(void *state, int size, tbx_tbuf_t *data, int doff);

/*! Generic Checksum container */
struct tbx_chksum_t{
    // The //!< form tells doxygen to document the PREVIOUS statement instead
    // of the one after. Useful if you want to document values in-line
    char state[CHKSUM_STATE_SIZE];  //!< Used to store state information as an overlay record
    int type;                    //!< Checksum type
    char *name;                  //!< Pointer to the string version of the checksum type
    tbx_chksum_reset_fn_t reset;   //!< Resets checksum to initial value
    tbx_chksum_size_fn_t size;   //!< Size of checksum in bytes
    tbx_chksum_get_fn_t get; //!< Returns the checksum string
    tbx_chksum_add_fn_t add; //!< Adds the data to the checksum
};

#ifdef __cplusplus
}
#endif

#endif


