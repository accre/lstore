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

#pragma once
#ifndef ACCRE_CHKSUM_H_INCLUDED
#define ACCRE_CHKSUM_H_INCLUDED

#include <tbx/toolbox_visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
/*! Generic checksum container (opaque) */
typedef struct tbx_chksum_t tbx_chksum_t;

// Functions
/*! @brief Verifies if a type is valid
 * @param type The type to test
 * @returns 1 if valid, 0 otherwise
 */
TBX_API int tbx_chksum_type_valid(int type);

/*! @brief Initializes checksum
 * @param cs Checksum to initialize
 * @param tbx_chksum_type Type of checksum
 * @returns 0 on success, error from library otherwise
 */
TBX_API int tbx_chksum_set(tbx_chksum_t *cs, int tbx_chksum_type);

/*! @brief Return checksum type corresponding to a given string
 * @param name Name of the checksum we desire
 * @returns Type index if found, -2 otherwise
 */
TBX_API int tbx_chksum_type_name(const char *name);

// Preprocessor macros
/*! @brief Return c-string representing the checksum's algorithm's name
 * @param cs Checksum to examine
 * @returns C-string of the checksum's algorithm's name */
#define tbx_chksum_name(cs) (cs)->name

/*! @brief Return checksum type
 * @param cs Checksum to examine
 * @returns The checksum type index */
#define tbx_chksum_type(cs) (cs)->type

/*! @brief Reset checksum to initial value
 * @param cs Checksum to reset
 * @returns Value from underlying library */
#define tbx_chksum_reset(cs) (cs)->reset((cs)->state)

/*! @brief Return checksum size
 * @param cs Checksum to examine
 * @param type Representation of checksum: CHKSUM_DIGEST_BIN or CHKSUM_DIGEST_HEX
 * @returns Size in bytes */
#define tbx_chksum_size(cs, type) (cs)->size((cs)->state, type)

/*! @brief Retrieve current checksum
 * @param cs Checksum to output
 * @param type Format to return checksum: CHKSUM_DIGEST_BIN or CHKSUM_DIGEST_HEX
 * @param value Pointer to string to begin writing
 * @return 0 on success, error from library otherwise
 * */
#define tbx_chksum_get(cs, type, value)  (cs)->get((cs)->state, type, value)

/*! @brief Add data to checksum
 * @param cs Destination checksum
 * @param size Bytes to add
 * @param data TBuffer containing our data
 * @param doff Offset within the TBuffer to begin reading
 * @returns 0 on success, error from library otherwise
 * */
#define tbx_chksum_add(cs, size, data, doff)  (cs)->add((cs)->state, size, data, doff)

/*! @brief Clear checksum
 * @param cs Checksum to clear */
#define tbx_chksum_clear(cs)  blank_tbx_chksum_set(cs)

/** Size of checksum state */
#define CHKSUM_STATE_SIZE  1024
/** Maximum size of resulting checksum in bytes */
#define CHKSUM_MAX_SIZE    257

// The @defgroup @{ @} syntax makes a group of related comments

/** @defgroup CHECKSUM_DIGEST Checksum return type
    @{
*/
/** Hexidecimal output */
#define CHKSUM_DIGEST_HEX   0
/** Binary output */
#define CHKSUM_DIGEST_BIN   1
/** @} */

/** @defgroup CHECKSUM_ALGORITHM Checksum algorithm
    @{
*/
#define CHKSUM_DEFAULT  -1 /*!< Default checksum(TODO?) */
#define CHKSUM_NONE      0 /*!< No checksum */
#define CHKSUM_SHA256    1 /*!< SHA256 */
#define CHKSUM_SHA512    2 /*!< SHA512 */
#define CHKSUM_SHA1      3 /*!< SHA1 */
#define CHKSUM_MD5       4 /*!< MD5 */
#define CHKSUM_MAX_TYPE  5 /*!< Number of checksums */
#define CHKSUM_TYPE_SIZE  (CHKSUM_MD5+1) /*!< Number of checksums */
/** @} */

// TEMPORARY
#if !defined toolbox_EXPORTS && defined LSTORE_HACK_EXPORT
#   include <tbx/transfer_buffer.h>
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
#endif


#ifdef __cplusplus
}
#endif

#endif
