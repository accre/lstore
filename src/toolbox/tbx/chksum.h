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

#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
/*! Generic checksum container (opaque) */
typedef struct tbx_chksum_t tbx_chksum_t;
typedef enum tbx_chksum_type_t tbx_chksum_type_t;
typedef enum tbx_chksum_digest_output_t tbx_chksum_digest_output_t;

// Functions

/*! @brief Converts binary number to hex
 * @param in_size size of input binary number
 * @param in binary value in
 * @param out Hex value output
 * @returns 0 on success, error from library otherwise
 */
TBX_API int tbx_chksum_bin2hex(int in_size, const unsigned char *in, char *out);

/*! @brief Verifies if a type is valid
 * @param type The type to test
 * @returns 1 if valid, 0 otherwise
 */
TBX_API int tbx_chksum_type_valid(tbx_chksum_type_t type);

/*! @brief Initializes checksum
 * @param cs Checksum to initialize
 * @param tbx_chksum_type Type of checksum
 * @returns 0 on success, error from library otherwise
 */
TBX_API int tbx_chksum_set(tbx_chksum_t *cs, tbx_chksum_type_t tbx_chksum_type);

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
#define tbx_chksum_clear(cs)  tbx_blank_chksum_set(cs)
TBX_API int tbx_blank_chksum_set(tbx_chksum_t *cs);

/** Size of checksum state */
#define CHKSUM_STATE_SIZE  1024
/** Maximum size of resulting checksum in bytes */
#define CHKSUM_MAX_SIZE    257

/*! Checksum return type */
enum tbx_chksum_digest_output_t {
    /** Hexidecimal output */
    CHKSUM_DIGEST_HEX,
    /** Binary output */
    CHKSUM_DIGEST_BIN
};

/*! @brief Checksum algorithm */
enum tbx_chksum_type_t {
    CHKSUM_DEFAULT, /*!< Default checksum(TODO?) */
    CHKSUM_NONE,    /*!< No checksum */
    CHKSUM_SHA256,  /*!< SHA256 */
    CHKSUM_SHA512,  /*!< SHA512 */
    CHKSUM_SHA1,    /*!< SHA1 */
    CHKSUM_MD5,     /*!< MD5 */
    CHKSUM_MAX_TYPE /*!< Number of checksums */
};

// TEMPORARY
#include <tbx/transfer_buffer.h>
/*! Checksum reset function pointer */
typedef int (*tbx_chksum_reset_fn_t)(void *state);
/*! Checksum sizeof function pointer */
typedef int (*tbx_chksum_size_fn_t)(void *state, tbx_chksum_digest_output_t type);
/*! Checksum return function pointer */
typedef int (*tbx_chksum_get_fn_t)(void *state, tbx_chksum_digest_output_t type, char *value);
/*! Checksum add data function pointer */
typedef int (*tbx_chksum_add_fn_t)(void *state, int size, tbx_tbuf_t *data, int doff);


/*! Generic Checksum container */
struct tbx_chksum_t{
    // The //!< form tells doxygen to document the PREVIOUS statement instead
    // of the one after. Useful if you want to document values in-line
    char state[CHKSUM_STATE_SIZE];  //!< Used to store state information as an overlay record
    tbx_chksum_type_t type;         //!< Checksum type
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
