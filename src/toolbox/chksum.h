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

#include "tbx/toolbox_visibility.h"
#include "transfer_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif


// The /** */ comment documents the succeeding line by default

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

// Alternate form of documentation: /*! */

/*! Checksum reset function pointer */
typedef int (*chksum_reset_fn_t)(void *state);
/*! Checksum sizeof function pointer */
typedef int (*chksum_size_fn_t)(void *state, int type);
/*! Checksum return function pointer */
typedef int (*chksum_get_fn_t)(void *state, int type, char *value);
/*! Checksum add data function pointer */
typedef int (*chksum_add_fn_t)(void *state, int size, tbuffer_t *data, int doff);


/*! Generic Checksum container */
typedef struct {
    // The //!< form tells doxygen to document the PREVIOUS statement instead
    // of the one after. Useful if you want to document values in-line
    char state[CHKSUM_STATE_SIZE];  //!< Used to store state information as an overlay record
    int type;                    //!< Checksum type
    char *name;                  //!< Pointer to the string version of the checksum type
    chksum_reset_fn_t reset;   //!< Resets checksum to initial value
    chksum_size_fn_t size;   //!< Size of checksum in bytes
    chksum_get_fn_t get; //!< Returns the checksum string
    chksum_add_fn_t add; //!< Adds the data to the checksum
} chksum_t;

//** Provide usage shortcuts
/*! @brief Return c-string representing the checksum's algorithm's name
 * @param cs Checksum to examine
 * @returns C-string of the checksum's algorithm's name */
#define chksum_name(cs) (cs)->name
/*! @brief Return checksum type
 * @param cs Checksum to examine
 * @returns The checksum type index */
#define chksum_type(cs) (cs)->type
/*! @brief Reset checksum to initial value
 * @param cs Checksum to reset
 * @returns Value from underlying library */
#define chksum_reset(cs) (cs)->reset((cs)->state)
/*! @brief Return checksum size
 * @param cs Checksum to examine
 * @param type Representation of checksum: CHKSUM_DIGEST_BIN or CHKSUM_DIGEST_HEX
 * @returns Size in bytes */
#define chksum_size(cs, type) (cs)->size((cs)->state, type)
/*! @brief Retrieve current checksum
 * @param cs Checksum to output
 * @param type Format to return checksum: CHKSUM_DIGEST_BIN or CHKSUM_DIGEST_HEX
 * @param value Pointer to string to begin writing
 * @return 0 on success, error from library otherwise
 * */
#define chksum_get(cs, type, value)  (cs)->get((cs)->state, type, value)
/*! @brief Add data to checksum
 * @param cs Destination checksum
 * @param size Bytes to add
 * @param data TBuffer containing our data
 * @param doff Offset within the TBuffer to begin reading
 * @returns 0 on success, error from library otherwise
 * */
#define chksum_add(cs, size, data, doff)  (cs)->add((cs)->state, size, data, doff)
/*! @brief Clear checksum
 * @param cs Checksum to clear */
#define chksum_clear(cs)  blank_chksum_set(cs)

/*! @brief Given a binary string, return the hexidecimal representation
 *
 * @param in_size Length of input string
 * @param in Input string
 * @param out String to write to
 * @returns Always returns zero
 **/
int convert_bin2hex(int in_size, const unsigned char *in, char *out);

/*! @brief Verifies if a type is valid
 * @param type The type to test
 * @returns 1 if valid, 0 otherwise
 */
TBX_API int chksum_valid_type(int type);

/*! @brief Initializes checksum
 * @param cs Checksum to initialize
 * @param chksum_type Type of checksum
 * @returns 0 on success, error from library otherwise
 */
TBX_API int chksum_set(chksum_t *cs, int chksum_type);

/*! @brief Return checksum type corresponding to a given string
 * @param name Name of the checksum we desire
 * @returns Type index if found, -2 otherwise
 */
TBX_API int chksum_name_type(const char *name);

/*! @brief Initializes cs to a completely blank checksum
 * @param cs Checksum struct to initialize
 * @returns Always returns zero
 */
int blank_chksum_set(chksum_t *cs);

#ifdef __cplusplus
}
#endif

#endif


