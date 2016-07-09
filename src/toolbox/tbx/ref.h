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
#ifndef ACCRE_REF_H_INCLUDED
#define ACCRE_REF_H_INCLUDED

#include <stdbool.h>
#include <tbx/atomic_counter.h>

// Types
/*! Typedef around reference count */
typedef struct tbx_ref_t tbx_ref_t;

/*! Function to be called when refcount reaches zero */
typedef void (*tbx_ref_release_fn_t)(tbx_ref_t *ref);

// Exported types. OK to be exported
/*! @brief Holds refcount-related variables
 *
 * This is a typedef around a struct and not just a bare atomic integer to
 * enforce type-safety, and to allow the option to later add different debug
 * fields based on #defines to trace and verify proper reference counting. For
 * instance a guard flag that makes sure tbx_ref_init was called before any
 * gets or puts were sent to the refcount
 */
struct tbx_ref_t {
    /*! Actual holder of the reference counter */
    /* FIXME: This volatile should be within the atomic typedef */
    volatile tbx_atomic_unit32_t refcount;
};

// Inline Functions
/*! @brief Properly initializes reference count
 * @param ref Refcount to initialize
 * This function is necessary because any cross-CPU caches need to be atomically
 * updated to be notified. Otherwise cache coherency can act up
 */
static inline void tbx_ref_init(tbx_ref_t *ref) {
    tbx_atomic_set(ref->refcount, 1);
}

/*! @brief Grabs a new reference incrementing the reference count
 * @param ref Refcount to increment
 * @returns Pointer to refcount
 */
static inline tbx_ref_t *tbx_ref_get(tbx_ref_t *ref) {
    tbx_atomic_inc(ref->refcount);
    return ref;
}

/*! @brief Decrements a reference count, cleaning up the object if necessary
 * @param ref Counter to decremnt
 * @param cleanup Function to call to destroy
 * @returns True if object was removed, false otherwise
 */
static inline bool tbx_ref_put(tbx_ref_t *ref, tbx_ref_release_fn_t cleanup) {
    if (tbx_atomic_dec(ref->refcount) == 0) {
        cleanup(ref);
        return true;
    } else {
        return false;
    }
}

// Precompiler Macros
#define container_of(ptr, type, member) \
  ((type *) ((char *) (ptr) - offsetof(type, member)))

#endif
