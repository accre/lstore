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
#ifndef ACCRE_OBJECT_H_INCLUDED
#define ACCRE_OBJECT_H_INCLUDED

#include <tbx/ref.h>

// Types
typedef struct tbx_vtable_t tbx_vtable_t;
typedef struct tbx_obj_t tbx_obj_t;

// Exported types. OK to be exported
/*!
 * A single instance of this exists per type. It MUST be the first element of
 * the type's vtable to allow type punning to work.
 */
struct tbx_vtable_t {
    /*! Function to be called when refcount reaches zero */
    tbx_ref_release_fn_t free_fn;
    /*! Human printable name of this type */
    char *name;
};

/*!
 * Each instance contains this object. It MUST be the first element of the
 * object to allow type punning for opaque types
 */
struct tbx_obj_t {
    const tbx_vtable_t *vtable;
    tbx_ref_t refcount;
};

// Inline functions
/*!
 * @brief Properly initializes reference count
 * @param obj Object to initialize
 * @param vtable The desired vtable for the object to use
 * This function is necessary because any cross-CPU caches need to be atomically
 * updated to be notified. Otherwise cache coherency can act up
 */
static inline void tbx_obj_init(tbx_obj_t *obj, const tbx_vtable_t *vtable) {
    tbx_ref_init(&obj->refcount);
    obj->vtable = vtable;
}

/*! 
 * @brief Grabs a new reference incrementing the reference count
 * @param ref Refcount to increment
 * @returns Pointer to refcount
 */
static inline tbx_obj_t *tbx_obj_get(tbx_obj_t *obj) {
    tbx_ref_get(&obj->refcount);
    return obj;
}

/*!
 * @brief Decrements a reference count, cleaning up the object if necessary
 * @param ref Counter to decremnt
 * @param cleanup Function to call to destroy
 * @returns True if object was removed, false otherwise
 */
static inline bool tbx_obj_put(tbx_obj_t *obj) {
    return tbx_ref_put(&obj->refcount, obj->vtable->free_fn);
}


#endif
