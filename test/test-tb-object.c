#include "task.h"
#include <tbx/object.h>
#include <stdio.h>

typedef struct {
    tbx_vtable_t base;
    void (*do_counter)(tbx_ref_t *ref);
} test_vtable_t;

typedef struct {
    tbx_obj_t desc;
    int counter;
} test_obj_t;

static int counter = 0;
static void inc_counter(tbx_ref_t *ref) {
    counter++;
}

static void dec_counter(tbx_ref_t *ref) {
    counter--;
}

static void inc_obj_counter(tbx_ref_t *desc) {
	test_obj_t *obj = (test_obj_t *) container_of(desc, tbx_obj_t, refcount);
    (obj->counter)++;
}

static void dec_obj_counter(tbx_ref_t *desc) {
	test_obj_t *obj = (test_obj_t *) container_of(desc, tbx_obj_t, refcount);
    (obj->counter)--;
}

const test_vtable_t vtableA = { .base.name = "TestA",
                                .base.free_fn = inc_obj_counter,
                                .do_counter = inc_counter };

const test_vtable_t vtableB = { .base.name = "TestB",
								.base.free_fn = dec_obj_counter,
                                .do_counter = dec_counter };

TEST_IMPL(tb_object) {
    test_obj_t objA;
    objA.counter = 0;
    objA.desc.vtable = &vtableA.base;
    tbx_ref_init(&objA.desc.refcount);

    test_obj_t objB;
    objB.counter = 0;
    objB.desc.vtable = &vtableB.base;
    tbx_ref_init(&objB.desc.refcount);

    // Test access to vtable base (type punning)
    ASSERT(strcmp("TestA", ((tbx_vtable_t *)objA.desc.vtable)->name) == 0);
    ASSERT(strcmp("TestB", ((tbx_vtable_t *)objB.desc.vtable)->name) == 0);

    // Test proper vtable access
    ((test_vtable_t *) objB.desc.vtable)->do_counter(NULL);
    ASSERT(counter == -1);
    ((test_vtable_t *) objA.desc.vtable)->do_counter(NULL);
    ASSERT(counter == 0);
    ((test_vtable_t *) objB.desc.vtable)->do_counter(NULL);
    ASSERT(counter == -1);
    ((test_vtable_t *) objB.desc.vtable)->do_counter(NULL);
    ASSERT(counter == -2);
    ((test_vtable_t *) objB.desc.vtable)->do_counter(NULL);
    ASSERT(counter == -3);

	// Test refcounting and free pointer deallocation
	tbx_ref_put(&objA.desc.refcount, ((tbx_vtable_t *) objA.desc.vtable)->free_fn);
	tbx_ref_put(&objB.desc.refcount, ((tbx_vtable_t *) objB.desc.vtable)->free_fn);
    ASSERT(objA.counter == 1);
    ASSERT(objB.counter == -1);

    return 0;
}

TEST_IMPL(tb_object_api) {
    test_obj_t objA;
    objA.counter = 0;
    tbx_obj_init(&objA.desc, (tbx_vtable_t *) &vtableA);
    ASSERT(objA.counter == 0);
    
    tbx_obj_t *ret = tbx_obj_get(&objA.desc);
    ASSERT(objA.counter == 0);
    ASSERT(ret = &objA.desc);

    tbx_obj_put(&objA.desc);
    ASSERT(objA.counter == 0);
    
    tbx_obj_put(&objA.desc);
    ASSERT(objA.counter == 1);

    return 0;
}
