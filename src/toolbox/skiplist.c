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

#define _log_module_index 104

#include <stdlib.h>
#include <assert.h>
#include "log.h"
#include "skiplist.h"

int skiplist_compare_fn_int(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);
int skiplist_compare_fn_strcmp(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);
int skiplist_compare_fn_strncmp(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);
int skiplist_compare_fn_ptr(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);

skiplist_compare_t skiplist_compare_int= {skiplist_compare_fn_int, (void *)(int)(long)(1)};
skiplist_compare_t skiplist_compare_strcmp= {skiplist_compare_fn_strcmp, (void *)(int)(long)1};
skiplist_compare_t skiplist_compare_strcmp_descending= {skiplist_compare_fn_strcmp, (void *)(int)(long)(-1)};
skiplist_compare_t skiplist_compare_ptr= {skiplist_compare_fn_ptr, NULL};

//*********************************************************************************
// Routines to handle NULL dup and free
//*********************************************************************************

skiplist_key_t *sl_passthru_dup(skiplist_key_t *key)
{
    return(key);
}
void sl_no_key_free(skiplist_key_t *key)
{
    log_printf(15, "key p=%p\n", key);
    return;
}
void sl_no_data_free(skiplist_data_t *data)
{
    log_printf(15, "data p=%p\n", data);
    return;
}
void sl_simple_free(skiplist_data_t *data)
{
    log_printf(15, "p=%p\n", data);
    free(data);
}

skiplist_key_t *sl_ptr_dup(skiplist_key_t *key)
{
    skiplist_key_t *d = (skiplist_key_t *)malloc(sizeof(skiplist_key_t));
    d = key;
    return(d);
}

skiplist_key_t *sl_string_dup(skiplist_key_t *key)
{
    char *dup = strdup((char *)key);
    return((skiplist_key_t *)dup);
}


//*********************************************************************************
//  List of simple compare routines
//*********************************************************************************

int skiplist_compare_fn_ptr(void *arg, skiplist_key_t *a, skiplist_key_t *b)
{
    int cmp = 1;

    if (a < b) {
        cmp = -1;
    } else if ( a == b) {
        cmp = 0;
    }

    log_printf(15, "skiplist_compare_ptr: cmp(%p,%p)=%d\n", a, b, cmp);
    return(cmp);
}

//*********************************************************************************

int skiplist_compare_fn_int(void *arg, skiplist_key_t *k1, skiplist_key_t *k2)
{
    int scale = (int)(long)arg;

    int *a = (int *)k1;
    int *b = (int *)k2;
    int cmp = 1;

    if (*a < *b) {
        cmp = -1;
    } else if ( *a == *b) {
        cmp = 0;
    }

    cmp = scale * cmp;

    log_printf(15, "skiplist_compare_int: cmp(%d,%d)=%d scale=%d\n", *a, *b, cmp, scale);
    return(cmp);
}

//*********************************************************************************

int skiplist_compare_fn_strcmp(void *arg, skiplist_key_t *k1, skiplist_key_t *k2)
{
    int scale = (int)(long)arg;

    char *a = (char *)k1;
    char *b = (char *)k2;

    return(scale*strcmp(a,b));
}

//*********************************************************************************

int skiplist_compare_fn_strncmp(void *arg, skiplist_key_t *k1, skiplist_key_t *k2)
{
    char *a = (char *)k1;
    char *b = (char *)k2;
    int n = (int)(long)arg;  //****************HACKERY INVOLVED***********************

    return(strncmp(a,b, n));
}

void skiplist_strncmp_set(skiplist_compare_t *compare, int n)
{
    long l = n;
    compare->fn = skiplist_compare_fn_strncmp;
    compare->arg = (void *)l;    //****************HACKERY INVOLVED***********************
}

//*********************************************************************************
//  create_skiplist_ele - Creates a skiplist element
//*********************************************************************************

skiplist_ele_t *create_skiplist_ele()
{
    skiplist_ele_t *se = (skiplist_ele_t *)malloc(sizeof(skiplist_ele_t));
    assert(se != 0);

    return(se);
}

//*********************************************************************************
//  create_skiplist_node - Creates a new skiplist node
//*********************************************************************************

skiplist_node_t *create_skiplist_node(int level)
{
    skiplist_node_t *sn = (skiplist_node_t *)malloc(sizeof(skiplist_node_t));
    assert(sn != NULL);

    sn->level = level;
    sn->key = NULL;
    sn->next = (skiplist_node_t **)malloc(sizeof(skiplist_node_t *)*(level+1));
    assert(sn->next != NULL);
    memset(sn->next, 0, sizeof(skiplist_node_t *)*(level+1));

    sn->ele.data = NULL;
    sn->ele.next = NULL;

    return(sn);
}

//*********************************************************************************
// destroy_skiplist_node - Destroys a skiplist node
//*********************************************************************************

void destroy_skiplist_node(skiplist_t *sl, skiplist_node_t *sn)
{
    skiplist_ele_t *se2;
    skiplist_ele_t *se = &(sn->ele);

    log_printf(15, "destroying node\n");

    sl->data_free(se->data);

    se = se->next;
    while (se != NULL) {
        se2 = se;
        se = se->next;
        sl->data_free(se2->data);
        free(se2);
    }

    sl->key_free(sn->key);

    free(sn->next);
    free(sn);
    return;
}

//*********************************************************************************
// skiplist_key_count - Returns the number of keys in the skiplist
//*********************************************************************************

int skiplist_key_count(skiplist_t *sl)
{
    return(sl->n_keys);
}


//*********************************************************************************
// skiplist_element_count - Returns the number of elements in the skiplist
//*********************************************************************************

int skiplist_element_count(skiplist_t *sl)
{
    return(sl->n_ele);
}

//*********************************************************************************
//  create_skiplist_full - Creates a new skip list
//*********************************************************************************

skiplist_t *create_skiplist_full(int maxlevels, double p, int allow_dups,
                                 skiplist_compare_t *compare,
                                 skiplist_key_t *(*dup)(skiplist_key_t *a),
                                 void (*key_free)(skiplist_key_t *a),
                                 void (*data_free)(skiplist_data_t *a))
{
    skiplist_t *sl = (skiplist_t *)malloc(sizeof(skiplist_t));
    assert(sl != NULL);

    { int result = apr_pool_create(&(sl->pool), NULL); assert(result == APR_SUCCESS); }
    { int result = apr_thread_mutex_create(&(sl->lock), APR_THREAD_MUTEX_DEFAULT, sl->pool); assert(result == APR_SUCCESS); }

    sl->n_keys = 0;
    sl->n_ele = 0;
    sl->current_max = 0;
    sl->max_levels = maxlevels;
    sl->p = p;
    sl->allow_dups = allow_dups;
    sl->compare = compare;
    sl->dup = (dup == NULL) ? sl_passthru_dup : dup;
    sl->key_free = (key_free == NULL) ? sl_no_key_free : key_free;
    sl->data_free = (data_free == NULL) ? sl_no_data_free : data_free;
    sl->head = create_skiplist_node(maxlevels-1);

    return(sl);
}

//*********************************************************************************
// create_skiplist - Shortcut to create a new skiplist using default values
//*********************************************************************************

skiplist_t *create_skiplist(int allow_dups,
                            skiplist_compare_t *compare,
                            skiplist_key_t *(*dup)(skiplist_key_t *a),
                            void (*key_free)(skiplist_key_t *a),
                            void (*data_free)(skiplist_data_t *a))
{
    return(create_skiplist_full(16, 0.25, allow_dups, compare, dup, key_free, data_free));
}

//*********************************************************************************
// empty_skiplist - Empties the skiplist of keys but leaves the head node
//*********************************************************************************

void empty_skiplist(skiplist_t *sl)
{
    skiplist_node_t *sn, *sn2;

    log_printf(15, "sl=%p\n", sl);

    //** Reset a few things
    sl->n_keys = 0;
    sl->n_ele = 0;
    sl->current_max = 0;

    //**Blank the head node
    sn = sl->head;
    sn2 = sn->next[0];
    memset(sn->next, 0, sizeof(skiplist_node_t *)*(sl->max_levels));

    //** and delete everything else
    sn = sn2;
    while (sn != NULL) {
        sn2 = sn;
        sn = sn->next[0];
//    sl->key_free(sn2->key);
        destroy_skiplist_node(sl, sn2);
    }

    return;
}

//*********************************************************************************
// destroy_skiplist - Destroys a skiplist
//*********************************************************************************

void destroy_skiplist(skiplist_t *sl)
{
    log_printf(15, "destroy_skiplist: sl=%p\n", sl);
    empty_skiplist(sl);

    destroy_skiplist_node(sl, sl->head);

    apr_thread_mutex_destroy(sl->lock);
    apr_pool_destroy(sl->pool);

    free(sl);

    return;
}


//*********************************************************************************
// get_random_level - Returns the nodes random level
//*********************************************************************************

int get_random_level(int max_level, double p, int current_max)
{
    int level = 0;
    double r;

    max_level--;
    r = RAND_MAX+1.0;
    r = (1.0*rand()) / r;
    while ((r < p) && (level < max_level)) {
        r = RAND_MAX+1.0;
        r = (1.0*rand()) / r;
        level++;
    }

    if (level > current_max) level = current_max+1;
    return(level);
}

//*********************************************************************************
// find_key_compare - Finds the key or the nearest insertion point using the supplied
//    comparison routine.
//*********************************************************************************

int find_key_compare(skiplist_t *sl, skiplist_node_t **ptr, skiplist_key_t *key, skiplist_compare_t *compare, int fixed_cmp)
{
    int cmp, i;
    skiplist_node_t *sn;

    cmp = -1;
    sn = sl->head;
    log_printf(15, "find_key: start head=%p\n", sn);
    for (i=sl->current_max; i>=0; i--) {
        cmp = -1;
        while ((cmp < 0) && (sn->next[i] != NULL)) {
            cmp = (key == NULL) ? fixed_cmp : compare->fn(compare->arg, sn->next[i]->key, key);
            log_printf(15, "find_key: -->ptr[%d]->next=%p cmp=%d\n", i,sn->next[i], cmp);
            if (cmp < 0) sn = sn->next[i];
        }

        log_printf(15, "find_key: ptr[%d]=%p\n", i,sn);
        ptr[i] = sn;
    }

    return(cmp);
}


//*********************************************************************************
// skiplist_first_key - Returns the 1st key in the list
//*********************************************************************************

skiplist_key_t *skiplist_first_key(skiplist_t *sl)
{
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    if (sl->n_keys <= 0) return(NULL);

    memset(ptr, 0, sizeof(ptr));
    find_key(sl, ptr, NULL, 1);
    return(ptr[0]->next[0]->key);
}

//*********************************************************************************
// skiplist_last_key - Returns the last key in the list
//*********************************************************************************

skiplist_key_t *skiplist_last_key(skiplist_t *sl)
{
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    if (sl->n_keys <= 0) return(NULL);

    memset(ptr, 0, sizeof(ptr));
    find_key(sl, ptr, NULL, -1);
    return(ptr[0]->key);
}

//*********************************************************************************
//  pos_insert_skiplist - Inserts a new node to the skiplist at the current position
//    and returns the new node
//*********************************************************************************

skiplist_node_t *pos_insert_skiplist(skiplist_t *sl, skiplist_node_t **ptr, skiplist_key_t *key, skiplist_data_t *data)
{
    skiplist_node_t *sn2;
    int level, i;

    //** New node has to be inserted
    level = get_random_level(sl->max_levels, sl->p, sl->current_max);
    sn2 = create_skiplist_node(level);

    log_printf(15, "insert_skiplist: new node.  level=%d\n", level);
    if (level > sl->current_max) {  //** Need to adjust the head to handle the higher levels
        for (i=sl->current_max+1; i<=level; i++) {
            ptr[i] = sl->head;
        }

        sl->current_max = level;
    }

    for (i=0; i<=level; i++) {
        sn2->next[i] = ptr[i]->next[i];
        ptr[i]->next[i] = sn2;
    }

    //** Finally store the data
    sn2->key = sl->dup(key);
    sn2->ele.data = data;

    return(sn2);
}


//*********************************************************************************
// insert_skiplist - Inserts the key into the skiplist
//*********************************************************************************

int insert_skiplist(skiplist_t *sl, skiplist_key_t *key, skiplist_data_t *data)
{
    int cmp;
    skiplist_ele_t *se;
    skiplist_node_t *sn;
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];

    memset(ptr, 0, sizeof(ptr));
    cmp = find_key(sl, ptr, key, 0);
    if (cmp == 0) {  //** Already a node with this value
        sn = ptr[0]->next[0];
        if (sl->allow_dups == 0) {
            sl->data_free(sn->ele.data);  //** So free the old data 1st
            sn->ele.data = data;
        } else {
            se = create_skiplist_ele();
            se->data = data;
            se->next = sn->ele.next;
            sn->ele.next = se;
            sl->n_ele++;
        }

        return(0);
    }

    sl->n_keys++;
    sl->n_ele++;
    sn = pos_insert_skiplist(sl, ptr, key, data);
    if (sn == NULL) return(-1);

    return(0);
}


//*********************************************************************************
// remove_skiplist - Removes an element from the skiplist
//     If data == NULL then all elements for the matching key are removed.
//     Otherwise only the element matching the data pointer is removed.
//     If the element(key/data) can't be located 1 is returned. Success returns 0.
//*********************************************************************************

int remove_skiplist(skiplist_t *sl, skiplist_key_t *key, skiplist_data_t *data)
{
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    skiplist_node_t *sn2;
    skiplist_ele_t *se, *se2, *prev;
    int cmp, i, empty_node, found;

    found = 0;
    empty_node = 0;
    memset(ptr, 0, sizeof(ptr));
    cmp = find_key(sl, ptr, key, 0);

    if (cmp != 0) {
        return(1);    //** No match so return
    }

    log_printf(15, "remove_skiplist: list=%p\n", sl);

    se = &(ptr[0]->next[0]->ele);
    if (data == NULL) {  //** Free all the data blocks
        found = 1;
        sl->n_ele--;
        if (se->data != NULL) sl->data_free(se->data);
        se = se->next;
        while (se != NULL) {
            sl->n_ele--;
            sl->data_free(se->data);
            se2 = se;
            se = se->next;
            free(se2);
        }

        empty_node = 1;
    } else {  //** Just free the block in question
        if (se->data == data) {   //** Head element is a match
            found = 1;
            sl->n_ele--;
            if (se->data != NULL) {
                sl->data_free(se->data);
            }
            se->data = NULL;
            if (se->next != NULL) {
                se2 = se->next;
                se->data = se2->data;
                se->next = se2->next;
                free(se2);
            } else {
                empty_node = 1;
            }
        } else {  //** Have to scan the list for a match
            prev = se;
            se = se->next;
            while ((se != NULL) && (found == 0)) {
                if (se->data == data) {
                    found = 1;
                    sl->n_ele--;
                    if (se->data != NULL) sl->data_free(se->data);
                    se2 = se->next;
                    prev->next = se2;
                    free(se);
                } else {
                    prev = se;
                    se = se->next;
                }
            }
        }

    }

    if (empty_node == 1) {
        sn2 = ptr[0]->next[0];
        for (i=0; i<=sn2->level; i++) {
            ptr[i]->next[i] = sn2->next[i];
        }

        if (sn2->level == sl->current_max) {
            i = sl->current_max;
            while ((i>0) && (sl->head->next[i] == NULL)) {
                i--;
            }
            sl->current_max = i;
        }

        sl->n_keys--;
        destroy_skiplist_node(sl, sn2);
    }

    found = (found == 1) ? 0 : 1;
    return(found);
}

//*********************************************************************************
//  iter_search_skiplist_compare - Returns an iterator starting at the requested key
//     or the next smallest key.
//    If key == NULL then it start at the 1st key
//*********************************************************************************

skiplist_iter_t OLD_iter_search_skiplist_compare(skiplist_t *sl, skiplist_key_t *key, skiplist_compare_t *compare)
{
    skiplist_iter_t it;

    it.sl = sl;
    it.ele = NULL;
    it.curr = NULL;
    it.prev = NULL;
    it.sn = NULL;
    it.compare = compare;
    memset(it.ptr, 0, sizeof(it.ptr));
    find_key_compare(sl, it.ptr, key, compare, 1);
    log_printf(15, "iter_search_skiplist: it.sn=%p\n", it.ptr[0]->next[0]);

    if (it.ptr[0]->next[0] != NULL) {
        it.sn = it.ptr[0]->next[0];
        it.ele = &(it.sn->ele);
    }

    return(it);
}

//*********************************************************************************
//  iter_search_skiplist_compare - Returns an iterator starting at the requested key
//     or the next smallest key.
//    If key == NULL then it start at the 1st key
//*********************************************************************************

skiplist_iter_t iter_search_skiplist_compare(skiplist_t *sl, skiplist_key_t *key, skiplist_compare_t *compare, int round_mode)
{
    skiplist_iter_t it;
    int cmp;

    it.sl = sl;
    it.ele = NULL;
    it.curr = NULL;
    it.prev = NULL;
    it.sn = NULL;
    it.compare = compare;
    memset(it.ptr, 0, sizeof(it.ptr));
    cmp = find_key_compare(sl, it.ptr, key, compare, 0);
    log_printf(15, "it.ptr->next=%p\n", it.ptr[0]->next[0]);

    if ((round_mode < -1) || ((round_mode < 0) && (cmp != 0))) {
        if (it.ptr[0] != NULL) {
//log_printf(15, "returning prev\n");
            it.sn = it.ptr[0];
            it.ele = &(it.sn->ele);
        } else if (it.ptr[0]->next[0] != NULL) {
//log_printf(15, "returning next\n");
            it.sn = it.ptr[0]->next[0];
            it.ele = &(it.sn->ele);
        }
//  } else if (round_mode == 0) {
    } else {
        if (it.ptr[0]->next[0] != NULL) {
            it.sn = it.ptr[0]->next[0];
            it.ele = &(it.sn->ele);
        }
//  } else {
//     if (it.ptr[0]->next[0] != NULL) {
//        if (it.ptr[0]->next[0]->next[0] != NULL) {
//           it.sn = it.ptr[0]->next[0]->next[0];
//           it.ele = &(it.sn->ele);
//        }
//     }
    }

    log_printf(15, "it.sn=%p\n", it.sn);

    return(it);
}

//*********************************************************************************
// search_skiplist_compare - Just returns the 1st matching data element
//*********************************************************************************

skiplist_data_t *search_skiplist_compare(skiplist_t *sl, skiplist_key_t *key, skiplist_compare_t *compare)
{
    skiplist_data_t *data;
    int cmp;
    skiplist_iter_t it = iter_search_skiplist_compare(sl, key, compare, 0);

    data = NULL;
    if (it.sn != NULL) {
        cmp = compare->fn(compare->arg, it.sn->key, key);
        if (cmp == 0) {
            data = it.sn->ele.data;
        }
    }

    return(data);
}


//*********************************************************************************
// next_skiplist - Returns the next key/data pair
//*********************************************************************************

int next_skiplist(skiplist_iter_t *it, skiplist_key_t **nkey, skiplist_data_t **ndata)
{
    int i;
    skiplist_node_t *sn2;

    if (it->ele == NULL) { //** Nothing left for this key so skip to the next one
        if (it->sn == NULL) {
            it->curr = NULL;
            *nkey = NULL;
            *ndata = NULL;
            log_printf(15, "next_skiplist: it->ele = NULL\n");
            return(1);
        }

        sn2 = it->sn;
        it->sn = it->sn->next[0];
        if (it->sn != NULL) {
            log_printf(15, "next_skiplist: next sn it->sn->level=%d\n", it->sn->level);

            it->ele = &(it->sn->ele);
            if (sn2 != NULL) for (i=0; i<=sn2->level; i++) it->ptr[i] = sn2->next[i];
        } else {
            it->curr = NULL;
            *nkey = NULL;
            *ndata = NULL;
            log_printf(15, "next_skiplist: it->sn = NULL\n");
            return(1);
        }
    }

    //** Get the next element
    *nkey = it->sn->key;
    *ndata = it->ele->data;
    log_printf(15, "next_skiplist: OK level=%d ptr=%p\n", it->sn->level, it->sn);

    //** and inc the pointers
    it->prev = it->curr;
    it->curr = it->ele;
    it->ele = it->ele->next;

    return(0);
}


//*********************************************************************************
// iter_remove_skiplist - Removes the current skiplist element
//*********************************************************************************

int iter_remove_skiplist(skiplist_iter_t *it)
{
    int empty_node, i;
    skiplist_ele_t *se;
    skiplist_node_t *sn2;

    if (it->curr == NULL) return(1);

    empty_node = 0;
    if (it->curr == &(it->sn->ele)) { //** Deleting the initial node
        it->sn->ele.data = NULL;
        if (it->sn->ele.next != NULL) {
            se = it->sn->ele.next;
            it->sn->ele.data = se->data;
            it->sn->ele.next = se->next;
            it->sl->n_ele--;
            it->sl->data_free(se->data);
            free(se);

            it->ele = &(it->sn->ele);
        } else {
            empty_node = 1;
        }
    } else {  //** Normal deletion
        it->prev->next = it->curr->next;
        it->sl->n_ele--;
        if (it->curr->data != NULL) it->sl->data_free(it->curr->data);
        free(it->curr);
    }


    //** Now see if we contract the list and remove the node
    if (empty_node == 1) {
        sn2 = it->sn;
        for (i=0; i<=sn2->level; i++) {
            it->ptr[i]->next[i] = sn2->next[i];
        }

        if (sn2->level == it->sl->current_max) {
            i = it->sl->current_max;
            while ((i>0) && (it->sl->head->next[i] == NULL)) {
                i--;
            }
            it->sl->current_max = i;
        }

        it->sl->n_keys--;
        destroy_skiplist_node(it->sl, sn2);

        //** Advance the iterator
        it->sn = it->ptr[0]->next[0];
        it->ele = &(it->sn->ele);
        it->prev = NULL;
    }

    it->curr = NULL;

    return(0);
}

