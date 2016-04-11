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

#define _log_module_index 105

#include <stdlib.h>
#include <assert.h>
#include "assert_result.h"
#include "log.h"
#include "skiplist.h"
#include "interval_skiplist.h"


//*********************************************************************************
//  append_isl_data - Appends an interval to the ISL node
//*********************************************************************************

void append_isl_data(isl_data_t **list, skiplist_data_t *data)
{
    isl_data_t *d = (isl_data_t *)malloc(sizeof(isl_data_t));
    assert(d != NULL);

//log_printf(15, "append_isl_data: list=%p data=%p\n", *list, data);
    d->data = data;
    d->next = *list;
    *list = d;
}

//*********************************************************************************
// copy_isl_data - Copies the ISL data chain between lists
//*********************************************************************************

void copy_isl_data(isl_data_t *src, isl_data_t **dest)
{
    isl_data_t *d;

    if (src == NULL) return;

    for (d = src; d != NULL; d = d->next) {
        append_isl_data(dest, d->data);
    }

}

//*********************************************************************************
//  copy_isl_node_data - Copies the ISL node data lists
//*********************************************************************************

void copy_isl_node_data(int src_level, isl_node_t *src, int dest_level, isl_node_t *dest)
{
    int level, i;

    level = (src_level > dest_level) ? src_level : dest_level;

    copy_isl_data(src->point, &(dest->point));

    for (i=0; i<=level; i++) copy_isl_data(src->edge[i], &(dest->edge[i]));
}

//*********************************************************************************
// free_isl_data - Frees the memory associated with an isl_data list
//*********************************************************************************

void free_isl_data(isl_data_t *data)
{
    isl_data_t *curr, *prev;

    curr = data;
    while (curr != NULL) {
        prev = curr;
        curr = curr->next;
        free(prev);
    }

}

//*********************************************************************************
// remove_isl_node - Free's all the memory associsated with the ISL node
//*********************************************************************************

void remove_isl_node(int level, isl_node_t *node)
{
    int i;

    free_isl_data(node->start);
    free_isl_data(node->point);

    for (i=0; i<=level; i++) free_isl_data(node->edge[i]);
    free(node->edge);
    free(node);
}

//*********************************************************************************
//  remove_isl_data - Removes an interval from the ISL node
//*********************************************************************************

int remove_isl_data(interval_skiplist_t *isl, isl_data_t **list, skiplist_data_t *data)
{
    isl_data_t *curr, *prev;

    prev = NULL;
    curr = *list;
    while (curr != NULL) {
        if (curr->data == data) break;
        prev = curr;
        curr = curr->next;
    }

    if (curr == NULL) return(1);  //** Didn't find it

    if (curr == *list) {  //** Head node
        *list = curr->next;
    } else if (prev) {
        prev->next = curr->next;
    }

    free(curr);

    return(0);
}

//*********************************************************************************
//  isl_node_is_epmty - Returns 1 is the node contains no data
//*********************************************************************************

int isl_node_is_empty(isl_node_t *isln, int level)
{
    if (isln->point != NULL) return(0);
    if (isln->start != NULL) return(0);
    if (isln->n_end > 0) return(0);

//  for(i=0; i<=level; i++) {
//    if (isln->edge[i] != NULL) return(0);
//  }

    return(1);
}


//*********************************************************************************
//  add_isl_node_level - Adds the level pointers to the node
//*********************************************************************************

int add_isl_node_level(isl_node_t *isln, int level)
{
    isln->edge = (isl_data_t **)malloc(sizeof(isl_data_t *)*(level+1));
    assert(isln->edge != NULL);
    memset(isln->edge, 0, sizeof(isl_data_t *)*(level+1));

    return(0);
}

//*********************************************************************************
//  create_isl_node - Creates a new interval skiplist node
//*********************************************************************************

isl_node_t *create_isl_node()
{
    isl_node_t *isln = (isl_node_t *)malloc(sizeof(isl_node_t));
    assert(isln != NULL);

    memset(isln, 0, sizeof(isl_node_t));

    return(isln);
}

//*********************************************************************************
// destroy_skiplist_node - Destroys a skiplist node
//*********************************************************************************

void destroy_isl_node(isl_node_t *isln)
{
    if (isln->edge != NULL) free(isln->edge);
    free(isln);

    return;
}

//*********************************************************************************
// interval_skiplist_first_key - Returns the 1st key
//*********************************************************************************

skiplist_key_t *interval_skiplist_first_key(interval_skiplist_t *isl)
{
    return(skiplist_first_key(isl->sl));
}

//*********************************************************************************
// interval_skiplist_last_key - Returns the last key
//*********************************************************************************

skiplist_key_t *interval_skiplist_last_key(interval_skiplist_t *isl)
{
    return(skiplist_last_key(isl->sl));
}

//*********************************************************************************
//  create_interval_skiplist_full - Creates a new interval skip list
//*********************************************************************************

interval_skiplist_t *create_interval_skiplist_full(int maxlevels, double p,
        skiplist_compare_t *compare,
        skiplist_key_t *(*dup)(skiplist_key_t *a),
        void (*key_free)(skiplist_key_t *a),
        void (*data_free)(skiplist_data_t *a))
{
    interval_skiplist_t *isl = (interval_skiplist_t *)malloc(sizeof(interval_skiplist_t));
    assert(isl != NULL);

    isl->n_intervals = 0;
    isl->data_free = (data_free == NULL) ? sl_no_data_free : data_free;
    isl->sl = create_skiplist_full(maxlevels, p, 1, compare, dup, key_free, sl_no_data_free);

    return(isl);
}

//*********************************************************************************
// create_interval_skiplist - Shortcut to create a new interval skiplist using default values
//*********************************************************************************

interval_skiplist_t *create_interval_skiplist(skiplist_compare_t *compare,
        skiplist_key_t *(*dup)(skiplist_key_t *a),
        void (*key_free)(skiplist_key_t *a),
        void (*data_free)(skiplist_data_t *a))
{
    return(create_interval_skiplist_full(16, 0.25, compare, dup, key_free, data_free));
}

//*********************************************************************************
// destroy_interval_skiplist - Destroys a skiplist
//*********************************************************************************

void destroy_interval_skiplist(interval_skiplist_t *isl)
{
    skiplist_node_t *sn;
    isl_node_t *isln;
    skiplist_t *sl = isl->sl;


    sn = sl->head->next[0];
    while (sn != NULL) {
        isln = sn->ele.data;
        remove_isl_node(sn->level, isln);
        sn = sn->next[0];
    }

    destroy_skiplist(sl);
    free(isl);

    return;
}


//*********************************************************************************
// insert_interval_skiplist - Inserts the interval into the skiplist
//*********************************************************************************

int insert_interval_skiplist(interval_skiplist_t *isl, skiplist_key_t *lo, skiplist_key_t *hi, skiplist_data_t *data)
{
    isl_node_t *isl_node;
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    skiplist_node_t *sn, *sn2, *sn_hi;
    int cmp, i, j;

    skiplist_t *sl = isl->sl;

    //** Inc the number of intervals
    isl->n_intervals++;

    //** Add the hi point first since we walk the edges from lo->hi;
    memset(ptr, 0, sizeof(ptr));
    cmp = find_key(sl, ptr, hi, 0);

    sn = NULL;
    if (cmp != 0) {  //** End point doesn't exist so need to create it.
        isl_node = create_isl_node();
        sn = pos_insert_skiplist(sl, ptr, isl->sl->dup(hi), isl_node);
        add_isl_node_level(isl_node, sn->level);
        for (i=0; i<=sn->level; i++) {
            if ((ptr[i] != NULL) && (ptr[i] != isl->sl->head)) copy_isl_data(((isl_node_t *)(ptr[i]->ele.data))->edge[i], &(isl_node->edge[i]));
        }
        sn_hi = sn;
    } else {
        sn_hi = ptr[0]->next[0];
    }


    //** Now start at the beginning and walk to the end
    memset(ptr, 0, sizeof(ptr));
    cmp = find_key(sl, ptr, lo, 0);
    sn = ptr[0]->next[0];

    if (cmp != 0) {  //** Starting point doesn't exist so need to create it.
        isl_node = create_isl_node();
        sn = pos_insert_skiplist(sl, ptr, isl->sl->dup(lo), isl_node);
        add_isl_node_level(isl_node, sn->level);
        for (i=0; i<=sn->level; i++) {
            if ((ptr[i] != NULL) && (ptr[i] != isl->sl->head)) copy_isl_data(((isl_node_t *)(ptr[i]->ele.data))->edge[i], &(isl_node->edge[i]));
        }

        memset(ptr, 0, sizeof(ptr));
        find_key(sl, ptr, lo, 0);
    }

    isl_node = (isl_node_t *)(sn->ele.data);
    if (sl->compare->fn(sl->compare->arg, lo,hi) == 0) { //** If a point just add it.  No need for a walk
        append_isl_data(&(isl_node->point), data);
        return(0);
    }

    //** start/end lists don't include points
    append_isl_data(&(isl_node->start), data);
    ((isl_node_t *)(sn_hi->ele.data))->n_end++;

//log_printf(15, "insert_interval_skiplist: starting walk to hi\n");

    //** Now do the walk
    while ((sn != sn_hi) && (sn != NULL)) {

        //** Find the highest edge and tag it
        i = sn->level+1;
        j = -1;
        while ((i>= 0) && (j == -1)) {
            i--;
            sn2 = sn->next[i];
            if (sn2 != NULL) {
                cmp = isl->sl->compare->fn(isl->sl->compare->arg, sn2->key, hi);
                if (cmp < 1) {
                    j = i;
                    append_isl_data(&(((isl_node_t *)(sn->ele.data))->edge[j]), data);
                }
            }
        }

        sn = sn->next[i];  //** Jump to the next element
    }

    return(0);
}


//*********************************************************************************
// remove_interval_skiplist - Removes the interval from the skiplist
//     If the element(data) can't be located 1 is returned. Success returns 0.
//*********************************************************************************

int remove_interval_skiplist(interval_skiplist_t *isl, skiplist_key_t *lo, skiplist_key_t *hi, skiplist_data_t *data)
{
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    skiplist_node_t *sn, *sn2, *sn_lo;
    isl_node_t *isln;
    int cmp, i, j, err;

//log_printf(15, "remove_interval_skiplist: START\n");
    memset(ptr, 0, sizeof(ptr));
    cmp = find_key(isl->sl, ptr, lo, 0);
    sn_lo = ptr[0]->next[0];

//i=isl->sl->compare->fn(isl->sl->compare->arg, sn_lo->key, lo);
//log_printf(15, "remove_interval_skiplist: find_key=%d cmp(sn,lo)=%d\n", cmp, i);

    if (cmp != 0) {
        return(1);    //** No match so return
    }

    //** Dec the number of intervals
    isl->n_intervals--;

    sn = sn_lo;
    isln = (isl_node_t *)(sn->ele.data);
    if (isl->sl->compare->fn(isl->sl->compare->arg, lo,hi) == 0) { //** This is a point
        err = remove_isl_data(isl, &(isln->point), data);
        if (isl_node_is_empty(isln, sn->level) == 1) {
            remove_isl_node(sn->level, isln);
            remove_skiplist(isl->sl, lo, NULL);
        }
        isl->data_free(data);
        return(err);
    }

    //** Points aren't listed on the start/end lists
    err = remove_isl_data(isl, &(isln->start), data);

//log_printf(15, "remove_interval_skiplist: starting walk to hi err=%d\n", err);

    //** Now walk to the end point removing the edge data links as we go
    while (isl->sl->compare->fn(isl->sl->compare->arg, sn->key, hi) < 0) {
        //** Find the highest edge and untag it
        i = sn->level+1;
        j = -1;
        while ((i>= 0) && (j == -1)) {
            i--;
            sn2 = sn->next[i];
            if (sn2 != NULL) {
                cmp = isl->sl->compare->fn(isl->sl->compare->arg, sn2->key, hi);
//log_printf(15, "remove_interval_skiplist: level=%d cmp=%d\n", i, cmp);  flush_log();
                if (cmp < 1) {
                    j = i;
                    remove_isl_data(isl, &(((isl_node_t *)(sn->ele.data))->edge[j]), data);
                }
            }
        }

        sn = sn->next[i];  //** Jump to the next element
    }

//log_printf(15, "remove_interval_skiplist: AFTER LOOP\n");  flush_log();

    //** Now check if we remove the end points
    isln = (isl_node_t *)(sn->ele.data);
    isln->n_end--;

//log_printf(15, "remove_interval_skiplist: aaaaaaa err=%d\n", err);  flush_log();

    if (isl_node_is_empty(isln, sn->level) == 1) {
        remove_isl_node(sn->level, isln);
        remove_skiplist(isl->sl, hi, NULL);
    }

//log_printf(15, "remove_interval_skiplist: bbbbbbbbb\n");  flush_log();

    if (sn != sn_lo) {     //** Remove the beginning if needed
       isln = (isl_node_t *)(sn_lo->ele.data);
       if (isl_node_is_empty(isln, sn_lo->level) == 1) {
           remove_isl_node(sn_lo->level, isln);
           remove_skiplist(isl->sl, lo, NULL);
       }
    }
//log_printf(15, "remove_interval_skiplist: before data free\n");  flush_log();

    //** Lastly free the data
    isl->data_free(data);

//log_printf(15, "remove_interval_skiplist: after data free\n");  flush_log();

    return(err);
}

//*********************************************************************************
//  iter_search_interval_skiplist - Returns an iterator for retreiving the list
//      of overlapping intervals.  If lo == NULL then the iterator starts with the
//      1st interval.  Likewise if hi == NULL the iterator runs to the end.
//*********************************************************************************

interval_skiplist_iter_t iter_search_interval_skiplist(interval_skiplist_t *isl, skiplist_key_t *lo, skiplist_key_t *hi)
{
    interval_skiplist_iter_t it;

    it.isl = isl;
    it.ele = NULL;
    it.lo = lo;
    it.hi = hi;
    it.mode = 0;
    it.finished = -1;
    it.ptr_level = isl->sl->current_max;
    memset(it.ptr, 0, sizeof(it.ptr));
    find_key(isl->sl, it.ptr, lo, 1);
    log_printf(15, "iter_search_interval_skiplist: it.sn=%p\n", it.ptr[0]->next[0]);

    if (it.ptr[0] != NULL) {
        it.sn = it.ptr[0];
//     it.isln = (isl_node_t *)(it.sn->ele.data);
//     it.ele = it.isln->start;
        it.sn = it.ptr[0]->next[0];
        it.ele = NULL;
    } else {
        it.finished = 1;
    }

    return(it);
}

//*********************************************************************************
// next_interval_skiplist - Returns the next overlapping interval
//
//  Here's the scanning sequence:
//     1. Scan lo node "start" list.  This has all the "new" intervals but excludes points.
//--     2. If lo node == lo key then also scan lo node points
//     2. Process all ptr[i]->edge[i] lists only.  This gets all the intervals that
//        start before the lo point.
//--     4. For each node from lo ... hi-1 add all edge[i] and point lists.
//     3. For each node from lo ... hi-1 add all start and  point lists.
//     4. Scan the hi nodes point and start list
//*********************************************************************************

skiplist_data_t *next_interval_skiplist(interval_skiplist_iter_t *it)
{
    int i, cmp;
    isl_node_t *isln;
    skiplist_data_t *data;
    skiplist_node_t *sn;


    if (it->finished == 1) return(NULL);

//log_printf(15, "next_interval_skiplist: START ptr_level=%d mode=%d\n", it->ptr_level, it->mode); flush_log();

    if (it->ele != NULL) {  //** Got a match on the current chain
        data = it->ele->data;
        it->ele = it->ele->next;
        return(data);
    }

    //** Have to switch to a different chain
    if (it->ptr_level > -1) {  //** Need to scan down the ptr list
        for (i=it->ptr_level; i>-1; i--) {
            if (it->ptr[i] != NULL) {
//log_printf(15, "next_interval_skiplist: ptr_level=%d i=%d\n", it->ptr_level, i); flush_log();
                isln = (isl_node_t *)(it->ptr[i]->ele.data);
                if (isln != NULL) {
                    if (isln->edge[i] != NULL) {
                        it->ptr_level = i-1;
                        it->ele = isln->edge[i];
                        data = it->ele->data;
                        it->ele = it->ele->next;
                        return(data);
                    }
                }
            }
        }

        it->ptr_level = -1;  //** Need to switch to scanning the start
//     it->sn = it->sn->next[0];
        it->mode = 0;
    }

    cmp = -100;
    if (it->mode >= 0) {
        for (sn=it->sn; sn != NULL; sn = sn->next[0]) {
            cmp = (it->hi == NULL) ? -1 : it->isl->sl->compare->fn(it->isl->sl->compare->arg, sn->key, it->hi);
            if (cmp > -1) {
                it->mode = -2;    //** At the end of the interval
                break;
            }

            isln = (isl_node_t *)(sn->ele.data);
            for (i=it->mode; i<2;  i++) {
                if (i == 0) {  //** Check if we handle the point list next
                    if (isln->point != NULL) {
//log_printf(15, "next_interval_skiplist: POINT ptr_level=%d mode=%d i=%d\n", it->ptr_level, it->mode, i); flush_log();
                        it->ele = isln->point;
                        data = it->ele->data;
                        it->ele = it->ele->next;
                        it->mode = i+1;
                        it->sn = sn;
                        return(data);
                    }
                } else if (i == 1) {   //** or the start list
                    if (isln->start != NULL) {
//log_printf(15, "next_interval_skiplist: START ptr_level=%d mode=%d i=%d\n", it->ptr_level, it->mode, i); flush_log();
                        it->ele = isln->start;
                        data = it->ele->data;
                        it->ele = it->ele->next;
                        it->mode = i+1;
                        it->sn = sn;
                        return(data);
                    }
                }
            }
            it->mode = 0;
        }
    }

    //** If we made it here then we are either at the hi point or finished
    if (cmp == 0) {
        isln = (isl_node_t *)sn->ele.data;
        if (it->mode == -2) { //** Scan the point
            it->mode = -3;
            if (isln->point != NULL) {
                it->ele = isln->point;
                data = it->ele->data;
                it->ele = it->ele->next;
                return(data);
            }
        }

        if (it->mode == -3) {
            it->mode = -4;
            if (isln->start != NULL) {  //** Scan the start list
                it->ele = isln->start;
                data = it->ele->data;
                it->ele = it->ele->next;
                return(data);
            }
        }

    }

    it->finished = 1;
    return(NULL);
}


//*********************************************************************************
// count_interval_skiplist - Returns the number of intervals overlapping the range
//*********************************************************************************

int count_interval_skiplist(interval_skiplist_t *isl, skiplist_key_t *lo, skiplist_key_t *hi)
{
    interval_skiplist_iter_t it;
    int count = 0;

    it = iter_search_interval_skiplist(isl, lo, hi);
    while (next_interval_skiplist(&it) != NULL) {
        count++;
    }

    return(count);
}

