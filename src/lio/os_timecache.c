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

//***********************************************************************
// Remote OS implementation for the client side
//***********************************************************************

//#define _log_module_index 213

#include "apr_wrapper.h"
#include "ex3_system.h"
#include "object_service_abstract.h"
#include "type_malloc.h"
#include "log.h"
#include "thread_pool.h"
#include "os_file.h"
#include "os_timecache.h"
#include "os_remote.h"

#define OSTC_LOCK(ostc); log_printf(5, "LOCK\n"); apr_thread_mutex_lock(ostc->lock)
#define OSTC_UNLOCK(ostc) log_printf(5, "UNLOCK\n"); apr_thread_mutex_unlock(ostc->lock)

#define OSTC_ITER_ALIST  0
#define OSTC_ITER_AREGEX 1

#define OSTC_MAX_RECURSE 500

#define OS_ATTR_LINK "os.attr_link"
#define OS_ATTR_LINK_LEN 12
#define OS_LINK "os.link"

typedef struct {
    char *key;
    void *val;
    char *link;
    int v_size;
    int type;
    apr_time_t expire;
} ostcdb_attr_t;

typedef struct {
    char *fname;
    int ftype;
    char *link;
    apr_pool_t *mpool;
    apr_hash_t *objects;
    apr_hash_t *attrs;
    apr_time_t expire;
} ostcdb_object_t;

typedef struct {
    char *fname;
    int mode;
    int max_wait;
    creds_t *creds;
    char *id;
    os_fd_t *fd_child;
} ostc_fd_t;

typedef struct {
    int n_keys;
    int n_keys_total;
    int ftype_index;
    char **key;
    void **val;
    int *v_size;
} ostc_cacheprep_t;


typedef struct {
    object_service_fn_t *os;
    os_attr_iter_t *it;
} ostc_attr_iter_t;

typedef struct {
    object_service_fn_t *os;
    op_generic_t *gop;
} ostc_remove_regex_t;

typedef struct {
    object_service_fn_t *os;
    os_object_iter_t **it_child;
    ostc_attr_iter_t it_attr;
    os_regex_table_t *attr;
    ostc_fd_t fd;
    creds_t *creds;
    void **val;
    int *v_size;
    int *v_size_initial;
    int n_keys;
    int v_max;
    ostc_cacheprep_t cp;
    int iter_type;
} ostc_object_iter_t;

typedef struct {
    object_service_fn_t *os;
    creds_t *creds;
    ostc_fd_t *fd;
    ostc_fd_t *fd_dest;
    char **src_path;
    char **key;
    char **key_dest;
    void **val;
    char *key_tmp;
    char *src_tmp;
    void *val_tmp;
    int *v_size;
    int v_tmp;
    int n;
} ostc_mult_attr_t;

typedef struct {
    object_service_fn_t *os;
    creds_t *creds;
    char *src_path;
    char *dest_path;
} ostc_move_op_t;

typedef struct {
    object_service_fn_t *os_child;//** child OS which does the heavy lifting
    apr_thread_mutex_t *lock;
    apr_thread_mutex_t *delayed_lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
    thread_pool_context_t *tpc;
    ostcdb_object_t *cache_root;
    apr_time_t entry_timeout;
    apr_time_t cleanup_interval;
    apr_thread_t *cleanup_thread;
    int shutdown;
} ostc_priv_t;

typedef struct {
    object_service_fn_t *os;
    os_fd_t **fd;
    os_fd_t *close_fd;
    creds_t *creds;
    op_generic_t *gop;
    os_fd_t *cfd;
    char *path;
    char *id;
    int mode;
    int max_wait;
} ostc_open_op_t;

op_status_t ostc_close_object_fn(void *arg, int tid);
op_status_t ostc_delayed_open_object(object_service_fn_t *os, ostc_fd_t *fd);

//***********************************************************************
// free_ostcdb_attr - Destroys a cached attribute
//***********************************************************************

void free_ostcdb_attr(ostcdb_attr_t *attr)
{
    log_printf(5, "removing\n");
    if (attr->key) free(attr->key);
    if (attr->val) free(attr->val);
    if (attr->link) free(attr->link);
    free(attr);
}

//***********************************************************************
// _new_ostcdb_attr - Creates a new attr object
//***********************************************************************

ostcdb_attr_t *new_ostcdb_attr(char *key, void *val, int v_size, apr_time_t expire)
{
    ostcdb_attr_t *attr;

    log_printf(5, "adding key=%s size=%d\n", key, v_size);
    type_malloc_clear(attr, ostcdb_attr_t, 1);

    attr->key = strdup(key);
    if (v_size > 0) {
        type_malloc(attr->val, void, v_size+1);
        memcpy(attr->val, val, v_size);
        ((char *)(attr->val))[v_size] = 0;  //** NULL terminate

    }
    attr->v_size = v_size;
    attr->expire = expire;

    return(attr);
}

//***********************************************************************
// free_ostcdb_object - Destroys a cache object
//***********************************************************************

void free_ostcdb_object(ostcdb_object_t *obj)
{
    ostcdb_object_t *o;
    ostcdb_attr_t *a;
    apr_hash_index_t *ohi;
    apr_hash_index_t *ahi;

    //** Free my attributes
    for (ahi = apr_hash_first(NULL, obj->attrs); ahi != NULL; ahi = apr_hash_next(ahi)) {
        a = apr_hash_this_val(ahi);
        free_ostcdb_attr(a);
    }

    //** Now free all the children objects
    if (obj->objects != NULL) {
        for (ohi = apr_hash_first(NULL, obj->objects); ohi != NULL; ohi = apr_hash_next(ohi)) {
            o = apr_hash_this_val(ohi);
            free_ostcdb_object(o);
        }
    }

    if (obj->fname != NULL) free(obj->fname);
    if (obj->link != NULL) free(obj->link);
    apr_pool_destroy(obj->mpool);
    free(obj);
}

//***********************************************************************
// new_ostcdb_object - Creates a new cache object
//***********************************************************************

ostcdb_object_t *new_ostcdb_object(char *entry, int ftype, apr_time_t expire, apr_pool_t *mpool)
{
    ostcdb_object_t *obj;

    type_malloc(obj, ostcdb_object_t, 1);

    obj->fname = entry;
    obj->expire = expire;
    obj->ftype = ftype;
    obj->link = NULL;
    apr_pool_create(&(obj->mpool), NULL);
    obj->objects = (ftype & OS_OBJECT_DIR) ? apr_hash_make(mpool) : NULL;
    obj->attrs = apr_hash_make(mpool);

    return(obj);
}

//***********************************************************************
// _ostc_cleanup - Clean's out the cache of expired objects/attributes
//     NOTE: ostc->lock must be held by the calling process
//***********************************************************************

int _ostc_cleanup(object_service_fn_t *os, ostcdb_object_t *obj, apr_time_t expired)
{
    ostcdb_object_t *o;
    ostcdb_attr_t *a;
    apr_hash_index_t *hi;
    int okept, akept, result;

    if (obj == NULL) return(0);  //** Nothing to do so return

    //** Recursively prune the objects
    okept = 0;
    if (obj->objects) {
        for (hi = apr_hash_first(NULL, obj->objects); hi != NULL; hi = apr_hash_next(hi)) {
            o = apr_hash_this_val(hi);
            result = _ostc_cleanup(os, o, expired);
            okept += result;
            if (result == 0) {
                apr_hash_set(obj->objects, o->fname, APR_HASH_KEY_STRING, NULL);
                free_ostcdb_object(o);
            }
        }
    }

    //** Free my expired attributes
    akept = 0;
    for (hi = apr_hash_first(NULL, obj->attrs); hi != NULL; hi = apr_hash_next(hi)) {
        a = apr_hash_this_val(hi);
        log_printf(5, "fname=%s attr=%s a->expire=" TT " expired=" TT "\n", obj->fname, a->key, a->expire, expired);
        if (a->expire < expired) {
            apr_hash_set(obj->attrs, a->key, APR_HASH_KEY_STRING, NULL);
            free_ostcdb_attr(a);
        } else {
            akept++;
        }
    }

    log_printf(5, "fname=%s akept=%d okept=%d o+a=%d\n", obj->fname, akept, okept, akept+okept);
    return(akept + okept);
}

//***********************************************************************
// ostc_cache_compact_thread - Thread for cleaning out the cache
//***********************************************************************

void *ostc_cache_compact_thread(apr_thread_t *th, void *data)
{
    object_service_fn_t *os = (object_service_fn_t *)data;
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    OSTC_LOCK(ostc);
    while (ostc->shutdown == 0) {
        apr_thread_cond_timedwait(ostc->cond, ostc->lock, ostc->cleanup_interval);

        log_printf(5, "START: Running an attribute cleanup\n");
        _ostc_cleanup(os, ostc->cache_root, apr_time_now());
        log_printf(5, "END: cleanup finished\n");
    }
    OSTC_UNLOCK(ostc);

    return(NULL);
}

//***********************************************************************
// ostc_attr_cacheprep_setup - Sets up the arrays for storing the attributes
//    in the time cache.
//***********************************************************************

void ostc_attr_cacheprep_setup(ostc_cacheprep_t *cp, int n_keys, char **key_src, void **val_src, int *v_size_src, int get_ftype)
{
    int i, j, n;
    void **vd;
    char **kd;
    int *vsd;

    cp->ftype_index = -1;

    //** Make the full list of attrs to get
    type_malloc(kd, char *, 2*n_keys+2);
    type_malloc_clear(vd, void *, 2*n_keys+2);
    type_malloc_clear(vsd, int, 2*n_keys+2);
    memcpy(kd, key_src, sizeof(char *) * n_keys);
    memcpy(vsd, v_size_src, sizeof(int) * n_keys);
    memcpy(vd, val_src, sizeof(void *) * n_keys);

    for (i=n_keys; i<2*n_keys; i++) {
        n = strlen(key_src[i-n_keys]);
        j = n + OS_ATTR_LINK_LEN + 1 + 1;
        type_malloc(kd[i], char, n + OS_ATTR_LINK_LEN + 1 + 1);
        snprintf(kd[i], j, "%s.%s", OS_ATTR_LINK, kd[i-n_keys]);
        vsd[i] = -OS_PATH_MAX;
        log_printf(5, "key[%d]=%s\n", i, kd[i]);
    }
    kd[2*n_keys] = strdup(OS_LINK);
    vsd[2*n_keys] = -OS_PATH_MAX;

    cp->n_keys = n_keys;
    cp->n_keys_total = 2*n_keys+1;
    cp->key = kd;
    cp->val = vd;
    cp->v_size = vsd;

    if (get_ftype == 1) {
        for (i=0; i<n_keys; i++) {
            if (strcmp(key_src[i], "os.type") == 0) {
                cp->ftype_index = i;
                break;
            }
        }

        if (cp->ftype_index == -1) {
            cp->ftype_index = 2*n_keys+1;
            cp->n_keys_total = 2*n_keys+2;
            cp->key[cp->ftype_index] = strdup("os.type");
            cp->v_size[cp->ftype_index] = -OS_PATH_MAX;
        }
    }
}

//***********************************************************************
// ostc_attr_cacheprep_destroy - Destroy shte structures created in the setup routine
//***********************************************************************

void ostc_attr_cacheprep_destroy(ostc_cacheprep_t *cp)
{
    int i;

    for (i=cp->n_keys; i<cp->n_keys_total; i++) {
        if (cp->key != NULL) {
            if (cp->key[i] != NULL) free(cp->key[i]);
        }
        if (cp->val != NULL) {
            if (cp->val[i] != NULL) free(cp->val[i]);
        }
    }

    if (cp->key != NULL) free(cp->key);
    if (cp->val != NULL) free(cp->val);
    if (cp->v_size != NULL) free(cp->v_size);
}

//***********************************************************************
//  ostc_attr_cacheprep_copy - Copies the user attributes back out
//***********************************************************************

void ostc_attr_cacheprep_copy(ostc_cacheprep_t *cp, void **val, int *v_size)
{
    //** Copy the data
    memcpy(val, cp->val, sizeof(char *) * cp->n_keys);
    memcpy(v_size, cp->v_size, sizeof(int) * cp->n_keys);
}


//***********************************************************************
//  ostc_attr_cacheprep_ftype - Retreives the file type from the
//  attributes
//***********************************************************************

int ostc_attr_cacheprep_ftype(ostc_cacheprep_t *cp)
{
    int ftype;

    if (cp->ftype_index == -1) return(-1);

    sscanf((char *)cp->val[cp->ftype_index], "%d", &ftype);
    return(ftype);
}

//***********************************************************************
// _ostc_cache_tree_walk - Walks the cache tree using the provided path
//   and optionally creates the target node if needed.
//
//   Returns 0 on success or a positive value representing the prefix that could
//      be mapped.
//
//   NOTE:  Assumes the cache lock is held
//***********************************************************************

int _ostc_cache_tree_walk(object_service_fn_t *os, char *fname, Stack_t *tree, ostcdb_object_t *replacement_obj, int add_terminal_ftype, int max_recurse)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    int i, n, start, end, loop, err;
    Stack_t rtree;
    ostcdb_object_t *curr, *next, *prev;

    log_printf(5, "fname=%s add_terminal_ftype=%d\n", fname, add_terminal_ftype);

    err = -1;
    if ((fname == NULL) || (max_recurse <= 0)) return(1);

    i=0;
    if ((stack_size(tree) == 0) || (fname[i] == '/')) {
        curr = ostc->cache_root;
        empty_stack(tree, 0);
    } else {
        curr = get_ele_data(tree);
    }
    move_to_bottom(tree);
    insert_below(tree, curr);

    loop = 0;
    while (fname[i] != 0) {
        //** Pick off any multiple /'s
        for (start=i; fname[start] == '/' ; start++) {}
        err = start;
        log_printf(5, "loop=%d start=%d i=%d fname[start]=%hhu\n", loop, start, i, fname[start]);
        if (fname[start] == 0) {
            if (loop == 0) {  //** This is the first (and last) token so push the root on the stack
                move_to_bottom(tree);
                insert_below(tree, curr);
            }
            err = 0;
            goto finished;
        }
        //** Find the trailing / or we reach the end
        for (end=start+1; fname[end] != '/' ; end++) {
            if (fname[end] == 0) break;
        }
        end--;
        i = end+1;

        //** Now do the lookup
        n = end - start +1;
        if (curr != NULL) {
            next = (curr->objects != NULL) ? apr_hash_get(curr->objects, (void *)&(fname[start]), n) : NULL;
        } else {
            next = NULL;
        }
        log_printf(5, "loop=%d start=%d end=%d i=%d next=%p end_char=%hhu prefix=%s\n", loop, start, end, i, next, fname[end], fname+start);

        if (next == NULL) {  //** Check if at the end
            if (fname[i] == 0) { //** Yup at the end
                if (add_terminal_ftype > 0)  { //** Want to add the terminal
                    if (replacement_obj == NULL) {
                        next = new_ostcdb_object(strndup(&(fname[start]), n), add_terminal_ftype, apr_time_now() + ostc->entry_timeout, ostc->mpool);
                    } else {
                        next = replacement_obj;
                    }
                    apr_hash_set(curr->objects, (void *)next->fname, n, next);
                    move_to_bottom(tree);
                    insert_below(tree, next);

                    err = 0;
                    goto finished;
                }
            } else if (fname[start] == '.') {  //** Check if it's a special entry
                if (n == 1) {//**Got a "."
                    continue;    //** Just restart the loop
                } else if (n == 2) {
                    if (fname[start+1] == '.') { //** Got a ".."
                        move_to_bottom(tree);
                        delete_current(tree, 1, 0);
                        curr = get_ele_data(tree);
                        continue;
                    }
                }
            }

            goto finished;
        } else if (next->link) {  //** Got a link
            if (fname[i] != 0) { //** If not at the end we need to follow it
                //*** Need to make a new stack and recurse it only keeping the bottom element
                init_stack(&rtree);
                dup_stack(tree, &rtree);
                if (_ostc_cache_tree_walk(os, next->link, &rtree, NULL, add_terminal_ftype, max_recurse-1) != 0) {
                    empty_stack(&rtree, 0);
                    err = -1;
                    goto finished;
                }
                move_to_bottom(&rtree);
                next = get_ele_data(&rtree);  //** This will get placed as the next object on the stack
                empty_stack(&rtree, 0);
            }
        }

        loop++;
        move_to_bottom(tree);
        insert_below(tree, next);
        curr = next;
    }

    //** IF we made it here and this is non-NULL we need to replace the object we just located
    //** With the one provided
    if (replacement_obj != NULL) {
        move_up(tree);
        prev = get_ele_data(tree);
        apr_hash_set(prev->objects, curr->fname, APR_HASH_KEY_STRING, NULL);
        apr_hash_set(prev->objects, replacement_obj->fname, strlen(replacement_obj->fname), replacement_obj);

        free_ostcdb_object(curr);

        move_to_bottom(tree);
        delete_current(tree, 1, 0);
        insert_below(tree, replacement_obj);
    }

    err = 0;
finished:
    log_printf(15, "fname=%s err=%d\n", fname, err);
    ostcdb_object_t *lo;
    move_to_top(tree);
    log_printf(15, "stack_size=%d\n", stack_size(tree));
    while ((lo = get_ele_data(tree)) != NULL) {
        log_printf(15, "pwalk lo=%s ftype=%d\n", lo->fname, lo->ftype);
        move_down(tree);
    }

    return(err);
}

//***********************************************************************
// ostcdb_resolve_attr_link - Follows the attribute symlinks and returns
//   final object and attribute
//***********************************************************************

int _ostcdb_resolve_attr_link(object_service_fn_t *os, Stack_t *tree, char *alink, ostcdb_object_t **lobj, ostcdb_attr_t **lattr, int max_recurse)
{
    Stack_t rtree;
    int i, n;
    char *aname;
    ostcdb_object_t *lo;
    ostcdb_attr_t *la;

    log_printf(5, "START alink=%s\n", alink);

    lo = NULL;
    la = NULL;

    //** 1st split the link into a path and attribute name
    n = strlen(alink);
    aname = NULL;
    for (i=n-1; i>=0; i--) {
        if (alink[i] == '/') {
            aname = alink + i + 1;
            alink[i] = 0;
            break;
        }
    }
    log_printf(5, "alink=%s aname=%s i=%d mr=%d\n", alink, aname, i, max_recurse);

    //** Copy the stack
    init_stack(&rtree);
    dup_stack(&rtree, tree);

    move_to_top(&rtree);
    log_printf(5, "stack_size=%d org=%d\n", stack_size(&rtree), stack_size(tree));
    while ((lo = get_ele_data(&rtree)) != NULL) {
        log_printf(5, "pwalk lo=%s ftype=%d\n", lo->fname, lo->ftype);
        move_down(&rtree);
    }
    lo = NULL;

    //** and pop the terminal which is up.  This will pop us up to the directory for the walk
    move_to_bottom(&rtree);
    delete_current(&rtree, 1, 0);
    if (_ostc_cache_tree_walk(os, alink, &rtree, NULL, 0, OSTC_MAX_RECURSE) != 0) {
        if (i> -1) alink[i] = '/';
        goto finished;
    }
    if (i> -1) alink[i] = '/'; //** Undo our string munge if needed
    move_to_bottom(&rtree);
    lo = get_ele_data(&rtree);  //** This will get placed as the next object on the stack

    if (lo == NULL) goto finished;
    la = apr_hash_get(lo->attrs, aname, APR_HASH_KEY_STRING);
    log_printf(5, "alink=%s aname=%s lattr=%p mr=%d\n", alink, aname, la, max_recurse);

    if (la != NULL) {
        log_printf(5, "alink=%s aname=%s lo=%s la->link=%s\n", alink, aname, lo->fname, la->link, max_recurse);
        if (la->link) {  //** Got to recurse
            aname = strdup(la->link);
            _ostcdb_resolve_attr_link(os, &rtree, aname, &lo, &la, max_recurse-1);
            free(aname);
        }
    }

finished:
    empty_stack(&rtree, 0);
    *lattr = la;
    *lobj = lo;

    return(((la != NULL) && (lo != NULL)) ? 0 : 1);
}


//***********************************************************************
//  ostc_cache_move_object - Moves an existing cache object within the cache
//***********************************************************************

void ostc_cache_move_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t stree, dtree;
    ostcdb_object_t *obj;

    init_stack(&stree);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, src_path, &stree, NULL, 0, OSTC_MAX_RECURSE) == 0) {
        move_to_bottom(&stree);
        obj = get_ele_data(&stree);
        init_stack(&dtree);
        _ostc_cache_tree_walk(os, src_path, &dtree, obj, obj->ftype, OSTC_MAX_RECURSE);  //** Do the walk and substitute
        empty_stack(&dtree, 0);
    }
    OSTC_UNLOCK(ostc);

    empty_stack(&stree, 0);
}

//***********************************************************************
//  ostc_cache_remove_object - Removes a cache object
//***********************************************************************

void ostc_cache_remove_object(object_service_fn_t *os, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj, *parent;

    init_stack(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, path, &tree, NULL, 0, OSTC_MAX_RECURSE) == 0) {
        move_to_bottom(&tree);
        obj = get_ele_data(&tree);
        move_up(&tree);
        parent = get_ele_data(&tree);
        apr_hash_set(parent->objects, obj->fname, APR_HASH_KEY_STRING, NULL);
        free_ostcdb_object(obj);
    }
    OSTC_UNLOCK(ostc);

    empty_stack(&tree, 0);
}

//***********************************************************************
//  ostc_remove_attr - Removes the given attributes from the object
//***********************************************************************

void ostc_cache_remove_attrs(object_service_fn_t *os, char *fname, char **key, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr;

    int i;

    init_stack(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    move_to_bottom(&tree);
    obj = get_ele_data(&tree);
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key[i], APR_HASH_KEY_STRING);
        if (attr != NULL) {
            apr_hash_set(obj->attrs, key[i], APR_HASH_KEY_STRING, NULL);
            free_ostcdb_attr(attr);
        }
    }
finished:
    OSTC_UNLOCK(ostc);

    empty_stack(&tree, 0);
}


//***********************************************************************
//  ostc_move_attr - Renames the given attributes from the object
//***********************************************************************

void ostc_cache_move_attrs(object_service_fn_t *os, char *fname, char **key_old, char **key_new, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr, *attr2;
    int i;

    init_stack(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    move_to_bottom(&tree);
    obj = get_ele_data(&tree);
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key_old[i], APR_HASH_KEY_STRING);
        if (attr != NULL) {
            apr_hash_set(obj->attrs, key_old[i], APR_HASH_KEY_STRING, NULL);
            attr2 = apr_hash_get(obj->attrs, key_new[i], APR_HASH_KEY_STRING);
            if (attr2 != NULL) {  //** Already have something by that name so delete it
                apr_hash_set(obj->attrs, key_new[i], APR_HASH_KEY_STRING, NULL);
                free_ostcdb_attr(attr2);
            }

            if (attr->key) free(attr->key);
            attr->key = strdup(key_new[i]);
            apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    empty_stack(&tree, 0);
}


//***********************************************************************
//  ostc_cache_process_attrs - Merges the attrs into the cache
//***********************************************************************

void ostc_cache_process_attrs(object_service_fn_t *os, char *fname, int ftype, char **key_list, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj, *aobj;
    ostcdb_attr_t *attr;
    char *key, *lkey;
    int i;

    init_stack(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, fname, &tree, NULL, ftype, OSTC_MAX_RECURSE) != 0) goto finished;

    log_printf(5, "fname=%s stack_size=%d\n", fname, stack_size(&tree));

    move_to_bottom(&tree);
    obj = get_ele_data(&tree);

    if (obj->link) {
        free(obj->link);
        obj->link = NULL;
    }
    if (val[2*n]) { //** got a symlink
        obj->link = (char *)val[2*n];
        val[2*n] = NULL;
    }
    for (i=0; i<n; i++) {
        key = key_list[i];
        if (strcmp(key, "os.lock") == 0) continue;  //** These we don't cache

        lkey = val[n+i];
        if (lkey != NULL) {  //** Got to find the linked attribute
            _ostcdb_resolve_attr_link(os, &tree, lkey, &aobj, &attr, OSTC_MAX_RECURSE);

            if (attr == NULL) continue;  //** If no atribute then skip updating
            log_printf(5, "TARGET obj=%s key=%s\n", aobj->fname, attr->key);

            //** Make the attr on the target link
            if (attr->val) free(attr->val);
            type_malloc(attr->val, void, v_size[i]+1);
            memcpy(attr->val, val[i], v_size[i]);
            ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
            attr->v_size = v_size[i];

            //** Now make the pointer on the source
            attr = apr_hash_get(obj->attrs, key, APR_HASH_KEY_STRING);
            if (attr == NULL) {
                log_printf(5, "NEW obj=%s key=%s link=%s\n", obj->fname, key, lkey);
                attr = new_ostcdb_attr(key, NULL, -1234, apr_time_now() + ostc->entry_timeout);
                apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
            } else {
                log_printf(5, "OLD obj=%s key=%s link=%s\n", obj->fname, key, lkey);
                if (attr->link) free(attr->link);
                if (attr->val) free(attr->val);
                attr->v_size = -1234;
                attr->val = NULL;
            }
            attr->link = lkey;
            val[n+i] = NULL;
            v_size[n+i] = 0;
        } else {
            attr = apr_hash_get(obj->attrs, key, APR_HASH_KEY_STRING);
            if (attr == NULL) {
                attr = new_ostcdb_attr(key, val[i], v_size[i], apr_time_now() + ostc->entry_timeout);
                apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
            } else {
                if (attr->link) {
                    free(attr->link);
                    attr->link = NULL;
                }
                if (attr->val) free(attr->val);
                if (v_size[i] > 0) {
                    type_malloc(attr->val, void, v_size[i]+1);
                    memcpy(attr->val, val[i], v_size[i]);
                    ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
                } else {
                    attr->val = NULL;
                }
                attr->v_size = v_size[i];
            }
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    empty_stack(&tree, 0);
}


//***********************************************************************
// ostc_cache_fetch - Attempts to process the attribute request from cached data
//***********************************************************************

op_status_t ostc_cache_fetch(object_service_fn_t *os, char *fname, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj, *lobj;
    ostcdb_attr_t *attr;
    op_status_t status = op_failure_status;
    void *va[n];
    int vs[n];
    int i, oops;

    init_stack(&tree);
    oops = 0;

//log_printf(5, "fname=%s\n", fname);
    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    move_to_bottom(&tree);
    obj = get_ele_data(&tree);
    oops = 1;
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key[i], APR_HASH_KEY_STRING);
        if (attr == NULL) goto finished;  //** Not in cache so need to pull it

        log_printf(5, "BEFORE obj=%s key=%s val=%s v_size=%d alink=%s olink=%s\n", obj->fname, attr->key, (char *)attr->val, attr->v_size, attr->link, obj->link);

        if (attr->link != NULL) {  //** Got to resolve the link
            _ostcdb_resolve_attr_link(os, &tree, attr->link, &lobj, &attr, OSTC_MAX_RECURSE);
            if (attr == NULL) goto finished;  //** Can't follow the link
        }
        log_printf(5, "AFTER obj=%s key=%s val=%s v_size=%d alink=%s olink=%s\n", obj->fname, attr->key, (char *)attr->val, attr->v_size, attr->link, obj->link);

        //** Store the original values in case we need to rollback
        vs[i] = v_size[i];
        va[i] = val[i];

        log_printf(5, "key=%s val=%s v_size=%d\n", attr->key, (char *)attr->val, attr->v_size);
        osf_store_val(attr->val, attr->v_size, &(val[i]), &(v_size[i]));
    }

    oops = 0;
    status = op_success_status;

finished:
    OSTC_UNLOCK(ostc);

    if (oops == 1) { //** Got to unroll the values stored
        oops = i;
        for (i=0; i<oops; i++) {
            if (vs[i] < 0) {
                if (val[i] != NULL) free(val[i]);
            }
            v_size[i] = vs[i];
            val[i] = va[i];
        }
    }

    log_printf(5, "fname=%s n=%d key[0]=%s status=%d\n", fname, n, key[0], status.op_status);
    empty_stack(&tree, 0);

    return(status);
}


//***********************************************************************
// ostc_cache_update_attrs - Updates the attributes alredy cached.
//     Attributes that aren't cached are ignored.
//***********************************************************************


void ostc_cache_update_attrs(object_service_fn_t *os, char *fname, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr;
    int i;

    init_stack(&tree);

    log_printf(15, "fname=%s n=%d key[0]=%s\n", fname, n, key[0]);

    OSTC_LOCK(ostc);
    if (_ostc_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    move_to_bottom(&tree);
    obj = get_ele_data(&tree);
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key[i], APR_HASH_KEY_STRING);
        if (attr == NULL) continue;  //** Not in cache so ignore updating it

        if (attr->link != NULL) {  //** Got to resolve the link
            _ostcdb_resolve_attr_link(os, &tree, attr->link, &obj, &attr, OSTC_MAX_RECURSE);
            if (attr == NULL) continue;  //** Can't follow the link
        }

        attr->v_size = v_size[i];
        if (attr->val) {
            free(attr->val);
            attr->val = NULL;
        }
        if (val) {
            type_malloc(attr->val, void, attr->v_size+1);
            memcpy(attr->val, val[i], attr->v_size);
            ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    empty_stack(&tree, 0);
}

//***********************************************************************
// _ostc_cache_populate_prefix - Recursively populates the prefix with a
//    minimal set of cache entries.
//***********************************************************************

int _ostc_cache_populate_prefix(object_service_fn_t *os, creds_t *creds, char *path, int prefix_len)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    Stack_t tree;
    ostc_cacheprep_t cp;
    os_fd_t *fd = NULL;
    char *key_array[1], *val_array[1];
    char *fname, *key, *val;
    int v_size[1];
    int err, start, end, len, ftype;
    int max_wait = 10;
    op_status_t status;

    len = strlen(path);
    if (len == 1) return(0);  //** Nothing to do.  Just a '/'

    init_stack(&tree);
    err = _ostc_cache_tree_walk(os, path, &tree, NULL, 0, OSTC_MAX_RECURSE);
    empty_stack(&tree, 0);
    if (err <= 0)  return(err);

    start = err;
    for (end=start; path[end] != '/' ; end++) {  //** Find the terminal end
        if (path[end] == 0) break;
    }

    fname = strndup(path, end);

    if (end <= prefix_len) {
        log_printf(0, "ERROR: Unable to make progress in recursion. end=%d prefix_len=%d fname=%s full_path=%s\n", end, prefix_len, fname, path);
        free(fname);
        return(-1234);
    }

    log_printf(5, "path=%s prefix=%s len=%d end=%d\n", path, fname, len, end);

    key = "system.inode"; val = NULL;
    key_array[0] = key;  val_array[0] = val;
    v_size[0] = -100;
    ostc_attr_cacheprep_setup(&cp, 1, key_array, (void **)val_array, v_size, 1);

    err = gop_sync_exec(os_open_object(ostc->os_child, creds, fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, max_wait));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        return(-1);
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    status = gop_sync_exec_status(os_get_multiple_attrs(ostc->os_child, creds, fd, cp.key, cp.val, cp.v_size, cp.n_keys_total));

    //** Close the parent
    err = gop_sync_exec(os_close_object(ostc->os_child, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    //** Store them in the cache on success
    if (status.op_status == OP_STATE_SUCCESS) {
        ftype = ostc_attr_cacheprep_ftype(&cp);
        log_printf(1, "storing=%s ftype=%d end=%d len=%d v_size[0]=%d\n", fname, ftype, end, len, cp.v_size[0]);
        ostc_cache_process_attrs(os, fname, ftype, cp.key, cp.val, cp.v_size, cp.n_keys);
        ostc_attr_cacheprep_copy(&cp, (void **)val_array, v_size);
        if (end < (len-1)) { //** Recurse and add the next layer
           log_printf(1, "recursing object=%s\n", path);
           err = _ostc_cache_populate_prefix(os, creds, path, end);
        }
    }

    ostc_attr_cacheprep_destroy(&cp);
    free(fname);
    if (v_size[0] > 0) free(val_array[0]);

    return(err);
}


//***********************************************************************
// ostc_remove_regex_object_fn - Simple passthru with purging of my cache.
//      This command is rarely used.  Hence the simple purging.
//***********************************************************************

op_status_t ostc_remove_regex_object_fn(void *arg, int tid)
{
    ostc_remove_regex_t *op = (ostc_remove_regex_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    op_status_t status;

    status = gop_sync_exec_status(op->gop);
    op->gop = NULL;  //** This way we don't accidentally clean it up again

    //** Since we don't know what was removed we're going to purge everything to make life easy.
    if (status.op_status == OP_STATE_SUCCESS) {
        apr_thread_mutex_lock(ostc->lock);
        _ostc_cleanup(op->os, ostc->cache_root, apr_time_now() + 4*ostc->entry_timeout);
        apr_thread_mutex_unlock(ostc->lock);
    }

    return(status);
}

//***********************************************************************
//  free_remove_regex - Frees a regex remove object
//***********************************************************************

void free_remove_regex(void *arg)
{
   ostc_remove_regex_t *op = (ostc_remove_regex_t *)arg;

   if (op->gop) gop_free(op->gop, OP_DESTROY);
   free(op);
}

//***********************************************************************
// ostc_remove_regex_object - Simple passthru with purging of my cache.
//      This command is rarely used.  Hence the simple purging.
//***********************************************************************

op_generic_t *ostc_remove_regex_object(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_remove_regex_t *op;

    type_malloc(op, ostc_remove_regex_t, 1);
    op->os = os;
    op->gop = os_remove_regex_object(ostc->os_child, creds, path, object_regex, obj_types, recurse_depth);
    gop_set_private(op->gop, op);

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_remove_regex_object_fn, (void *)op, free_remove_regex, 1));
}

//***********************************************************************
// ostc_abort_remove_regex_object - Aborts a bulk remove call
//***********************************************************************

op_generic_t *ostc_abort_remove_regex_object(object_service_fn_t *os, op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_remove_regex_t *op;

    op = (ostc_remove_regex_t *)gop_get_private(gop);
    return(os_abort_remove_regex_object(ostc->os_child, op->gop));
}

//***********************************************************************
// ostc_remove_object_fn - Handles the actual object removal
//***********************************************************************

op_status_t ostc_remove_object_fn(void *arg, int tid)
{
    ostc_move_op_t *op = (ostc_move_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    op_status_t status;


    status = gop_sync_exec_status(os_remove_object(ostc->os_child, op->creds, op->src_path));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    ostc_cache_remove_object(op->os, op->src_path);

    return(status);
}


//***********************************************************************
// ostc_remove_object - Generates a remove object operation
//***********************************************************************

op_generic_t *ostc_remove_object(object_service_fn_t *os, creds_t *creds, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_move_op_t *op;

    type_malloc(op, ostc_move_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = path;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_remove_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

op_generic_t *ostc_regex_object_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_regex_object_set_multiple_attrs(ostc->os_child, creds, id, path, object_regex, object_types, recurse_depth, key, val, v_size, n_attrs));
}

//***********************************************************************
// ostc_abort_regex_object_set_multiple_attrs - Aborts a bulk attr call
//***********************************************************************

op_generic_t *ostc_abort_regex_object_set_multiple_attrs(object_service_fn_t *os, op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_abort_regex_object_set_multiple_attrs(ostc->os_child, gop));
}


//***********************************************************************
//  ostc_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

op_generic_t *ostc_exists(object_service_fn_t *os, creds_t *creds, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_exists(ostc->os_child, creds, path));
}

//***********************************************************************
// ostc_create_object - Creates an object
//***********************************************************************

op_generic_t *ostc_create_object(object_service_fn_t *os, creds_t *creds, char *path, int type, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_create_object(ostc->os_child, creds, path, type, id));
}


//***********************************************************************
// ostc_symlink_object - Generates a symbolic link object operation
//***********************************************************************

op_generic_t *ostc_symlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_symlink_object(ostc->os_child, creds, src_path, dest_path, id));
}


//***********************************************************************
// ostc_hardlink_object - Generates a hard link object operation
//***********************************************************************

op_generic_t *ostc_hardlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_hardlink_object(ostc->os_child, creds, src_path, dest_path, id));
}

//***********************************************************************
// ostc_move_object_fn - Handles the actual object move
//***********************************************************************

op_status_t ostc_move_object_fn(void *arg, int tid)
{
    ostc_move_op_t *op = (ostc_move_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    op_status_t status;


    status = gop_sync_exec_status(os_move_object(ostc->os_child, op->creds, op->src_path, op->dest_path));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    ostc_cache_move_object(op->os, op->creds, op->src_path, op->dest_path);

    return(status);
}

//***********************************************************************
// ostc_delayed_open_object - Actual opens the object for defayed opens
//***********************************************************************

op_status_t ostc_delayed_open_object(object_service_fn_t *os, ostc_fd_t *fd)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    op_status_t status;
    os_fd_t *cfd;

log_printf(5, "DELAYED_OPEN fd=%s\n", fd->fname);
    status = gop_sync_exec_status(os_open_object(ostc->os_child, fd->creds, fd->fname, fd->mode, fd->id, &cfd, fd->max_wait));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    apr_thread_mutex_lock(ostc->delayed_lock);
    if (fd->fd_child == NULL) {
        fd->fd_child = cfd;
        apr_thread_mutex_unlock(ostc->delayed_lock);
    } else { //** Somebody else beat us to it so close mine and throw it away
        apr_thread_mutex_unlock(ostc->delayed_lock);

        gop_sync_exec(os_close_object(ostc->os_child, cfd));
    }

    return(status);
}

//***********************************************************************
// ostc_move_object - Generates a move object operation
//***********************************************************************

op_generic_t *ostc_move_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_move_op_t *op;

    type_malloc(op, ostc_move_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = src_path;
    op->dest_path = dest_path;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_move_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_copy_multiple_attrs_fn - Handles the actual attribute copy operation
//***********************************************************************

op_status_t ostc_copy_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    op_status_t status;

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    if (ma->fd_dest->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd_dest);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_copy_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->fd_dest->fd_child, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** We're just going to remove the destination attributes and rely on a refetch to get the changes
    ostc_cache_remove_attrs(ma->os, ma->fd_dest->fname, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

op_generic_t *ostc_copy_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->fd = (ostc_fd_t *)fd_src;
    ma->fd_dest = (ostc_fd_t *)fd_dest;
    ma->key = key_src;
    ma->key_dest = key_dest;
    ma->n = n;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_copy_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_copy_attr - Generates a copy object attribute operation
//***********************************************************************

op_generic_t *ostc_copy_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->fd = fd_src;
    ma->fd_dest = fd_dest;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_copy_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_symlink_attrs_fn - Handles the actual attribute symlinking
//***********************************************************************

op_status_t ostc_symlink_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    op_status_t status;

    if (ma->fd_dest->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd_dest);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_symlink_multiple_attrs(ostc->os_child, ma->creds, ma->src_path, ma->key, ma->fd_dest->fd_child, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** We're just going to remove the destination attributes and rely on a refetch to get the changes
    ostc_cache_remove_attrs(ma->os, ma->fd_dest->fname, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

op_generic_t *ostc_symlink_multiple_attrs(object_service_fn_t *os, creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->src_path = src_path;
    ma->key = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = key_dest;
    ma->n = n;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_symlink_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_symlink_attr - Generates a link attribute operation
//***********************************************************************

op_generic_t *ostc_symlink_attr(object_service_fn_t *os, creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->src_path = &(ma->src_tmp);
    ma->src_tmp = src_path;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_symlink_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_move_attrs_fn - Handles the actual attribute move operation
//***********************************************************************

op_status_t ostc_move_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    op_status_t status;

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_move_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** Move them in the cache
    ostc_cache_move_attrs(ma->os, ma->fd->fname, ma->key, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

op_generic_t *ostc_move_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key_old;
    ma->key_dest = key_new;
    ma->n = n;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_move_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_move_attr - Generates a move object attribute operation
//***********************************************************************

op_generic_t *ostc_move_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_old;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_new;

    ma->n = 1;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_move_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_get_attrs_fn - Handles the actual attribute get operation
//***********************************************************************

op_status_t ostc_get_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    op_status_t status;
    int ftype;
    ostc_cacheprep_t cp;


    //** 1st see if we can satisfy everything from cache
    status = ostc_cache_fetch(ma->os, ma->fd->fname, ma->key, ma->val, ma->v_size, ma->n);

if (status.op_status == OP_STATE_SUCCESS) {
  log_printf(10, "ATTR_CACHE_HIT: fname=%s key[0]=%s n_keys=%d\n", ma->fd->fname, ma->key[0], ma->n);
} else {
  log_printf(10, "ATTR_CACHE_MISS fname=%s key[0]=%s n_keys=%d\n", ma->fd->fname, ma->key[0], ma->n);
}
    if (status.op_status == OP_STATE_SUCCESS) return(status);

    _ostc_cache_populate_prefix(ma->os, ma->creds, ma->fd->fname, 0);

    ostc_attr_cacheprep_setup(&cp, ma->n, ma->key, ma->val, ma->v_size, 1);

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) goto failed;
    }

    status = gop_sync_exec_status(os_get_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, cp.key, cp.val, cp.v_size, cp.n_keys_total));

    //** Store them in the cache on success
    if (status.op_status == OP_STATE_SUCCESS) {
        ftype = ostc_attr_cacheprep_ftype(&cp);
        ostc_cache_process_attrs(ma->os, ma->fd->fname, ftype, cp.key, cp.val, cp.v_size, cp.n_keys);
        ostc_attr_cacheprep_copy(&cp, ma->val, ma->v_size);
    }

failed:
    ostc_attr_cacheprep_destroy(&cp);

    return(status);
}

//***********************************************************************
// ostc_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *ostc_get_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_get_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *ostc_get_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = 1;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_get_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_set_attrs_fn - Handles the actual attribute set operation
//***********************************************************************

op_status_t ostc_set_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    op_status_t status;

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_set_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->val, ma->v_size, ma->n));

    //** Failed just return
    if (status.op_status == OP_STATE_SUCCESS) return(status);

    //** Update the cache on the keys we know about
    ostc_cache_update_attrs(ma->os, ma->fd->fname, ma->key, ma->val, ma->v_size, ma->n);

    return(status);
}

//***********************************************************************
// ostc_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

op_generic_t *ostc_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_set_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

op_generic_t *ostc_set_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = &(ma->val_tmp);
    ma->val_tmp = val;
    ma->v_size = &(ma->v_tmp);
    ma->v_tmp = v_size;
    ma->n = 1;

    return(new_thread_pool_op(ostc->tpc, NULL, ostc_set_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************

//***********************************************************************
// ostc_next_attr - Returns the next matching attribute
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

int ostc_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
    ostc_attr_iter_t *it = (ostc_attr_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    return(os_next_attr(ostc->os_child, it->it, key, val, v_size));
}

//***********************************************************************
// ostc_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

os_attr_iter_t *ostc_create_attr_iter(object_service_fn_t *os, creds_t *creds, os_fd_t *ofd, os_regex_table_t *attr, int v_max)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_attr_iter_t *it;
    ostc_fd_t *fd = (ostc_fd_t *)ofd;
    op_status_t status;

    if (fd->fd_child == NULL) {
        status = ostc_delayed_open_object(os, fd);
        if (status.op_status == OP_STATE_FAILURE) return(NULL);
    }

    type_malloc(it, ostc_attr_iter_t, 1);

    it->it = os_create_attr_iter(ostc->os_child, creds, fd->fd_child, attr, v_max);
    if (it == NULL) {
        free(it);
        return(NULL);
    }

    it->os = os;

    return(it);
}


//***********************************************************************
// ostc_destroy_attr_iter - Destroys an attribute iterator
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

void ostc_destroy_attr_iter(os_attr_iter_t *oit)
{
    ostc_attr_iter_t *it = (ostc_attr_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    os_destroy_attr_iter(ostc->os_child, it->it);
    free(it);
}

//***********************************************************************
// ostc_next_object - Returns the iterators next matching object
//***********************************************************************

int ostc_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    ostc_object_iter_t *it = (ostc_object_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;
    int ftype, i;

    log_printf(5, "START\n");

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return(-2);
    }

    ftype = os_next_object(ostc->os_child, it->it_child, fname, prefix_len);
    //** Last object so return
    if (ftype <= 0) {
        *fname = NULL;
        *prefix_len = -1;
        log_printf(5, "No more objects\n");
        return(ftype);
    }

    if (it->iter_type == OSTC_ITER_ALIST) {
        //** Copy any results back
        ostc_attr_cacheprep_copy(&(it->cp), it->val, it->v_size);
        ostc_cache_process_attrs(it->os, *fname, ftype, it->cp.key, it->cp.val, it->cp.v_size, it->n_keys);

        //** We have to do a manual cleanup and can't call the CP destroy method
        for (i=it->cp.n_keys; i<it->cp.n_keys_total; i++) {
            if (it->cp.val[i] != NULL) {
                free(it->cp.val[i]);
                it->cp.val[i] = NULL;
            }
        }
    }

    log_printf(5, "END\n");

    return(ftype);
}

//***********************************************************************
// ostc_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void ostc_destroy_object_iter(os_object_iter_t *oit)
{
    ostc_object_iter_t *it = (ostc_object_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return;
    }

    if (it->it_child != NULL) os_destroy_object_iter(ostc->os_child, it->it_child);
    if (it->iter_type == OSTC_ITER_ALIST) ostc_attr_cacheprep_destroy(&(it->cp));

    if (it->v_size_initial != NULL) free(it->v_size_initial);

    free(it);
}

//***********************************************************************
// ostc_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

os_object_iter_t *ostc_create_object_iter(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
        os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_iter_t *it;
    os_attr_iter_t **achild;

    log_printf(5, "START\n");


    //** Make the iterator handle
    type_malloc_clear(it, ostc_object_iter_t, 1);
    it->iter_type = OSTC_ITER_AREGEX;
    it->os = os;
    it->attr = attr;

    //** Set up the FD for the iterator
    it->fd.creds = creds;
    it->fd.fname = NULL;
    it->fd.mode = OS_MODE_READ_IMMEDIATE;
    it->fd.id = NULL;
    it->fd.max_wait = 60;

    achild = NULL;
    if (it_attr != NULL) {
        it->it_attr.os = os;
        achild = &(it->it_attr.it);
        *it_attr = (os_attr_iter_t *)&(it->it_attr);
    }

    //** Make the gop and execute it
    it->it_child = os_create_object_iter(ostc->os_child, creds,  path, object_regex, object_types, attr, recurse_depth, achild, v_max);

    log_printf(5, "it_child=%p\n", it->it_child);

    //** Clean up if an error occurred
    if (it->it_child == NULL) {
        ostc_destroy_object_iter(it);
        it = NULL;
    }

    log_printf(5, "END\n");

    return(it);
}

//***********************************************************************
// ostc_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *ostc_create_object_iter_alist(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
        int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_iter_t *it;

    log_printf(5, "START\n");


    //** Make the iterator handle
    type_malloc_clear(it, ostc_object_iter_t, 1);
    it->iter_type = OSTC_ITER_ALIST;
    it->os = os;
    it->val = val;
    it->v_size = v_size;
    it->n_keys = n_keys;
    ostc_attr_cacheprep_setup(&(it->cp), it->n_keys, key, val, v_size, 0);

    type_malloc(it->v_size_initial, int, n_keys);
    memcpy(it->v_size_initial, it->v_size, n_keys*sizeof(int));

    //** Make the gop and execute it
    it->it_child = os_create_object_iter_alist(ostc->os_child, creds, path, object_regex, object_types,
                   recurse_depth, it->cp.key, it->cp.val, it->cp.v_size, it->cp.n_keys_total);

    log_printf(5, "it_child=%p\n", it->it_child);
    //** Clean up if an error occurred
    if (it->it_child == NULL) {
        ostc_destroy_object_iter(it);
        it = NULL;
    }

    log_printf(5, "END\n");

    return(it);
}


//***********************************************************************
// ostc_open_object_fn - Handles the actual object open
//***********************************************************************

op_status_t ostc_open_object_fn(void *arg, int tid)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    op_status_t status;
    ostc_fd_t *fd;
    Stack_t tree;
    int err;

log_printf(5, "mode=%d OS_MODE_READ_IMMEDIATE=%d fname=%s\n", op->mode, OS_MODE_READ_IMMEDIATE, op->path);

    if (op->mode == OS_MODE_READ_IMMEDIATE) { //** Can use a delayed open if the object is in cache
        init_stack(&tree);
        OSTC_LOCK(ostc);
        err = _ostc_cache_tree_walk(op->os, op->path, &tree, NULL, 0, OSTC_MAX_RECURSE);
        OSTC_UNLOCK(ostc);
        empty_stack(&tree, 0);
        if (err == 0) goto finished;
    }

    //** Force an immediate file open
log_printf(5, "forced open of file. fname=%s\n", op->path);
    status = gop_sync_exec_status(op->gop);
    op->gop = NULL;

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

finished:
    //** Make my version of the FD
    type_malloc(fd, ostc_fd_t, 1);
    fd->fname = op->path;
    op->path = NULL;
    fd->fd_child = op->cfd;
    *op->fd = fd;
    fd->mode = op->mode;
    fd->creds = op->creds;
    fd->id = op->id;
    fd->max_wait = op->max_wait;

    return(op_success_status);
}


//***********************************************************************
//  ostc_open_free - Frees an open op structure
//***********************************************************************

void ostc_open_free(void *arg)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;

    if (op->path != NULL) free (op->path);
    if (op->gop != NULL) gop_free(op->gop, OP_DESTROY);
    free(op);
}

//***********************************************************************
//  ostc_open_object - Makes the open file op
//***********************************************************************

op_generic_t *ostc_open_object(object_service_fn_t *os, creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op;
    op_generic_t *gop;

    type_malloc(op, ostc_open_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->path = strdup(path);
    op->mode = mode;
    op->fd = pfd;
    op->id = id;
    op->max_wait = max_wait;
    op->cfd = NULL;

    op->gop = os_open_object(ostc->os_child, op->creds, op->path, op->mode, op->id, &(op->cfd), op->max_wait);
    gop = new_thread_pool_op(ostc->tpc, NULL, ostc_open_object_fn, (void *)op, ostc_open_free, 1);

    gop_set_private(gop, op);
    return(gop);
}

//***********************************************************************
//  ostc_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

op_generic_t *ostc_abort_open_object(object_service_fn_t *os, op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op = (ostc_open_op_t *)gop_get_private(gop);

    return(os_abort_open_object(ostc->os_child, op->gop));
}


//***********************************************************************
// ostc_close_object_fn - Handles the actual object close
//***********************************************************************

op_status_t ostc_close_object_fn(void *arg, int tid)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    op_status_t status;
    ostc_fd_t *fd = (ostc_fd_t *)op->close_fd;

    status = (fd->fd_child != NULL) ? gop_sync_exec_status(os_close_object(ostc->os_child, fd->fd_child)) : op_success_status;

    if (fd->fname != NULL) free(fd->fname);
    free(fd);

    return(status);
}

//***********************************************************************
//  ostc_close_object - Closes the object
//***********************************************************************

op_generic_t *ostc_close_object(object_service_fn_t *os, os_fd_t *ofd)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op;

    type_malloc(op, ostc_open_op_t, 1);
    op->os = os;
    op->close_fd = ofd;
    return(new_thread_pool_op(ostc->tpc, NULL, ostc_close_object_fn, (void *)op, free, 1));
}

//***********************************************************************
//  ostc_fsck_object - Allocates space for the object check
//***********************************************************************

op_generic_t *ostc_fsck_object(object_service_fn_t *os, creds_t *creds, char *fname, int ftype, int resolution)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_fsck_object(ostc->os_child, creds, fname, ftype, resolution));
}


//***********************************************************************
// ostc_next_fsck - Returns the next problem object
//***********************************************************************

int ostc_next_fsck(object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_next_fsck(ostc->os_child, oit, bad_fname, bad_atype));
}

//***********************************************************************
// ostc_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *ostc_create_fsck_iter(object_service_fn_t *os, creds_t *creds, char *path, int mode)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_create_fsck_iter(ostc->os_child, creds, path, mode));
}


//***********************************************************************
// ostc_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void ostc_destroy_fsck_iter(object_service_fn_t *os, os_fsck_iter_t *oit)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    os_destroy_fsck_iter(ostc->os_child, oit);
}


//***********************************************************************
// ostc_cred_init - Intialize a set of credentials
//***********************************************************************

creds_t *ostc_cred_init(object_service_fn_t *os, int type, void **args)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_cred_init(ostc->os_child, type, args));
}

//***********************************************************************
// ostc_cred_destroy - Destroys a set ot credentials
//***********************************************************************

void ostc_cred_destroy(object_service_fn_t *os, creds_t *creds)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_cred_destroy(ostc->os_child, creds));
}


//***********************************************************************
// ostc_destroy
//***********************************************************************

void ostc_destroy(object_service_fn_t *os)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    apr_status_t value;

    //** Shut the child down
    if (ostc->os_child != NULL) {
        os_destroy(ostc->os_child);
    }

    //** Signal we're shutting down
    OSTC_LOCK(ostc);
    ostc->shutdown = 1;
    apr_thread_cond_signal(ostc->cond);
    OSTC_UNLOCK(ostc);

    //** Wait for the cleanup thread to complete
    apr_thread_join(&value, ostc->cleanup_thread);

    //** Dump the cache 1 last time just to be safe
    _ostc_cleanup(os, ostc->cache_root, apr_time_now() + 4*ostc->entry_timeout);
    free_ostcdb_object(ostc->cache_root);

    free(ostc);
    free(os);
}

//***********************************************************************
//  object_service_timecache_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_timecache_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
    object_service_fn_t *os;
    ostc_priv_t *ostc;
    os_create_t *os_create;
    char *str, *ctype;

    log_printf(10, "START\n");
    if (section == NULL) section = "os_timecache";

    type_malloc_clear(os, object_service_fn_t, 1);
    type_malloc_clear(ostc, ostc_priv_t, 1);
    os->priv = (void *)ostc;

    str = inip_get_string(fd, section, "os_child", NULL);
    if (str != NULL) {  //** Running in test/temp
        ctype = inip_get_string(fd, str, "type", OS_TYPE_REMOTE_CLIENT);
        os_create = lookup_service(ess, OS_AVAILABLE, ctype);
        ostc->os_child = (*os_create)(ess, fd, str);
        if (ostc->os_child == NULL) {
            log_printf(1, "Error loading object service!  type=%s section=%s\n", ctype, str);
            fprintf(stderr, "Error loading object service!  type=%s section=%s\n", ctype, str);
            fflush(stderr);
            abort();
        }
        free(ctype);
        free(str);
    } else {
        log_printf(0, "ERROR:  Missing child OS!\n");
        abort();
    }

    ostc->entry_timeout = apr_time_from_sec(inip_get_integer(fd, section, "entry_timeout", 20));
    ostc->cleanup_interval = apr_time_from_sec(inip_get_integer(fd, section, "cleanup_interval", 120));

    apr_pool_create(&ostc->mpool, NULL);
    apr_thread_mutex_create(&(ostc->lock), APR_THREAD_MUTEX_DEFAULT, ostc->mpool);
    apr_thread_mutex_create(&(ostc->delayed_lock), APR_THREAD_MUTEX_DEFAULT, ostc->mpool);
    apr_thread_cond_create(&(ostc->cond), ostc->mpool);

    //** Make the root node
    ostc->cache_root = new_ostcdb_object(strdup("/"), OS_OBJECT_DIR, 0, ostc->mpool);

    //** Get the thread pool to use
    { int result = (ostc->tpc = lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED)); assert(result != NULL); }

    //** Set up the fn ptrs
    os->type = OS_TYPE_TIMECACHE;

    os->destroy_service = ostc_destroy;
    os->cred_init = ostc_cred_init;
    os->cred_destroy = ostc_cred_destroy;
    os->exists = ostc_exists;
    os->create_object = ostc_create_object;
    os->remove_object = ostc_remove_object;
    os->remove_regex_object = ostc_remove_regex_object;
    os->abort_remove_regex_object = ostc_abort_remove_regex_object;
    os->move_object = ostc_move_object;
    os->symlink_object = ostc_symlink_object;
    os->hardlink_object = ostc_hardlink_object;
    os->create_object_iter = ostc_create_object_iter;
    os->create_object_iter_alist = ostc_create_object_iter_alist;
    os->next_object = ostc_next_object;
    os->destroy_object_iter = ostc_destroy_object_iter;
    os->open_object = ostc_open_object;
    os->close_object = ostc_close_object;
    os->abort_open_object = ostc_abort_open_object;
    os->get_attr = ostc_get_attr;
    os->set_attr = ostc_set_attr;
    os->symlink_attr = ostc_symlink_attr;
    os->copy_attr = ostc_copy_attr;
    os->get_multiple_attrs = ostc_get_multiple_attrs;
    os->set_multiple_attrs = ostc_set_multiple_attrs;
    os->copy_multiple_attrs = ostc_copy_multiple_attrs;
    os->symlink_multiple_attrs = ostc_symlink_multiple_attrs;
    os->move_attr = ostc_move_attr;
    os->move_multiple_attrs = ostc_move_multiple_attrs;
    os->regex_object_set_multiple_attrs = ostc_regex_object_set_multiple_attrs;
    os->abort_regex_object_set_multiple_attrs = ostc_abort_regex_object_set_multiple_attrs;
    os->create_attr_iter = ostc_create_attr_iter;
    os->next_attr = ostc_next_attr;
    os->destroy_attr_iter = ostc_destroy_attr_iter;

    os->create_fsck_iter = ostc_create_fsck_iter;
    os->destroy_fsck_iter = ostc_destroy_fsck_iter;
    os->next_fsck = ostc_next_fsck;
    os->fsck_object = ostc_fsck_object;

    thread_create_assert(&(ostc->cleanup_thread), NULL, ostc_cache_compact_thread, (void *)os, ostc->mpool);

    log_printf(10, "END\n");

    return(os);
}

