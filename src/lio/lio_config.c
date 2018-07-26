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

#define _log_module_index 193

#include <apr.h>
#include <apr_dso.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <ctype.h>
#include <gop/mq_ongoing.h>
#include <gop/mq.h>
#include <gop/tp.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/skiplist.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "cache/amp.h"
#include "ds.h"
#include "ds/ibp.h"
#include "ex3.h"
#include "ex3/system.h"
#include "lio.h"
#include "remote_config.h"
#include "os.h"
#include "os/file.h"
#include "rs.h"
#include "rs/simple.h"
#include "service_manager.h"

typedef struct {
    int count;
    void *object;
    char *key;
} lc_object_container_t;

typedef struct {
    char *prefix;
    int len;
} lfs_mount_t;

int lio_parallel_task_count = 100;

// ** Define the global LIO config
lio_config_t *lio_gc = NULL;
lio_cache_t *_lio_cache = NULL;
tbx_log_fd_t *lio_ifd = NULL;
FILE *_lio_ifd = NULL;
char *_lio_exe_name = NULL;

int _lfs_mount_count = -1;
lfs_mount_t *lfs_mount = NULL;

apr_pool_t *_lc_mpool = NULL;
apr_thread_mutex_t *_lc_lock = NULL;
tbx_list_t *_lc_object_list = NULL;

lio_config_t *lio_create_nl(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name);
void lio_destroy_nl(lio_config_t *lio);

char **myargv = NULL;  // ** This is used to hold the new argv we return from lio_init so we can properly clean it up

//***************************************************************
// check_for_section - Checks to make sure the section
//    exists in the config file and if it doesn't it complains
//    and exists
//***************************************************************

void check_for_section(tbx_inip_file_t *fd, char *section, char *err_string)
{
    if (tbx_inip_group_find(fd, section) == NULL) {
        fprintf(stderr, "Missing section! section=%s\n", section);
        fprintf(stderr, "%s", err_string);
        fflush(stderr);
        exit(EINVAL);
    }
}

//***************************************************************
//  _lc_object_destroy - Decrements the LC object and removes it
//       if no other references exist.  It returns the number of
//       remaining references.  So it can be safely destroyed
//       when 0 is returned.
//***************************************************************

int _lc_object_destroy(char *key)
{
    int count = 0;
    lc_object_container_t *lcc;

    lcc = tbx_list_search(_lc_object_list, key);
    if (lcc != NULL) {
        lcc->count--;
        count = lcc->count;
        log_printf(15, "REMOVE key=%s count=%d lcc=%p\n", key, count, lcc);

        if (lcc->count <= 0) {
            tbx_list_remove(_lc_object_list, key, lcc);
            free(lcc->key);
            free(lcc);
        }
    } else {
        log_printf(15, "REMOVE-FAIL key=%s\n", key);
    }

    return(count);
}

//***************************************************************
//  _lc_object_get - Retrieves an LC object
//***************************************************************

void *_lc_object_get(char *key)
{
    void *obj = NULL;
    lc_object_container_t *lcc;

    lcc = tbx_list_search(_lc_object_list, key);
    if (lcc != NULL) {
        lcc->count++;
        obj = lcc->object;
    }

    if (obj != NULL) {
        log_printf(15, "GET key=%s count=%d lcc=%p\n", key, lcc->count, lcc);
    } else {
        log_printf(15, "GET-FAIL key=%s\n", key);
    }
    return(obj);
}

//***************************************************************
//  lc_object_put - Adds an LC object
//***************************************************************

void _lc_object_put(char *key, void *obj)
{
    lc_object_container_t *lcc;

    tbx_type_malloc(lcc, lc_object_container_t, 1);

    log_printf(15, "PUT key=%s count=1 lcc=%p\n", key, lcc);

    lcc->count = 1;
    lcc->object = obj;
    lcc->key = strdup(key);
    tbx_list_insert(_lc_object_list, lcc->key, lcc);

    return;
}

//***************************************************************
// _lio_load_plugins - Loads the optional plugins
//***************************************************************

void _lio_load_plugins(lio_config_t *lio, tbx_inip_file_t *fd)
{
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *key;
    apr_dso_handle_t *handle;
    int error;
    void *sym;
    char *section, *name, *library, *symbol;
    char buffer[1024];

    g = tbx_inip_group_first(fd);
    while (g) {
        if (strcmp(tbx_inip_group_get(g), "plugin") == 0) { //** Got a Plugin section
            //** Parse the plugin section
            ele = tbx_inip_ele_first(g);
            section = name = library = symbol = NULL;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                if (strcmp(key, "section") == 0) {
                    section = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "name") == 0) {
                    name = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "library") == 0) {
                    library = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "symbol") == 0) {
                    symbol = tbx_inip_ele_get_value(ele);
                }

                ele = tbx_inip_ele_next(ele);
            }

            //** Sanity check it
            error = 1;
            if ((section == NULL) || (name == NULL) || (library == NULL) || (symbol == NULL)) goto fail;

            //** Attempt to load it
            snprintf(buffer, sizeof(buffer), "plugin_handle:%s", library);
            handle = _lc_object_get(buffer);
            if (handle == NULL) {
                if (apr_dso_load(&handle, library, _lc_mpool) != APR_SUCCESS) goto fail;
                _lc_object_put(buffer, handle);
                add_service(lio->ess, "plugin", buffer, handle);
            }
            if (apr_dso_sym(&sym, handle, symbol) != APR_SUCCESS) goto fail;

            if (lio->plugin_stack == NULL) lio->plugin_stack = tbx_stack_new();
            log_printf(5, "library=%s\n", buffer);
            tbx_stack_push(lio->plugin_stack, strdup(buffer));
            add_service(lio->ess, section, name, sym);

            error = 0;  //** Skip cleanup
fail:
            if (error != 0) {
                log_printf(0, "ERROR loading plugin!  section=%s name=%s library=%s symbol=%s\n", section, name, library, symbol);
            }
        }
        g = tbx_inip_group_next(g);
    }
}

//***************************************************************
// _lio_destroy_plugins - Destroys the optional plugins
//***************************************************************

void _lio_destroy_plugins(lio_config_t *lio)
{
    char *library_key;
    apr_dso_handle_t *handle;
    int count;

    if (lio->plugin_stack == NULL) return;

    while ((library_key = tbx_stack_pop(lio->plugin_stack)) != NULL) {
        log_printf(5, "library_key=%s\n", library_key);
        count = _lc_object_destroy(library_key);
        if (count <= 0) {
            handle = lio_lookup_service(lio->ess, "plugin", library_key);
            if (handle != NULL) apr_dso_unload(handle);
        }
        free(library_key);
    }

    tbx_stack_free(lio->plugin_stack, 0);
}


//***************************************************************
//  lio_find_lfs_mounts - Finds the LFS mounts and creates the global
//     internal table for use by the path commands
//***************************************************************

void lio_find_lfs_mounts()
{
    tbx_stack_t *stack;
    FILE *fd;
    lfs_mount_t *entry;
    char *text, *prefix, *bstate;
    int i, fin;
    size_t ns;

    stack = tbx_stack_new();

    fd = fopen("/proc/mounts", "r");
    if (fd) {
        text = NULL;
        while (getline(&text, &ns, fd) != -1) {
            log_printf(5, "getline=%s", text);
            if (strncasecmp(text, "lfs:", 4) == 0) { //** Found a match
                tbx_stk_string_token(text, " ", &bstate, &fin);
                prefix = tbx_stk_string_token(NULL, " ", &bstate, &fin);
                if (prefix != NULL) { //** Add it
                    tbx_type_malloc_clear(entry, lfs_mount_t, 1);
                    entry->prefix = strdup(prefix);
                    entry->len = strlen(entry->prefix);
                    tbx_stack_push(stack, entry);
                    log_printf(5, "mount prefix=%s len=%d\n", entry->prefix, entry->len);
                }
            }

            free(text);
            text = NULL;
        }
        if (text != NULL)
            free(text);  //** Getline() always returns something
    }
    //** Convert it to a simple array
    _lfs_mount_count = tbx_stack_count(stack);
    tbx_type_malloc(lfs_mount, lfs_mount_t, _lfs_mount_count);
    for (i=0; i<_lfs_mount_count; i++) {
        entry = tbx_stack_pop(stack);
        lfs_mount[i].prefix = entry->prefix;
        lfs_mount[i].len = entry->len;
        free(entry);
    }

    tbx_stack_free(stack, 0);
}

//***************************************************************
//  lio_path_release - Decrs a path tuple object
//***************************************************************

void lio_path_release(lio_path_tuple_t *tuple)
{
    char buffer[1024];

    if (tuple->path != NULL) free(tuple->path);
    if (tuple->lc == NULL) return;

    apr_thread_mutex_lock(_lc_lock);
    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_id(tuple->creds), tuple->lc->obj_name);

    log_printf(15, "START object=%s\n", buffer);

    if (_lc_object_destroy(buffer) <= tuple->lc->anonymous_creation) {
        lio_destroy_nl(tuple->lc);
    }

    log_printf(15, "END object=%s\n", buffer);

    apr_thread_mutex_unlock(_lc_lock);

    return;
}

//***************************************************************
// lio_path_local_make_absolute - Converts the local relative
//     path to an absolute one.
//***************************************************************

void lio_path_local_make_absolute(lio_path_tuple_t *tuple)
{
    char *p, *rp, *pp;
    int i, n, last_slash, glob_index;
    char path[OS_PATH_MAX];
    char c;

    log_printf(5, "initial path=%s\n", tuple->path);
    if (tuple->path == NULL) return;

    p = tuple->path;
    n = strlen(p);
    last_slash = -1;
    glob_index = -1;
    if ((p[0] == '*') || (p[0] == '?') || (p[0] == '[')) goto wildcard;

    for (i=0; i<n; i++) {
        if (p[i] == '/') last_slash = i;
        if ((p[i] == '*') || (p[i] == '?') || (p[i] == '[')) {
            if (p[i-1] != '\\') {
                glob_index = i;
                break;
            }
        }
    }

    log_printf(5, "last_slash=%d\n", last_slash);

wildcard:
    if (last_slash == -1) {  //** Just using the CWD as the base for the glob
        if (strcmp(p, ".") == 0) {
            pp = realpath(".", path);
            last_slash = n;
        } else if (strcmp(p, "..") == 0) {
            pp = realpath("..", path);
            last_slash = n;
        } else {
            pp = realpath(".", path);
            last_slash = 0;
        }

        if (last_slash != n) {
            if (pp == NULL) i = 0;  //** This is a NULL statement but it makes the compiler happy about not using the return of realpath
            i = strlen(path);
            path[i] = '/';  //** Need to add the trailing / to the path
            path[i+1] = 0;
        }
        rp = strdup(path);
    } else {
        if (last_slash == -1) last_slash = n;
        c = p[last_slash];
        p[last_slash] = 0;
        rp = realpath(p, NULL);
        p[last_slash] = c;
        if ((p[n-1] == '/') && (last_slash == n)) last_slash--;  //** '/' terminator so preserve it
    }

    log_printf(5, "p=%s realpath=%s last_slash=%d n=%d glob_index=%d\n", p, rp, last_slash, n, glob_index);

    if (rp != NULL) {
        if (last_slash == n) {
            tuple->path = rp;
        } else {
            snprintf(path, sizeof(path), "%s%s", rp, &(p[last_slash]));
            tuple->path = strdup(path);
            free(rp);
        }
        free(p);
    }
}

//***************************************************************
// lio_path_wildcard_auto_append - Auto appends a "*" if the path
//      ends in "/".
//     Returns 0 if no change and 1 if a wildcard was added.
//***************************************************************

int lio_path_wildcard_auto_append(lio_path_tuple_t *tuple)
{
    int n;

    if (tuple->path == NULL) return(0);

    n = strlen(tuple->path);
    if (tuple->path[n-1] == '/') {
        tuple->path = realloc(tuple->path, n+1+1);
        tuple->path[n] = '*';
        tuple->path[n+1] = 0;

        log_printf(5, " tweaked tuple=%s\n", tuple->path);
        return(1);
    }

    return(0);
}

//*************************************************************************
// lio_path_tuple_copy - Returns a new tuple with just the path different
//*************************************************************************

lio_path_tuple_t lio_path_tuple_copy(lio_path_tuple_t *curr, char *fname)
{
    lio_path_tuple_t tuple;
    lio_path_tuple_t *t2;
    char buffer[4096];

    tuple = *curr;
    tuple.path = fname;

    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_id(curr->creds), curr->lc->obj_name);
    apr_thread_mutex_lock(_lc_lock);    
    t2 = _lc_object_get(buffer);
    apr_thread_mutex_unlock(_lc_lock);

    if (t2 == NULL) {
        log_printf(0, "ERROR: missing tuple! obj=%s\n", buffer);
        fprintf(stderr, "ERROR: missing tuple! obj=%s\n", buffer);
        abort();
    }
    return(tuple);
}

//***************************************************************
//  lio_path_resolve_base - Returns a  path tuple object
//      containing the cred, lc, and path
//***************************************************************

lio_path_tuple_t lio_path_resolve_base(char *lpath)
{
    char *userid,*pp_section, *fname, *pp_mq, *pp_host, *pp_cfg, *config, *obj_name;
    void *cred_args[2];
    lio_path_tuple_t tuple, *tuple2;
    tbx_inip_file_t *ifd;
    char uri[1024];
    char buffer[1024];
    int n, is_lio, pp_port;

    userid = NULL;
    pp_mq = NULL;
    pp_host = NULL;
    pp_port = 0;
    pp_cfg = NULL;
    pp_section = NULL;
    fname = NULL;
    is_lio = lio_parse_path(lpath, &userid, &pp_mq, &pp_host, &pp_port, &pp_cfg, &pp_section, &fname);
    if (is_lio == -1) { //** Can't parse the path
        memset(&tuple, 0, sizeof(tuple));
        goto finished;
    }

    //** See if we have a path.  If we don't then blow away what we got above and assume it's all a path
    if (!fname) {
        fname = strdup(lpath);
        is_lio = 0;
        strncpy(uri, lio_gc->obj_name, sizeof(uri));        
    } else if ((lio_gc) && (!pp_mq) && (!pp_host) && (!pp_cfg) && (!pp_section) && (pp_port == 0)) { //** Check if we just have defaults if so use the global context
        strncpy(uri, lio_gc->obj_name, sizeof(uri));
    } else {
        if (!pp_mq) pp_mq = strdup("RC");
        if (pp_port == 0) pp_port = 6711;
        if (!pp_section) pp_section = strdup("lio");

        //** Based on the host we may need to adjust the config file
        if (pp_host) {
            if (!pp_cfg) pp_cfg = strdup("lio");
        } else if (!pp_cfg) {
            pp_cfg = strdup("LOCAL");
        }

        snprintf(uri, sizeof(uri), "lstore://%s|%s:%d:%s:%s", pp_mq, pp_host, pp_port, pp_cfg, pp_section);
    }

    log_printf(15, "START: lpath=%s user=%s uri=%s path=%s\n", lpath, userid, uri, fname);

    apr_thread_mutex_lock(_lc_lock);

    if (userid == NULL) {
        snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_id(lio_gc->creds), uri);
    } else {
        snprintf(buffer, sizeof(buffer), "tuple:%s@%s", userid, uri);
    }

    tuple2 = _lc_object_get(buffer);
    if (tuple2 != NULL) { //** Already exists!
        tuple = *tuple2;
        tuple.path = fname;
        goto finished;
    }

    //** Get the LC
    n = 0;
    tuple.lc = _lc_object_get(uri);
    if (tuple.lc == NULL) { //** Doesn't exist so need to load it
        if (pp_host == NULL) {
            tuple.lc = lio_create_nl(lio_gc->ifd, pp_section, userid, uri, _lio_exe_name);  //** USe the non-locking routine
            if (tuple.lc == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname != NULL) free(fname);
                goto finished;
            }
            tuple.lc->ifd = tbx_inip_dup(tuple.lc->ifd);  //** Dup the ifd
        } else { //** Look up using the remote config query
            if (rc_client_get_config(uri, &config, &obj_name) != 0) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname != NULL) free(fname);
                goto finished;
            }

            strncpy(uri, obj_name, sizeof(uri));
            free(obj_name);

            ifd = tbx_inip_string_read(config);
            if (config) free(config);
            if (ifd == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname) free(fname);
                goto finished;
            }
            tuple.lc = lio_create_nl(ifd, pp_section, userid, uri, _lio_exe_name);  //** USe the non-locking routine
            if (tuple.lc == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                tbx_inip_destroy(ifd);
                if (fname != NULL) free(fname);
                goto finished;
            }
        }

        tuple.lc->anonymous_creation = 1;
        n = 1; //** Flag as anon for cred check
    }

    //** Now determine the user
    if (userid == NULL) {
        userid = an_cred_get_id(tuple.lc->creds);  //** IF not specified default to the one in the LC
    }

    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", userid, uri);
    tuple2 = _lc_object_get(buffer);
    if (tuple2 == NULL) { //** Doesn't exist so insert the tuple
        cred_args[0] = tuple.lc->ifd;
        cred_args[1] = strdup(userid);
        tuple.creds = os_cred_init(tuple.lc->os, OS_CREDS_INI_TYPE, (void **)cred_args);
        tbx_type_malloc_clear(tuple2, lio_path_tuple_t, 1);
        tuple2->creds = tuple.creds;
        tuple2->lc = tuple.lc;
        tuple2->path = "ANONYMOUS";
        log_printf(15, "adding anon creds tuple=%s\n", buffer);
        _lc_object_put(buffer, tuple2);  //** Add it to the table
    } else if (n==1) {//** This is default user tuple just created so mark it as anon as well
        log_printf(15, "marking anon creds tuple=%s\n", buffer);
        tuple2->path = "ANONYMOUS-DEFAULT";
    }

    tuple.creds = tuple2->creds;
    tuple.path = fname;

finished:
    apr_thread_mutex_unlock(_lc_lock);

    if (pp_mq != NULL) free(pp_mq);
    if (pp_host != NULL) free(pp_host);
    if (pp_cfg != NULL) free(pp_cfg);
    if (pp_section != NULL) free(pp_section);

    tuple.is_lio = is_lio;
    log_printf(15, "END: uri=%s path=%s is_lio=%d\n", tuple.path, uri, tuple.is_lio);
    return(tuple);
}

//***************************************************************
// lio_path_auto_fuse_convert - Automatically detects and converts
//    local paths sitting on a LFS mount
//***************************************************************

lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *ltuple)
{
    char path[OS_PATH_MAX];
    lio_path_tuple_t tuple;
    int do_convert, prefix_len, i;

    //** Convert if to an absolute path
    lio_path_local_make_absolute(ltuple);
    tuple = *ltuple;

    //** Now check if the prefixes match
    for (i=0; i < _lfs_mount_count; i++) {
        if (strncmp(ltuple->path, lfs_mount[i].prefix, lfs_mount[i].len) == 0) {
            do_convert = 0;
            prefix_len = lfs_mount[i].len;
            if ((int)strlen(ltuple->path) > prefix_len) {
                if (ltuple->path[prefix_len] == '/') {
                    do_convert = 1;
                    snprintf(path, sizeof(path), "@:%s", &(ltuple->path[prefix_len]));
                }
            } else {
                do_convert = 1;
                snprintf(path, sizeof(path), "@:/");
            }

            if (do_convert == 1) {
                log_printf(5, "auto convert\n");
                log_printf(5, "auto convert path=%s\n", path);
                tuple = lio_path_resolve_base(path);
                lio_path_release(ltuple);
                break;  //** Found a match so kick out
            }
        }
    }

    return(tuple);
}

//***************************************************************
//  lio_path_resolve - Returns a  path tuple object
//      containing the cred, lc, and path
//***************************************************************

lio_path_tuple_t lio_path_resolve(int auto_fuse_convert, char *lpath)
{
    lio_path_tuple_t tuple;

    tuple = lio_path_resolve_base(lpath);

    log_printf(5, "auto_fuse_convert=%d\n", auto_fuse_convert);
    if ((tuple.is_lio == 0) && (auto_fuse_convert > 0)) {
        return(lio_path_auto_fuse_convert(&tuple));
    }

    return(tuple);
}

//***************************************************************
// lc_object_remove_unused  - Removes unused LC's from the global
//     table.  The default, remove_all_unsed=0, is to only
//     remove anonymously created LC's.
//***************************************************************

void lc_object_remove_unused(int remove_all_unused)
{
    tbx_list_t *user_lc;
    tbx_list_iter_t it;
    lc_object_container_t *lcc, *lcc2;
    lio_path_tuple_t *tuple;
    lio_config_t *lc;
    char *key;
    tbx_stack_t *stack;

    stack = tbx_stack_new();

    apr_thread_mutex_lock(_lc_lock);

    //** Make a list of all the different LC's in use from the tuples
    //** Keep track of the anon creds for deletion
    user_lc = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);
    it = tbx_list_iter_search(_lc_object_list, "tuple:", 0);
    while ((tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&lcc)) == 0) {
        if (strncmp(lcc->key, "tuple:", 6) != 0) break;  //** No more tuples
        tuple = lcc->object;
        if (tuple->path == NULL) {
            tbx_list_insert(user_lc, tuple->lc->obj_name, tuple->lc);
            log_printf(15, "user_lc adding key=%s lc=%s\n", lcc->key, tuple->lc->obj_name);
        } else {
            log_printf(15, "user_lc marking creds key=%s for removal\n", lcc->key);
            if (strcmp(tuple->path, "ANONYMOUS") == 0) tbx_stack_push(stack, lcc);
        }
    }

    //** Go ahead and delete the anonymously created creds (as long as they aren't the LC default
    while ((lcc = tbx_stack_pop(stack)) != NULL) {
        tuple = lcc->object;
        _lc_object_destroy(lcc->key);
//     if (strcmp(tuple->path, "ANONYMOUS") == 0) os_cred_destroy(tuple->lc->os, tuple->creds);
        os_cred_destroy(tuple->lc->os, tuple->creds);
    }

    //** Now iterate through all the LC's
    it = tbx_list_iter_search(_lc_object_list, "lstore://", 0);
    while ((tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&lcc)) == 0) {
        if (strncmp(lcc->key, "lstore://", 9) != 0) break;  //** No more LCs
        lc = lcc->object;
        log_printf(15, "checking key=%s lc=%s anon=%d count=%d\n", lcc->key, lc->obj_name, lc->anonymous_creation, lcc->count);
        lcc2 = tbx_list_search(user_lc, lc->obj_name);
        if (lcc2 == NULL) {  //** No user@lc reference so safe to delete from that standpoint
            log_printf(15, "not in user_lc key=%s lc=%s anon=%d count=%d\n", lcc->key, lc->obj_name, lc->anonymous_creation, lcc->count);
            if (((lc->anonymous_creation == 1) && (lcc->count <= 1)) ||
                    ((remove_all_unused == 1) && (lcc->count <= 0))) {
                tbx_stack_push(stack, lcc);
            }
        }
    }

    while ((lcc = tbx_stack_pop(stack)) != NULL) {
        lc = lcc->object;
        _lc_object_destroy(lcc->key);
        lio_destroy_nl(lc);
    }

    apr_thread_mutex_unlock(_lc_lock);

    tbx_stack_free(stack, 0);
    tbx_list_destroy(user_lc);

    return;
}

//***************************************************************
// lio_print_options - Prints the standard supported lio options
//   Use "LIO_COMMON_OPTIONS" in the arg list
//***************************************************************

void lio_print_options(FILE *fd)
{
    fprintf(fd, "    LIO_COMMON_OPTIONS\n");
    fprintf(fd, "       -d level           - Set the debug level (0-20).  Defaults to 0\n");
    fprintf(fd, "       -log log_out       - Set the log output file.  Defaults to using config setting\n");
    fprintf(fd, "       -no-auto-lfs       - Disable auto-conversion of LFS mount paths to lio\n");
    fprintf(fd, "       -c config_uri      - Config file to use.  Either local or remote\n");
    fprintf(fd, "          [lstore://][user@][MQ_NAME|]HOST:[port:][cfg:][section]\n");
    fprintf(fd, "                            Get the config from a remote LServer\n");
    fprintf(fd, "          [ini://]/path/to/ini_file\n");
    fprintf(fd, "                            Local INI config file\n");
    fprintf(fd, "          file:///path/to/file\n");
    fprintf(fd, "                            File with a single line containing either an lstore or init URI\n");
    fprintf(fd, "       -lc user@config    - Use the user and config section specified for making the default LIO\n");
    fprintf(fd, "       -np N              - Number of simultaneous operations. Default is %d.\n", lio_parallel_task_count);
    fprintf(fd, "       -i N               - Print information messages of level N or greater. No header is printed\n");
    fprintf(fd, "       -it N              - Print information messages of level N or greater. Thread ID header is used\n");
    fprintf(fd, "       -if N              - Print information messages of level N or greater. Full header is used\n");
    fprintf(fd, "       -ilog info_log_out - Where to send informational log output.\n");
    fprintf(fd, "       --print-config     - Print the loaded config.\n");
    fprintf(fd, "\n");
    tbx_inip_print_hint_options(fd);
}

//***************************************************************
//  lio_destroy_nl - Destroys a LIO config object - NO locking
//***************************************************************

void lio_destroy_nl(lio_config_t *lio)
{
    lc_object_container_t *lcc;
    lio_path_tuple_t *tuple;

    //** Update the lc count for the creds
    log_printf(1, "Destroying LIO context %s\n", lio->obj_name);

    if (_lc_object_destroy(lio->obj_name) > 0) {  //** Still in use so return.
        return;
    }

    //** The creds is a little tricky cause we need to get the tuple first
    lcc = tbx_list_search(_lc_object_list, lio->creds_name);
    tuple = (lcc != NULL) ? lcc->object : NULL;
    if (_lc_object_destroy(lio->creds_name) <= 0) {
        os_cred_destroy(lio->os, lio->creds);
        free(tuple);
    }

    free(lio->creds_name);

    log_printf(15, "removing lio=%s\n", lio->obj_name);

    if (_lc_object_destroy(lio->rs_section) <= 0) {
        rs_destroy_service(lio->rs);
    }
    free(lio->rs_section);

    ds_attr_destroy(lio->ds, lio->da);
    if (_lc_object_destroy(lio->ds_section) <= 0) {
        ds_destroy_service(lio->ds);
    }
    free(lio->ds_section);

    if (_lc_object_destroy(lio->os_section) <= 0) {
        os_destroy_service(lio->os);
    }
    free(lio->os_section);

    if (_lc_object_destroy(lio->tpc_unlimited_section) <= 0) {
        gop_tp_context_destroy(lio->tpc_unlimited);
    }
    free(lio->tpc_unlimited_section);

    if (_lc_object_destroy(lio->tpc_cache_section) <= 0) {
        gop_tp_context_destroy(lio->tpc_cache);
    }
    free(lio->tpc_cache_section);

    if (_lc_object_destroy(ESS_ONGOING_CLIENT) <= 0) {
        gop_mq_ongoing_t *on = lio_lookup_service(lio->ess, ESS_RUNNING, ESS_ONGOING_CLIENT);
        if (on != NULL) {  //** And also the ongoing client
            gop_mq_ongoing_destroy(on);
        }
    }

    if (_lc_object_destroy(lio->mq_section) <= 0) {  //** Destroy the MQ context
        gop_mq_destroy_context(lio->mqc);
    }
    free(lio->mq_section);

    if (lio->section_name != NULL) free(lio->section_name);

    void *val = lio_lookup_service(lio->ess, ESS_RUNNING, "jerase_paranoid");
    remove_service(lio->ess, ESS_RUNNING, "jerase_paranoid");
    if (val) free(val);

    _lio_destroy_plugins(lio);

    lio_exnode_service_set_destroy(lio->ess);
    lio_exnode_service_set_destroy(lio->ess_nocache);

    tbx_inip_destroy(lio->ifd);

    //** Table of open files
    tbx_list_destroy(lio->open_index);

    //** Remove ourselves to the info handler
    tbx_siginfo_handler_remove(lio_open_files_info_fn, lio);

    //** Blacklist if used
    if (lio->blacklist != NULL) blacktbx_list_destroy(lio->blacklist);

    if (lio->obj_name) free(lio->obj_name);

    apr_thread_mutex_destroy(lio->lock);
    apr_pool_destroy(lio->mpool);

    if (lio->exe_name != NULL) free(lio->exe_name);
    free(lio);

    return;
}

//***************************************************************
//  lio_destroy - Destroys a LIO config object
//***************************************************************

void lio_destroy(lio_config_t *lio)
{
    apr_thread_mutex_lock(_lc_lock);
    lio_destroy_nl(lio);
    apr_thread_mutex_unlock(_lc_lock);
}

//***************************************************************
// lio_create_nl - Creates a lio configuration according to the config file
//   NOTE:  No locking is used
//***************************************************************

lio_config_t *lio_create_nl(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name)
{
    lio_config_t *lio;
    int n, cores, max_recursion;
    char buffer[1024];
    void *cred_args[2];
    char *ctype, *stype;
    ds_create_t *ds_create;
    rs_create_t *rs_create;
    os_create_t *os_create;
    cache_load_t *cache_create;
    lio_path_tuple_t *tuple;
    gop_mq_ongoing_t *on = NULL;
    int *val;

    //** Add the LC first cause it may already exist
    log_printf(1, "START: Creating LIO context %s\n", obj_name);

    lio = _lc_object_get(obj_name);
    if (lio != NULL) {  //** Already loaded so can skip this part
        return(lio);
    }

    tbx_type_malloc_clear(lio, lio_config_t, 1);
    lio->ess = lio_exnode_service_set_create();
    lio->auto_translate = 1;
    if (exe_name) lio->exe_name = strdup(exe_name);

    //** Add it to the table for ref counting
    _lc_object_put(obj_name, lio);

    lio->obj_name = strdup(obj_name);

    lio->ifd = ifd;
    if (lio->ifd == NULL) {
        // TODO: The error handling here needs to be more measured
        log_printf(-1, "ERROR: Failed to parse INI1\n");
        return NULL;
    }

    _lio_load_plugins(lio, lio->ifd);  //** Load the plugins

    check_for_section(lio->ifd, section, "No primary LIO config section!\n");
    lio->timeout = tbx_inip_get_integer(lio->ifd, section, "timeout", 120);
    lio->max_attr = tbx_inip_get_integer(lio->ifd, section, "max_attr_size", 10*1024*1024);
    lio->calc_adler32 = tbx_inip_get_integer(lio->ifd, section, "calc_adler32", 0);
    lio->readahead = tbx_inip_get_integer(lio->ifd, section, "readahead", 0);
    lio->readahead_trigger = lio->readahead * tbx_inip_get_double(lio->ifd, section, "readahead_trigger", 1.0);

    //** Check and see if we need to enable the blacklist
    stype = tbx_inip_get_string(lio->ifd, section, "blacklist", NULL);
    if (stype != NULL) { //** Yup we need to parse and load those params
        check_for_section(lio->ifd, section, "No blacklist section found!\n");
        lio->blacklist = blacklist_load(lio->ifd, stype);
        add_service(lio->ess, ESS_RUNNING, "blacklist", lio->blacklist);
        free(stype);
    }

    //** Add the Jerase paranoid option
    tbx_type_malloc(val, int, 1);  //** NOTE: this is not freed on a destroy
    *val = tbx_inip_get_integer(lio->ifd, section, "jerase_paranoid", 0);
    add_service(lio->ess, ESS_RUNNING, "jerase_paranoid", val);

    cores = tbx_inip_get_integer(lio->ifd, section, "tpc_unlimited", 200);
    max_recursion = tbx_inip_get_integer(lio->ifd, section, "tpc_max_recursion", 10);
    sprintf(buffer, "tpc:%d", cores);
    stype = buffer;
    lio->tpc_unlimited_section = strdup(stype);
    lio->tpc_unlimited = _lc_object_get(stype);
    if (lio->tpc_unlimited == NULL) {  //** Need to load it
        n = 0.1 * cores;
        if (n > 10) n = 10;
        if (n <= 0) n = 1;
        lio->tpc_unlimited = gop_tp_context_create("UNLIMITED", n, cores, max_recursion);
        if (lio->tpc_unlimited == NULL) {
            log_printf(0, "Error loading tpc_unlimited threadpool!  n=%d\n", cores);
            fprintf(stderr, "ERROR createing tpc_unlimited threadpool! n=%d\n", cores);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->tpc_unlimited);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_TPC_UNLIMITED, lio->tpc_unlimited);


    cores = tbx_inip_get_integer(lio->ifd, section, "tpc_cache", 100);
    sprintf(buffer, "tpc-cache:%d", cores);
    stype = buffer;
    lio->tpc_cache_section = strdup(stype);
    lio->tpc_cache = _lc_object_get(stype);
    if (lio->tpc_cache == NULL) {  //** Need to load it
        n = 0.1 * cores;
        if (n > 10) n = 10;
        if (n <= 0) n = 1;
        lio->tpc_cache = gop_tp_context_create("CACHE", n, cores, max_recursion);
        if (lio->tpc_cache == NULL) {
            log_printf(0, "Error loading tpc_cache threadpool!  n=%d\n", cores);
            fprintf(stderr, "ERROR createing tpc_cache threadpool! n=%d\n", cores);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->tpc_cache);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_TPC_CACHE, lio->tpc_cache);


    stype = tbx_inip_get_string(lio->ifd, section, "mq", "mq_context");
    check_for_section(lio->ifd, stype, "No MQ context in LIO config!\n");
    lio->mq_section = stype;
    lio->mqc = _lc_object_get(stype);
    if (lio->mqc == NULL) {  //** Need to load it
        lio->mqc = gop_mq_create_context(lio->ifd, stype);
        if (lio->mqc == NULL) {
            log_printf(1, "Error loading MQ service! stype=%s\n", stype);
            fprintf(stderr, "ERROR loading MQ service! stype=%s\n", stype);
            fflush(stderr);
            abort();
        } else {
            add_service(lio->ess, ESS_RUNNING, ESS_MQ, lio->mqc);  //** It's used by the block loader
        }

        _lc_object_put(stype, lio->mqc);  //** Add it to the table

        //** Add the shared ongoing object
        on = gop_mq_ongoing_create(lio->mqc, NULL, 1, ONGOING_CLIENT);
        _lc_object_put(ESS_ONGOING_CLIENT, on);  //** Add it to the table
    } else {
        on = _lc_object_get(ESS_ONGOING_CLIENT);
    }

    add_service(lio->ess, ESS_RUNNING, ESS_ONGOING_CLIENT, on);

    stype = tbx_inip_get_string(lio->ifd, section, "ds", DS_TYPE_IBP);
    check_for_section(lio->ifd, stype, "No primary Data Service (ds) found in LIO config!\n");
    lio->ds_section = stype;
    lio->ds = _lc_object_get(stype);
    if (lio->ds == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", DS_TYPE_IBP);
        ds_create = lio_lookup_service(lio->ess, DS_SM_AVAILABLE, ctype);
        lio->ds = (*ds_create)(lio->ess, lio->ifd, stype);
        if (lio->ds == NULL) {
            log_printf(1, "Error loading data service!  type=%s\n", ctype);
            fprintf(stderr, "ERROR loading data service!  type=%s\n", ctype);
            fflush(stderr);
            abort();
        } else {
            add_service(lio->ess, DS_SM_RUNNING, ctype, lio->ds);  //** It's used by the block loader
        }
        free(ctype);

        _lc_object_put(stype, lio->ds);  //** Add it to the table
    } else {
        add_service(lio->ess, DS_SM_RUNNING, stype, lio->ds);  //** It's used by the block loader
    }
    lio->da = ds_attr_create(lio->ds);

    add_service(lio->ess, ESS_RUNNING, ESS_DS, lio->ds);  //** This is needed by the RS service
    add_service(lio->ess, ESS_RUNNING, ESS_DA, lio->da);  //** This is needed by the RS service

    stype = tbx_inip_get_string(lio->ifd, section, "rs", RS_TYPE_SIMPLE);
    check_for_section(lio->ifd, stype, "No Resource Service (rs) found in LIO config!\n");
    lio->rs_section = stype;
    lio->rs = _lc_object_get(stype);
    if (lio->rs == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", RS_TYPE_SIMPLE);
        rs_create = lio_lookup_service(lio->ess, RS_SM_AVAILABLE, ctype);
        lio->rs = (*rs_create)(lio->ess, lio->ifd, stype);
        if (lio->rs == NULL) {
            log_printf(1, "Error loading resource service!  type=%s section=%s\n", ctype, stype);
            fprintf(stderr, "Error loading resource service!  type=%s section=%s\n", ctype, stype);
            fflush(stderr);
            abort();
        }
        free(ctype);

        _lc_object_put(stype, lio->rs);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_RS, lio->rs);

    stype = tbx_inip_get_string(lio->ifd, section, "os", "osfile");
    check_for_section(lio->ifd, stype, "No Object Service (os) found in LIO config!\n");
    lio->os_section = stype;
    lio->os = _lc_object_get(stype);
    if (lio->os == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", OS_TYPE_FILE);
        os_create = lio_lookup_service(lio->ess, OS_AVAILABLE, ctype);
        lio->os = (*os_create)(lio->ess, lio->ifd, stype);
        if (lio->os == NULL) {
            log_printf(1, "Error loading object service!  type=%s section=%s\n", ctype, stype);
            fprintf(stderr, "Error loading object service!  type=%s section=%s\n", ctype, stype);
            fflush(stderr);
            abort();
        }
        free(ctype);

        _lc_object_put(stype, lio->os);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_OS, lio->os);

    cred_args[0] = lio->ifd;
    cred_args[1] = (user == NULL) ? tbx_inip_get_string(lio->ifd, section, "user", "guest") : strdup(user);
    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", (char *)cred_args[1], lio->obj_name);
    stype = buffer;
    lio->creds_name = strdup(buffer);
    tuple = _lc_object_get(stype);
    if (tuple == NULL) {  //** Need to load it
        lio->creds = os_cred_init(lio->os, OS_CREDS_INI_TYPE, (void **)cred_args);
        tbx_type_malloc_clear(tuple, lio_path_tuple_t, 1);
        tuple->creds = lio->creds;
        tuple->lc = lio;
        _lc_object_put(stype, tuple);  //** Add it to the table
    } else {
        lio->creds = tuple->creds;
    }
    if (cred_args[1] != NULL) free(cred_args[1]);

    if (_lio_cache == NULL) {
        stype = tbx_inip_get_string(lio->ifd, section, "cache", CACHE_TYPE_AMP);
        check_for_section(lio->ifd, stype, "No Cache section found in LIO config!\n");
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", CACHE_TYPE_AMP);
        cache_create = lio_lookup_service(lio->ess, CACHE_LOAD_AVAILABLE, ctype);
        _lio_cache = (*cache_create)(lio->ess, lio->ifd, stype, lio->da, lio->timeout);
        if (_lio_cache == NULL) {
            log_printf(0, "Error loading cache service!  type=%s\n", ctype);
            fprintf(stderr, "Error loading cache service!  type=%s\n", ctype);
            fflush(stderr);
            abort();
        }
        free(stype);
        free(ctype);
    }

    //** This is just used for creating empty exnodes or dup them.
    //** Since it's missing the cache it doesn't get added to the global cache list
    //** and accidentally generate collisions.  Especially useful for mkdir to copy the parent exnode
    lio->ess_nocache = clone_service_manager(lio->ess);

    lio->cache = _lio_cache;
    add_service(lio->ess, ESS_RUNNING, ESS_CACHE, lio->cache);

    //** Table of open files
    lio->open_index = tbx_sl_new_full(10, 0.5, 0, &ex_id_compare, NULL, NULL, NULL);

    assert_result(apr_pool_create(&(lio->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(lio->lock), APR_THREAD_MUTEX_DEFAULT, lio->mpool);

    //** Add ourselves to the info handler
    tbx_siginfo_handler_add(lio_open_files_info_fn, lio);

    log_printf(1, "END: uri=%s\n", obj_name);

    return(lio);
}


//***************************************************************
// lio_create - Creates a lio configuration according to the config file
//***************************************************************

lio_config_t *lio_create(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name)
{
    lio_config_t *lc;

    apr_thread_mutex_lock(_lc_lock);
    lc = lio_create_nl(ifd, section, user, obj_name, exe_name);
    apr_thread_mutex_unlock(_lc_lock);

    return(lc);
}

//***************************************************************
// lio_print_path_options - Prints the path options to the device
//***************************************************************

void lio_print_path_options(FILE *fd)
{
    fprintf(fd, "    LIO_PATH_OPTIONS: [-rp regex_path | -gp glob_path]  [-ro regex_objext | -go glob_object] [path_1 ... path_N]\n");
    fprintf(fd, "       -rp regex_path  - Regex of path to scan\n");
    fprintf(fd, "       -gp glob_path   - Glob of path to scan\n");
    fprintf(fd, "       -ro regex_obj   - Regex for final object selection.\n");
    fprintf(fd, "       -go glob_obj    - Glob for final object selection.\n");
    fprintf(fd, "       path1 .. pathN  - Glob of paths to target\n");
    fprintf(fd, "\n");
}

//***************************************************************
// lio_parse_path_options - Parses the path options
//***************************************************************

int lio_parse_path_options(int *argc, char **argv, int auto_mode, lio_path_tuple_t *tuple, lio_os_regex_table_t **rp, lio_os_regex_table_t **ro)
{
    int nargs, i;

    *rp = NULL;
    *ro = NULL;

    if (*argc == 1) return(0);

    nargs = 1;  //** argv[0] is preserved as the calling name

    i=1;
    do {
        if (strcmp(argv[i], "-rp") == 0) { //** Regex for path
            i++;
            *tuple = lio_path_resolve(auto_mode, argv[i]);  //** Pick off the user/host
            *rp = lio_os_regex2table(tuple->path);
            i++;
        } else if (strcmp(argv[i], "-ro") == 0) {  //** Regex for object
            i++;
            *ro = lio_os_regex2table(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-gp") == 0) {  //** Glob for path
            i++;
            *tuple = lio_path_resolve(auto_mode, argv[i]);  //** Pick off the user/host
            *rp = lio_os_path_glob2regex(tuple->path);
            i++;
        } else if (strcmp(argv[i], "-go") == 0) {  //** Glob for object
            i++;
            *ro = lio_os_path_glob2regex(argv[i]);
            i++;
        } else {
            if (i!=nargs)argv[nargs] = argv[i];
            nargs++;
            i++;
        }
    } while (i<*argc);

    if (*argc == nargs) return(0);  //** Nothing was processed

    //** Adjust argv to reflect the parsed arguments
    *argc = nargs;

    return(1);
}

//***************************************************************
// env2args - Converts an env string to argc/argv
//***************************************************************

int env2args(char *env, int *argc, char ***eargv)
{
    int i, n, fin;
    char *bstate, **argv;

    n = 100;
    tbx_type_malloc_clear(argv, char *, n);

    i = 0;
    argv[i] = tbx_stk_string_token(env, " ", &bstate, &fin);
    while (fin == 0) {
        i++;
        if (i==n) {
            n += 10;
            tbx_type_realloc(argv, char *, n);
        }
        argv[i] = tbx_stk_string_token(NULL, " ", &bstate, &fin);
    }

    *argc = i;
    if (i == 0) {
        *argv = NULL;
    } else {
        tbx_type_realloc(argv, char *, i);
    }

    *eargv = argv;
    return(0);
}

//***************************************************************
// lio_init - Initializes LIO for use.  argc and argv are
//    modified by removing LIO common options.
//
//   NOTE: This should be called AFTER any fork() calls because
//         it spawns new threads!
//***************************************************************

//char **t2 = NULL;
int lio_init(int *argc, char ***argvp)
{
    int i, j, k, ll, ll_override, neargs, nargs, auto_mode, ifll, if_mode, print_config;
    FILE *fd;
    tbx_inip_file_t *ifd;
    tbx_stack_t *hints;
    char *name, *info_fname;
    char var[4096];
    char *dummy = NULL;
    char *env;
    char **eargv;
    char **argv;
    char *out_override = NULL;
    char *cfg_name = NULL;
    char *config = NULL;
    char *remote_config = NULL;
    char *section_name = "lio";
    char *userid = NULL;
    char *home;
    char *obj_name;
    char buffer[4096];
    char text[4096];

    if(NULL != lio_gc && lio_gc->ref_cnt > 0) {
        // lio_gc is a global singleton, if it is already initialized don't initialize again. (Note this implementation is not entirely immune to race conditions)
        lio_gc->ref_cnt++;
        return 0;
    }

    argv = *argvp;

    //** Setup the info signal handler.  We'll reset the name after we've got a lio_gc
    tbx_siginfo_install(NULL, SIGUSR1);

    gop_init_opque_system();  //** Initialize GOP.  This needs to be done after any fork() calls
    exnode_system_init();

    tbx_set_log_level(-1);  //** Disables log output

    //** Create the lio object container
    apr_pool_create(&_lc_mpool, NULL);
    apr_thread_mutex_create(&_lc_lock, APR_THREAD_MUTEX_DEFAULT, _lc_mpool);
    _lc_object_list = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);

    //** Grab the exe name
    //** Determine the preferred environment variable based on the calling name to use for the args
    lio_os_path_split(argv[0], &dummy, &name);
    _lio_exe_name = name;
    if (dummy != NULL) free(dummy);
    j = strncmp(name, "lio_", 4) == 0 ? 4 : 0;
    i = 0;
    memcpy(var, "LIO_OPTIONS_", 12);
    k = 12;
    while ((name[j+i] != 0) && (i<3900)) {
        // FIXME: Bad for UTF-8
        var[k+i] = toupper(name[j+i]);
        i++;
    }
    var[k+i] = 0;

    env = getenv(var); //** Get the exe based options if available
    if (env == NULL) env = getenv("LIO_OPTIONS");  //** If not get the global default

    if (env != NULL) {  //** Got args so prepend them to the front of the list
        env = strdup(env);  //** Don't want to mess up the actual env variable
        eargv = NULL;
        env2args(env, &neargs, &eargv);

        if (neargs > 0) {
            ll = *argc + neargs;
            tbx_type_malloc_clear(myargv, char *, ll);
            myargv[0] = argv[0];
            memcpy(&(myargv[1]), eargv, sizeof(char *)*neargs);
            if (*argc > 1) memcpy(&(myargv[neargs+1]), &(argv[1]), sizeof(char *)*(*argc - 1));
            argv = myargv;
            *argvp = myargv;
            *argc = ll;
            free(eargv);

        }
    }

    tbx_type_malloc_clear(myargv, char *, *argc);

    //** Parse any arguments
    nargs = 1;  //** argv[0] is preserved as the calling name
    myargv[0] = argv[0];
    i=1;
    print_config = 0;
    ll_override = -100;
    ifll = 0;
    if_mode = INFO_HEADER_NONE;
    info_fname = NULL;
    auto_mode = -1;

    //** Load the hints if we have any
    hints = tbx_stack_new();
    tbx_inip_hint_options_parse(hints, argv, argc);

    if (*argc < 2) goto no_args;  //** Nothing to parse

    do {
        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            ll_override = atoi(argv[i]);
            tbx_set_log_level(ll_override);
            i++;
        } else if (strcmp(argv[i], "-log") == 0) { //** Override log file output
            i++;
            out_override = argv[i];
            i++;
        } else if (strcmp(argv[i], "-no-auto-lfs") == 0) { //** Regex for path
            i++;
            auto_mode = 0;
        } else if (strcmp(argv[i], "-np") == 0) { //** Parallel task count
            i++;
            lio_parallel_task_count = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0) { //** Info level w/o header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_NONE;
        } else if (strcmp(argv[i], "-it") == 0) { //** Info level w thread header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_THREAD;
        } else if (strcmp(argv[i], "-if") == 0) { //** Info level w full header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_FULL;
        } else if (strcmp(argv[i], "-ilog") == 0) { //** Override Info log file output
            i++;
            info_fname = argv[i];
            i++;
        } else if (strcmp(argv[i], "-c") == 0) { //** Load a config file from either a remote or local source
            i++;
            cfg_name = argv[i];
            i++;
        } else if (strcmp(argv[i], "-lc") == 0) { //** Default LIO config section
            i++;
            section_name = argv[i];
            i++;
        } else if (strcmp(argv[i], "--print-config") == 0) { //** Default LIO config section
            i++;
            print_config = 1;
        } else {
            myargv[nargs] = argv[i];
            nargs++;
            i++;
        }
    } while (i<*argc);

no_args:

    //** Make the info logging device
    if (info_fname != NULL) { //** User didn't specify anything
        if (strcmp(info_fname, "stdout") == 0) {
            fd = stdout;
        } else if (strcmp(info_fname, "stderr") == 0) {
            fd = stderr;
        } else {
            fd = fopen(info_fname, "w+");
        }
        if (fd != NULL) _lio_ifd = fd;
    }

    if (_lio_ifd == NULL) _lio_ifd = stdout;

    lio_ifd = tbx_info_create(_lio_ifd, if_mode, ifll);

    //** Adjust argv to reflect the parsed arguments
    *argvp = myargv;
    *argc = nargs;

    if (!cfg_name) { //** Nothing explicitly passed in so try and use a default
        if (lio_os_local_filetype("default") != 0) {  //** Local remote config
            cfg_name = "file://default";
        } else if (lio_os_local_filetype("lio.cfg") != 0) {  //** Local INI file
            cfg_name = "ini://lio.cfg";
        } else {
            home = getenv("HOME");
            snprintf(var, sizeof(var), "%s/.lio/default", home);
            snprintf(buffer, sizeof(buffer), "%s/.lio/lio.cfg", home);
            if (lio_os_local_filetype(var) != 0) {  //* $HOME default
                snprintf(buffer, sizeof(buffer), "file://%s", var);
                cfg_name = buffer;
            } else if (lio_os_local_filetype(var) != 0) {  //* $HOME INI file
                snprintf(var, sizeof(var), "file://%s", buffer);
                cfg_name = var;
            } else if (lio_os_local_filetype("/etc/lio/default") != 0) {
                cfg_name = "file:///etc/lio/default";
            } else if (lio_os_local_filetype("/etc/lio/lio.cfg") != 0) {
                cfg_name = "file:///etc/lio/lio.cfg";
            }
        }

        if (!cfg_name) {
            printf("Missing config file!\n");
            exit(1);
        }
    }


    if (strncasecmp(cfg_name, "file://", 7) == 0) { //** Load the default and see what we have
        fd = fopen(cfg_name+7, "r");
        if (!fd) {
            printf("Failed to open config file: %s  errno=%d\n", cfg_name+8, errno);
            exit(1);
        }
        if (!fgets(text, sizeof(text), fd)) {
            printf("No data in config file: %s  errno=%d\n", cfg_name, errno);
            exit(1);
        }
        fclose(fd);

        i = strlen(text);
        if (i == 0) {
            printf("No data in config file: %s\n", cfg_name);
            exit(1);
        }
        if (text[i-1] == '\n') text[i-1] = '\0';
        if (strncasecmp(text, "file://", 7) == 0) { //** Can't recursively load files
            printf("Config file must contain either a ini:// or lstore:// URI!\n");
            printf("Loaded from file %s\n", cfg_name);
            printf("Text: %s\n", text);
            exit(1);
        }
        cfg_name = text;
    }

    //** See what we load for default
    if ((strncasecmp(cfg_name, "ini://", 6) == 0) || (cfg_name[0] == '/')) { //** It's a local file to load
        ifd = (cfg_name[0] == '/') ? tbx_inip_file_read(cfg_name) : tbx_inip_file_read(cfg_name+6);
        i = 9 + 2 + 1 + 6 + 1 + 6 + 6 + 1 + sizeof(section_name) + 20;
        tbx_type_malloc(obj_name, char, i);
        dummy = NULL;
        snprintf(obj_name, i, "lstore://%s|%s:%d:%s:%s", "RC", dummy, 6711, "LOCAL", section_name);
    } else {            //** Try and load a remote config
        if (rc_client_get_config(cfg_name, &config, &obj_name) == 0) {
            ifd = tbx_inip_string_read(config);
            free(config);
        } else {
            printf("Failed loading config: %s\n", cfg_name);
            exit(1);
        }
    }

    if (ifd == NULL) {
        printf("Failed to parse INI file! config=%s\n", cfg_name);
        exit(1);
    }

    tbx_inip_hint_list_apply(ifd, hints);  //** Apply the hints
    tbx_inip_hint_list_destroy(hints);     //** and cleanup

    tbx_mlog_load(ifd, out_override, ll_override);
    lio_gc = lio_create(ifd, section_name, userid, obj_name, name);
    if (!lio_gc) {
        log_printf(-1, "Failed to create lio context.\n");
        return 1;
    }

    if (print_config) {
        char *cfg_dump = tbx_inip_serialize(lio_gc->ifd);
        info_printf(lio_ifd, 0, "-------------------Dumping LStore config-------------------\n");
        info_printf(lio_ifd, 0, "Config string: %s   Section: %s\n", cfg_name, section_name);
        info_printf(lio_ifd, 0, "-----------------------------------------------------------\n\n");
        info_printf(lio_ifd, 0, "%s", cfg_dump);
        info_printf(lio_ifd, 0, "-----------------------------------------------------------\n\n");
        if (cfg_dump) free(cfg_dump);
    }


    lio_gc->ref_cnt = 1;

    if (obj_name) free(obj_name);

    if (auto_mode != -1) lio_gc->auto_translate = auto_mode;

    if (userid != NULL) free(userid);

    lio_find_lfs_mounts();  //** Make the global mount prefix table

    //** Get the Work Que started
    i = tbx_inip_get_integer(lio_gc->ifd, section_name, "wq_n", 5);
    lio_wq_startup(i);

    //** See if we run a remote config server
    remote_config = tbx_inip_get_string(lio_gc->ifd, section_name, "remote_config", NULL);
    if (remote_config) {
        rc_server_install(lio_gc, remote_config);
        free(remote_config);
    }

    //** Install the signal handler hook to get info
    dummy = tbx_inip_get_string(lio_gc->ifd, section_name, "info_fname", "/tmp/lio_info.txt");
    tbx_siginfo_install(dummy, SIGUSR1);

    log_printf(1, "INIT completed\n");

    return(0);
}

//***************************************************************
//  lio_shutdown - Shuts down the LIO system
//***************************************************************

int lio_shutdown()
{
    log_printf(1, "SHUTDOWN started\n");

    lio_gc->ref_cnt--;
    if(NULL != lio_gc && lio_gc->ref_cnt > 0) {
        return 0;
    }

    rc_server_destroy();

    cache_destroy(_lio_cache);
    _lio_cache = NULL;

    lio_wq_shutdown();

    lio_destroy(lio_gc);
    lio_gc = NULL;  //** Reset the global to NULL so it's not accidentally reused.

    //lc_object_remove_unused(0);

    exnode_system_destroy();

    gop_shutdown();

    tbx_info_destroy(lio_ifd);
    lio_ifd = NULL;
    if (_lio_exe_name) free(_lio_exe_name);
    if (myargv != NULL) free(myargv);

    apr_thread_mutex_destroy(_lc_lock);
    apr_pool_destroy(_lc_mpool);
    _lc_mpool = NULL;
    _lc_lock  = NULL;

    tbx_siginfo_shutdown();

    return(0);
}

