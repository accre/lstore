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
// Simple file backed object storage service implementation
//***********************************************************************

#define _log_module_index 155

//** this makes sure the dirname/basename routines don't touch the path
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>

#include <libgen.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include "opque.h"
#include "ex3_system.h"
#include "object_service_abstract.h"
#include "list.h"
#include "random.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_file.h"

#define MAX_PATH 1024
#define SAFE_MIN_LEN 10
#define FILE_ATTR_PREFIX "##FILE_ATTRIBUTES##"
#define DIR_PERMS S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH

atomic_int_t _path_parse_count = 0;
apr_thread_mutex_t *_path_parse_lock = NULL;
apr_pool_t *_path_parse_pool = NULL;

typedef struct {
  int base_path_len;
  char *base_path;
  thread_pool_context_t *tpc;
} osfile_priv_t;

typedef struct {
  object_service_fn_t *os;
  char *object_name;
  char *attr_dir;
  int ftype;
} osfile_fd_t;

typedef struct {
  osfile_fd_t *fd;
  DIR *d;
  os_regex_table_t *regex;
  char *key;
  void *value;
  int v_max;
} osfile_attr_iter_t;

typedef struct {
  DIR *d;
  struct dirent *entry;
  char path[MAX_PATH];
  regex_t *preg;
  long prev_pos;
  long curr_pos;
  int firstpass;
} osf_obj_level_t;

typedef struct {
  object_service_fn_t *os;
  os_regex_table_t *table;
  os_regex_table_t *attr;
  os_regex_table_t *recurse_table;
  osf_obj_level_t *level_info;
  os_creds_t *creds;
  os_attr_iter_t *it_attr;
  os_fd_t *fd;
  int recurse_depth;
  int max_level;
  int v_max;
  int curr_level;
} osf_object_iter_t;

typedef struct {
  object_service_fn_t *os;
  os_creds_t *creds;
  os_regex_table_t *rpath;
  int recurse_depth;
} osfile_remove_regex_op_t;

op_generic_t *osfile_set_attr(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size);
os_attr_iter_t *osfile_create_attr_iter(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, os_regex_table_t *attr, int v_max);
void osfile_destroy_attr_iter(os_attr_iter_t *oit);
op_generic_t *osfile_open_object(object_service_fn_t *os, os_creds_t *creds, char *path, os_fd_t **pfd);
op_generic_t *osfile_close_object(object_service_fn_t *os, os_fd_t *fd); 
os_object_iter_t *osfile_create_object_iter(object_service_fn_t *os, os_creds_t *creds, os_regex_table_t *path, os_regex_table_t *attr, int recurse_depth, int v_max);
int osfile_next_object(os_object_iter_t *oit, char **fname, os_attr_iter_t **it_attr);
void osfile_destroy_object_iter(os_object_iter_t *it);


//***********************************************************************
// safe_remove - Does a simple check that the object to be removed 
//     is not "/".
//***********************************************************************

int safe_remove(object_service_fn_t *os, char *path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;

  if ((strlen(path) > SAFE_MIN_LEN) && (strncmp(osf->base_path, path, osf->base_path_len) == 0)) {
     return(remove(path));
  }

  return(-1);
}

//***********************************************************************
//  path_split - Splits the path into a basename and dirname
//***********************************************************************

void path_split(char *path, char **dir, char **file)
{
  apr_thread_mutex_lock(_path_parse_lock);
  *dir = strdup(dirname(path));
  *file = strdup(basename(path));
  apr_thread_mutex_unlock(_path_parse_lock);
}

//***********************************************************************
// filetype - Determines the file type
//***********************************************************************

int filetype(char *path)
{
  struct stat s;
  int err;

  err = stat(path, &s);
  if (err == 0) {
    if (S_ISREG(s.st_mode)) {
       err = OS_OBJECT_FILE;
    } else if (S_ISDIR(s.st_mode)) {
       err = OS_OBJECT_DIR;
    } else if (S_ISLNK(s.st_mode)) {
       err = OS_OBJECT_LINK;
    } else {
       err = -2;
    }
  }

  return(err);    
}

//***********************************************************************
// osf_is_dir_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_dir_empty(char *path)
{
  DIR *d;
  struct dirent *entry;

  int empty = 1;

  d = opendir(path);
  if (d == NULL) return(1);

  while ((empty == 1) && ((entry = readdir(d)) != NULL)) {
     if (strcmp(entry->d_name, FILE_ATTR_PREFIX) != 0) empty = 0;
  }

  closedir(d);
  
  return(empty);
}
//***********************************************************************
// osf_is_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_empty(char *path)
{
  int ftype;
 
  ftype = filetype(path);
  if (ftype == OS_OBJECT_FILE) {  //** Simple file
    return(1);
  } else if (ftype == OS_OBJECT_DIR) { //** Directory
    return(osf_is_dir_empty(path));
  }

  return(0);
}

//***********************************************************************
// osf_next_object - Returns the iterators next object
//***********************************************************************

char *osf_next_object(osf_object_iter_t *it)
{
  int i;
  osf_obj_level_t *itl;
  char fname[MAX_PATH];

  do {
    itl = &(it->level_info[it->curr_level]);
    while ((itl->entry = readdir(itl->d)) != NULL) {
       itl->prev_pos = itl->curr_pos;
       itl->curr_pos = telldir(itl->d);
       i = regexec(itl->preg, itl->entry->d_name, 0, NULL, 0);
       if (i == 0) { //** Got a match
          if (it->curr_level < it->max_level) {
             if (it->curr_level >= it->table->n-1) { //** From here on all hits are matches
                snprintf(fname, MAX_PATH, "%s/%s", itl->path, itl->entry->d_name);
                i = filetype(fname);
                if (i == OS_OBJECT_FILE) {
                   return(strdup(fname));
                } else {
                   if ((itl->firstpass == 1) && (it->curr_level < it->max_level-1)) { //** 1st time through so recurse
                      itl->firstpass = 0;
                      seekdir(itl->d, itl->prev_pos);  //** Move the dirp back one slot
                      it->curr_level++;
                      itl = &(it->level_info[it->curr_level]);
                      strncpy(itl->path, fname, MAX_PATH);
                      itl->d = opendir(itl->path);
                      itl->curr_pos = telldir(itl->d);
                   } else {  //Already been here so just return the path
                      itl->firstpass = 1;
                      return(strdup(fname));
                   }
                }
             } else {
                snprintf(fname, MAX_PATH, "%s/%s", itl->path, itl->entry->d_name);
                it->curr_level++;
                itl = &(it->level_info[it->curr_level]);
                strncpy(itl->path, fname, MAX_PATH);
                itl->d = opendir(itl->path);
                itl->curr_pos = telldir(itl->d);
             }
          }
       }
    }

    closedir(itl->d);
    itl->d = NULL;
    it->curr_level--;
  } while (it->curr_level > 0);

  return(NULL);
}

//***********************************************************************
// osf_object_iter - Generic object iterator
//***********************************************************************

osf_object_iter_t *DUMMY_osf_object_iter(object_service_fn_t *os, os_regex_table_t *path, int recurse_depth)
{
  osf_object_iter_t *it;
  osf_obj_level_t *itl;
  int i;

  type_malloc_clear(it, osf_object_iter_t, 1);

  it->os = os;
  it->table = path;
  it->recurse_depth = recurse_depth;
  it->max_level = path->n + recurse_depth;

  if (recurse_depth > 0) {
    it->recurse_table = os_path_glob2regex("*");
  }

  type_malloc_clear(it->level_info, osf_obj_level_t, it->max_level);
  for (i=0; i<it->table->n; i++) {
    itl = &(it->level_info[i]);
    itl->preg = &(path->regex_entry[i].compiled);
  }

  for (i=it->table->n; i<it->max_level; i++) {
    itl = &(it->level_info[i]);
    itl->preg = &(it->recurse_table->regex_entry[0].compiled);
    itl->firstpass = 1;
  }

  return(it);
}

//***********************************************************************
// osf_object_iter_destroy - Destroys a generic object iterator
//***********************************************************************

void DUMMY_osf_object_iter_destroy(osf_object_iter_t *it)
{
  int i;

  //** Close any open directories
  for (i=0; i<it->max_level; i++) {
    if (it->level_info[i].d != NULL) closedir(it->level_info[i].d);
  }
  
  //** Free the recurse table if needed
  if (it->recurse_depth > 0) os_regex_table_destroy(it->recurse_table);

  free(it->level_info);
  free(it);
}

//***********************************************************************
// osf_purge_dir - Removes all files from the path and will recursively
//     purge sudirs based o nteh recursion depth
//***********************************************************************

int osf_purge_dir(object_service_fn_t *os, char *path, int depth)
{
  int ftype;
  char fname[MAX_PATH];
  DIR *d;
  struct dirent *entry;

  d = opendir(path);
  if (d == NULL) return(1);

  while ((entry = readdir(d)) != NULL) {
     snprintf(fname, MAX_PATH, "%s/%s", path, entry->d_name);
     ftype = filetype(fname);
     if (ftype == OS_OBJECT_FILE) {
        safe_remove(os, fname);
     } else if (ftype == OS_OBJECT_DIR) {
       if (depth > 0) {
          osf_purge_dir(os, fname, depth-1);
          safe_remove(os, fname);
       }
     }
  }

  closedir(d);

  return(0);
}

//***********************************************************************
// osf_object_remove - Removes the current dir or object (non-recursive)
//***********************************************************************

int osf_object_remove(object_service_fn_t *os, char *path)
{
  int ftype;
  char *dir, *base;
  char fattr[MAX_PATH];

  ftype = filetype(path);

  if (ftype == OS_OBJECT_FILE) { //** It's a file
     remove(path);  //** Remove the file
     path_split(path, &dir, &base);
     snprintf(fattr, MAX_PATH, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     osf_purge_dir(os, fattr, 0);
     remove(fattr);
     free(dir); free(base);     
  } else if (ftype == OS_OBJECT_DIR) {  //** A directory
     osf_purge_dir(os, path, 0);  //** Removes all the files
     snprintf(fattr, MAX_PATH, "%s/%s", path,  FILE_ATTR_PREFIX);
     osf_purge_dir(os, fattr, 1);
     safe_remove(os, fattr);
     safe_remove(os, path);
  }

  return(0);

}

//***********************************************************************
// osfile_create_object - Creates an object
//***********************************************************************

op_generic_t *osfile_remove_object(object_service_fn_t *os, os_creds_t *c, char *path)
{
  osfile_priv_t *osf = (osfile_priv_t *)os;
  int ftype;
  char fname[MAX_PATH];
  op_generic_t *gop;

  snprintf(fname, MAX_PATH, "%s/%s", osf->base_path, path);
  ftype = filetype(fname);
  if (ftype == OS_OBJECT_FILE) {  //** Regular file so rm the attributes dir and the object
     osf_object_remove(os, fname);
     gop = gop_dummy(op_success_status);     
  } else {  //** Directory so make sure it's empty
    if (osf_is_empty(path) != 1) {
       return(gop_dummy(op_failure_status));
    }

    //** The directory is empty so can safely remove it
    osf_object_remove(os, fname);
    gop = gop_dummy(op_success_status);     
  }

  return(gop);
}

//***********************************************************************
// osfile_remove_regex_fn - Does the actual bulk object removal
//***********************************************************************

op_status_t osfile_remove_regex_fn(void *arg, int id)
{
  osfile_remove_regex_op_t *op = (osfile_remove_regex_op_t *)arg;
  os_object_iter_t *it;
  char *fname;
  int ftype;

  it = osfile_create_object_iter(op->os, op->creds, op->rpath, NULL, 0, 0);

  while (osfile_next_object(it, &fname, NULL) == 0) {
     ftype = filetype(fname);
     if (ftype == OS_OBJECT_FILE) { //** Simple file removal
        safe_remove(op->os, fname);
     } else {  //** It's a directory
        osf_purge_dir(op->os, fname, op->recurse_depth);
     }
  }

  osfile_destroy_object_iter(it);

  return(op_success_status);
}

//***********************************************************************
// osfile_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************


op_generic_t *osfile_remove_regex_object(object_service_fn_t *os, os_creds_t *creds, os_regex_table_t *path, int recurse_depth)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;
  osfile_remove_regex_op_t *op;

  type_malloc(op, osfile_remove_regex_op_t, 1);

  op->os = os;
  op->creds = creds;
  op->rpath = path;
  op->recurse_depth = recurse_depth;

  return(new_thread_pool_op(osf->tpc, NULL, osfile_remove_regex_fn, (void *)op, free, 1));  
}


//***********************************************************************
// osfile_create_object - Creates an object
//***********************************************************************

op_generic_t *osfile_create_object(object_service_fn_t *os, os_creds_t *c, char *path, int type)
{
  osfile_priv_t *osf = (osfile_priv_t *)os;
  FILE *fd;
  int err;
  char *dir, *base;
  char fname[MAX_PATH];
  char fattr[MAX_PATH];

  if (type == OS_OBJECT_FILE) {
     snprintf(fname, MAX_PATH, "%s/%s", osf->base_path, path);
     fd = fopen(fname, "w");
     if (fd == NULL) return(gop_dummy(op_failure_status));

     fprintf(fd, "user=%s", (char *)c);
     fclose(fd);

     //** Also need to make the attributes entry  
     path_split(fattr, &dir, &base);
     snprintf(fattr, MAX_PATH, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     err = mkdir(fattr, DIR_PERMS);
     if (err != 0) {
        log_printf(0, "osfiel_create_object: Error creating object attr directory! path=%s full=%s\n", path, fattr);
        safe_remove(os, fname);
        free(dir); free(base);
        return(gop_dummy(op_failure_status));
     } else {
        free(dir); free(base);
     }
  } else {  //** Directory object
     snprintf(fname, MAX_PATH, "%s/%s", osf->base_path, path);
     err = mkdir(fname, DIR_PERMS);
     if (err != 0) return(gop_dummy(op_failure_status));

     //** Also need to make the attributes entry 
     snprintf(fattr, MAX_PATH, "%s/%s", fname, FILE_ATTR_PREFIX);
     err = mkdir(fattr, DIR_PERMS);
     if (err != 0) {
        log_printf(0, "osfile_create_object: Error creating object attr directory! path=%s full=%s\n", path, fattr);
        safe_remove(os, fname);
        return(gop_dummy(op_failure_status));
     }
     
  }

  //** Lastly store the object type
  sprintf(fname, "%d", type);
  osfile_set_attr(os, path, c, "type", fname, strlen(fname));

  return(gop_dummy(op_success_status));
}

//***********************************************************************
// osfile_move_object - Moves an object
//***********************************************************************

op_generic_t *osfile_move_object(object_service_fn_t *os, os_creds_t *creds, char *src_path, char *dest_path)
{
  int ftype;
  char sfname[MAX_PATH];
  char dfname[MAX_PATH];
  char *dir, *base;
  int err;

  ftype = filetype(src_path);

  err = rename(src_path, dest_path);  //** Move the file/dir

  if ((ftype == 0) && (err==0)) { //** File move 
     //** Also need to move the attributes entry  
     path_split(src_path, &dir, &base);
     snprintf(sfname, MAX_PATH, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     free(dir); free(base);
     path_split(dest_path, &dir, &base);
     snprintf(dfname, MAX_PATH, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     free(dir); free(base);

     err = rename(sfname, dfname);
  } 

  return(gop_dummy((err == 0) ? op_success_status : op_failure_status));
}

//***********************************************************************
// osf_get_attr - Gets the attribute given the name and base directory
//***********************************************************************

int osf_get_attr(object_service_fn_t *os, char *attr_dir, char *attr, void **val, int *v_size)
{
  FILE *fd;
  char fname[MAX_PATH];

  snprintf(fname, MAX_PATH, "%s/%s", attr_dir, attr);
  fd = fopen(fname, "r");
  if (fd == NULL) return(-1);

  if (*v_size < 0) { //** Need to determine the size
     fseek(fd, 0L, SEEK_END);
     *v_size = ftell(fd);
     fseek(fd, 0L, SEEK_SET);
     *val = malloc(*v_size);
  }

  *v_size = fread(*val, *v_size, 1, fd);
  fclose(fd);

  return(0);
}

//***********************************************************************
// osfile_get_attr - Retreives a single object attribute
//   If *v_size == -1 then space is allocated and upon return *v_size
//   contains the bytes loaded
//***********************************************************************

op_generic_t *osfile_get_attr(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  int err;
  
  err = osf_get_attr(os, fd->attr_dir, key, val, v_size);

  return(gop_dummy((err==0) ? op_success_status : op_failure_status)); 
}

//***********************************************************************
// osfile_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size == -1 for the attribute then space is allocated. Upon
//   return *v_size contains the bytes stored
//***********************************************************************

op_generic_t *osfile_get_multiple_attrs(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  int err, i;

  err = 0;
  for (i=0; i<n; i++) {
    err =+ osf_get_attr(fd->os, fd->attr_dir, key[i], &(val[i]), &(v_size[i]));
  }

  return(gop_dummy((err==0) ? op_success_status : op_failure_status)); 
}

//***********************************************************************
// osf_set_attr - Sets the attribute given the name and base directory
//***********************************************************************

int osf_set_attr(object_service_fn_t *os, char *attr_dir, char *attr, void *val, int v_size)
{
  FILE *fd;
  char fname[MAX_PATH];

  snprintf(fname, MAX_PATH, "%s/%s", attr_dir, attr);
  if (val == NULL) { //** Want to remove the attribute
     safe_remove(os, fname);
  } else {
     fd = fopen(fname, "w");
     if (fd == NULL) return(-1);
     fwrite(val, v_size, 1, fd);
     fclose(fd);
  }
  return(0);
}

//***********************************************************************
// osfile_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

op_generic_t *osfile_set_attr(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, char *key, void *val, int v_size)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  int err;

  err = osf_set_attr(fd->os, fd->attr_dir, key, val, v_size);

  return(gop_dummy((err==0) ? op_success_status : op_failure_status)); 
}

//***********************************************************************
// osfile_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

op_generic_t *osfile_set_multiple_attrs(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  int err, i;

  err = 0;
  for (i=0; i<n; i++) {
    err =+ osf_set_attr(fd->os, fd->attr_dir, key[i], val[i], v_size[i]);
  }

  return(gop_dummy((err==0) ? op_success_status : op_failure_status)); 
}

//***********************************************************************
// osfile_next_attr - Returns the next matching attribute
//***********************************************************************

int osfile_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
  osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
  int i, n;
  struct dirent *entry;
  os_regex_table_t *rex = it->regex;

  if (it->key != NULL) { free(it->key); it->key = NULL; }
  if (it->value != NULL) { free(it->value); it->value = NULL; }

  while ((entry = readdir(it->d)) != NULL) {
     for (i=0; i<rex->n; i++) {
       n = regexec(&(rex->regex_entry[i].compiled), entry->d_name, 0, NULL, 0);
       if (n == 0) { //** got a match
          osf_get_attr(it->fd->os, it->fd->attr_dir, entry->d_name, val, v_size);
          *key = strdup(entry->d_name);
          return(0);
       }
     }      
  }

  return(-1);
}

//***********************************************************************
// osfile_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//***********************************************************************

os_attr_iter_t *osfile_create_attr_iter(object_service_fn_t *os, os_creds_t *creds, os_fd_t *ofd, os_regex_table_t *attr, int v_max)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  osfile_attr_iter_t *it;

  type_malloc_clear(it, osfile_attr_iter_t, 1);

  it->d = opendir(fd->attr_dir);
  it->regex = attr;
  it->fd = fd;
  
  return((os_attr_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_attr_iter - Destroys an attribute iterator
//***********************************************************************

void osfile_destroy_attr_iter(os_attr_iter_t *oit)
{
  osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
  if (it->d != NULL) closedir(it->d);

  if (it->key != NULL) { free(it->key); }
  if (it->value != NULL) { free(it->value); }

  free(it);
}

//***********************************************************************
// osfile_next_object - Returns the iterators next matching object
//***********************************************************************

int osfile_next_object(os_object_iter_t *oit, char **fname, os_attr_iter_t **it_attr)
{
  osf_object_iter_t *it = (osf_object_iter_t *)oit;

  *fname = osf_next_object(it);
  if (*fname != NULL) {
     if (it_attr != NULL) {
        if (it->it_attr != NULL) osfile_destroy_attr_iter(it->it_attr);
        if (it->fd != NULL) gop_free(osfile_close_object(it->os, it->fd), OP_DESTROY);

        gop_free(osfile_open_object(it->os, it->creds, *fname, &(it->fd)), OP_DESTROY);
        it->it_attr = osfile_create_attr_iter(it->os, it->creds, it->fd, it->attr, it->v_max);
     }

     return(0);
  }

  return(1);
}


//***********************************************************************
// osfile_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//***********************************************************************

os_object_iter_t *osfile_create_object_iter(object_service_fn_t *os, os_creds_t *creds, os_regex_table_t *path, os_regex_table_t *attr, int recurse_depth, int v_max)
{
  osf_object_iter_t *it;
  osf_obj_level_t *itl;
  int i;

  type_malloc_clear(it, osf_object_iter_t, 1);

  it->os = os;
  it->table = path;
  it->recurse_depth = recurse_depth;
  it->max_level = path->n + recurse_depth;
  it->creds = creds;
  it->v_max = v_max;
  it->attr = attr;

  if (recurse_depth > 0) {
    it->recurse_table = os_path_glob2regex("*");
  }

  type_malloc_clear(it->level_info, osf_obj_level_t, it->max_level);
  for (i=0; i<it->table->n; i++) {
    itl = &(it->level_info[i]);
    itl->preg = &(path->regex_entry[i].compiled);
  }

  for (i=it->table->n; i<it->max_level; i++) {
    itl = &(it->level_info[i]);
    itl->preg = &(it->recurse_table->regex_entry[0].compiled);
    itl->firstpass = 1;
  }

  return((os_object_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void osfile_destroy_object_iter(os_object_iter_t *oit)
{
  osf_object_iter_t *it = (osf_object_iter_t *)oit;

  int i;

  //** Close any open directories
  for (i=0; i<it->max_level; i++) {
    if (it->level_info[i].d != NULL) closedir(it->level_info[i].d);
  }
  
  //** Free the recurse table if needed
  if (it->recurse_depth > 0) os_regex_table_destroy(it->recurse_table);

  if (it->it_attr != NULL) osfile_destroy_attr_iter(it->it_attr);
  if (it->fd != NULL) osfile_close_object(it->os, it->fd);

  free(it->level_info);
  free(it);
}

//***********************************************************************
// osfile_open_object - Opens an object
//***********************************************************************

op_generic_t *osfile_open_object(object_service_fn_t *os, os_creds_t *creds, char *path, os_fd_t **pfd)
{
  osfile_fd_t *fd;
  int ftype;
  char *dir, *base;
  char fname[MAX_PATH];

  *pfd = NULL;
  ftype = filetype(path);
  if (ftype < 0) return(gop_dummy(op_failure_status));

  type_malloc(fd, osfile_fd_t, 1);
  
  fd->os = os;
  fd->ftype = ftype;
  fd->object_name = strdup(path);

  if (ftype == OS_OBJECT_FILE) {
     strncpy(fname, path, MAX_PATH);
     path_split(fname, &dir, &base);
     snprintf(fname, MAX_PATH, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
     fd->attr_dir = strdup(fname);
     free(dir); free(base);
  } else if (ftype == OS_OBJECT_DIR) {
     snprintf(fname, MAX_PATH, "%s/%s", path, FILE_ATTR_PREFIX);
     fd->attr_dir = strdup(fname);
  }

  *pfd = (os_fd_t *)fd;
  return(gop_dummy(op_success_status));
}

//***********************************************************************
// osfile_close_object - Closes an object
//***********************************************************************

op_generic_t *osfile_close_object(object_service_fn_t *os, os_fd_t *ofd)
{
  osfile_fd_t *fd = (osfile_fd_t *)ofd;
  if (fd == NULL) return(gop_dummy(op_success_status));

  free(fd->object_name);
  free(fd->attr_dir);
  free(fd);

  return(gop_dummy(op_success_status));
}

//***********************************************************************
// osfile_logout - Logout
//***********************************************************************

void osfile_logout(object_service_fn_t *os, os_creds_t *c)
{
  free(c);
}

//***********************************************************************
// osfile_login - Login
//***********************************************************************

os_creds_t *osfile_login(object_service_fn_t *os, char *userid, int type, void *arg)
{
  return((void *)strdup(userid));
}

//***********************************************************************
// osfile_destroy
//***********************************************************************

void osfile_destroy(object_service_fn_t *os)
{
  osfile_priv_t *osf = (osfile_priv_t *)os->priv;

  free(osf->base_path);
  free(osf);
  free(os);

  int i = atomic_dec(_path_parse_count);
  if (i <= 0) {
     apr_thread_mutex_destroy(_path_parse_lock);
     apr_pool_destroy(_path_parse_pool);
  }
}

//***********************************************************************
//  object_service_file_create - Creates a file backed OS
//***********************************************************************

object_service_fn_t *object_service_file_create(void *arg, char *fname)
{
  exnode_abstract_set_t *es = (exnode_abstract_set_t *)arg;
  inip_file_t *fd;
  object_service_fn_t *os;
  osfile_priv_t *osf;

  atomic_inc(_path_parse_count);
  if (_path_parse_pool == NULL) {
     apr_pool_create(&_path_parse_pool, NULL);
     apr_thread_mutex_create(&_path_parse_lock, APR_THREAD_MUTEX_DEFAULT, _path_parse_pool);
  }

  type_malloc(os, object_service_fn_t, 1); 
  type_malloc(osf, osfile_priv_t, 1);
  os->priv = (void *)osf;
  
  osf->tpc = es->tpc_cpu;

  osf->base_path = NULL;
  if (fname == NULL) {
      osf->base_path = strdup("./osfile");
  } else {
     fd = inip_read(fname);
     osf->base_path = inip_get_string(fd, "osfile","base_path", "./osfile");
     inip_destroy(fd);
  }

  osf->base_path_len = strlen(osf->base_path);

  os->type = OS_TYPE_FILE;

  os->destroy_service = osfile_destroy;
  os->login = osfile_login;
  os->logout = osfile_logout;
  os->create_object = osfile_create_object;
  os->remove_object = osfile_remove_object;
  os->remove_regex_object = osfile_remove_regex_object;
  os->move_object = osfile_move_object;
  os->link_object = NULL;
  os->create_object_iter = osfile_create_object_iter;
  os->next_object = osfile_next_object;
  os->destroy_object_iter = osfile_destroy_object_iter;
  os->open_object = osfile_open_object;  
  os->close_object = osfile_close_object;
  os->get_attr = osfile_get_attr; 
  os->set_attr = osfile_set_attr;
  os->get_multiple_attrs = osfile_get_multiple_attrs;
  os->set_multiple_attrs = osfile_set_multiple_attrs;
  os->create_attr_iter = osfile_create_attr_iter;
  os->next_attr = osfile_next_attr;
  os->destroy_attr_iter = osfile_destroy_attr_iter;

  return(os);
}

