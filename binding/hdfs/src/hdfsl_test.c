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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <apr_pools.h>
#include <apr_hash.h>
#include <lio/os.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include "hdfs_lstore.h"

const int bufsize = 10*1024*1024;
char *random_buffer;

#define DE_DIR  0
#define DE_FILE 1

//*************************************************************************
//** My random routines for reproducible runs
//*************************************************************************

void my_random_seed(unsigned int seed)
{
    srandom(seed);
}

//*************************************************************************

int my_get_random(void *vbuf, int nbytes)
{
    char *buf = (char *)vbuf;
    int i;
    unsigned short int v;
    unsigned short int *p;
    int ncalls = nbytes / sizeof(v);
    int nrem = nbytes % sizeof(v);

    for (i=0; i<ncalls; i++) {
        p = (unsigned short int *)&(buf[i*sizeof(v)]);
        *p = random();
    }

    if (nrem > 0) {
        v = random();
        memcpy(&(buf[ncalls*sizeof(v)]), &v, nrem);
    }

    return(0);
}

//*************************************************************************
// compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

int compare_buffers_print(char *b1, char *b2, int len, int offset)
{
    int i, mode, last, ok;
    int start, end, k;

    i = memcmp(b1, b2, len);
    if (i == 0) return(0);

    mode = (b1[0] == b2[0]) ? 0 : 1;
    start = offset;
    last = len - 1;

    fprintf(stdout, "Printing comparision breakdown -- Single byte matches are suppressed (len=%d)\n", len);
    for (i=0; i<len; i++) {
        if (mode == 0) {  //** Matching range
            if ((b1[i] != b2[i]) || (last == i)) {
                end = offset + i-1;
                k = end - start + 1;
                fprintf(stdout, "   MATCH : %d -> %d (%d bytes)\n", start, end, k);

                start = offset + i;
                mode = 1;
            }
        } else {
            if ((b1[i] == b2[i]) || (last == i)) {
                ok = 0;  //** Suppress single byte matches
                if (last != i) {
                    if (b1[i+1] == b2[i+1]) ok = 1;
                }
                if ((ok == 1) || (last == i)) {
                    end = offset + i-1;
                    k = end - start + 1;
                    fprintf(stdout, "   DIFFER: %d -> %d (%d bytes)\n", start, end, k);

                    start = offset + i;
                    mode = 0;
                }
            }
        }
    }

    return(1);
}


//*************************************************************************************
// my_rw_tests - Run the R/W tests
//*************************************************************************************
int my_rw_tests(hdfs_lstore_t *ctx, char *fname)
{
    hdfsl_fd_t *fd;
    char *buffer;
    int err;

    err = 0;
    tbx_type_malloc_clear(buffer, char, bufsize);

    //**Create a file
    fd = lstore_open(ctx, fname, HDFSL_OPEN_WRITE);
    assert(fd != NULL);

    //**Dump some data to it
    err += lstore_write(fd, random_buffer, bufsize);
    assert(err == 0);

    //** Close it
    lstore_close(fd);

    //** Read it back
    fd = lstore_open(ctx, fname, HDFSL_OPEN_READ);
    assert(fd != NULL);
    err += lstore_read(fd, buffer, bufsize);
    assert(err == 0);
    err += compare_buffers_print(random_buffer, buffer, bufsize, 0);
    assert(err == 0);
    lstore_close(fd);

    //** Now open it in append mode and add some data
    fd = lstore_open(ctx, fname, HDFSL_OPEN_APPEND);
    assert(fd != NULL);
    err += lstore_write(fd, random_buffer, bufsize);
    assert(err == 0);
    lstore_close(fd);

    //**Read it back
    fd = lstore_open(ctx, fname, HDFSL_OPEN_READ);
    assert(fd != NULL);
    err += lstore_read(fd, buffer, bufsize);
    assert(err == 0);
    err += compare_buffers_print(random_buffer, buffer, bufsize, 0);
    assert(err == 0);
    memset(buffer, 0, bufsize);
    err += lstore_read(fd, buffer, bufsize);
    assert(err == 0);
    err += compare_buffers_print(random_buffer, buffer, bufsize, bufsize);
    assert(err == 0);
    lstore_close(fd);

    //** Lastly remove it
    err += lstore_delete(ctx, fname, 0);
    assert(err == 0);

    free(buffer);

    if (err == 0) {
        printf("PASSED R/W tests!\n");
    } else {
        printf("FAILED R/W tests!\n");
    }
    return(err);
}

//*************************************************************************************
// walk_find_match - Scans the table looking for the object
//*************************************************************************************

int walk_find_match(char **table, int n, char *key)
{
    int i;

    for (i=0; i<n; i++) {
        if (table[i]) {
            if (strcmp(table[i], key) == 0) return(i);
        }
    }

    return(-1);
}

//*************************************************************************************
// walk_check - Walks the actual
//*************************************************************************************

int walk_check(hdfs_lstore_t *ctx, char *prefix, char **table, int n)
{
    int used[n], i, j, k, plen;
    hdfsl_fstat_iter_t *it;
    hdfsl_fstat_t  fstat;

    plen = strlen(prefix);
    memset(used, 0, n * sizeof(int));

    it = lstore_fstat_iter(ctx, prefix, 100);
    while (lstore_fstat_iter_next(it, &fstat) == 0) {
        assert(strncmp(prefix, fstat.path, plen) == 0);
        i = walk_find_match(table, n, fstat.path+plen);
        assert(i != -1);
        used[i] = 1;
        free(fstat.path);
        if (fstat.user) free(fstat.user);
        if (fstat.group) free(fstat.group);
        if (fstat.symlink) free(fstat.symlink);

    }
    lstore_fstat_iter_destroy(it);

    j = k = 0;
    for (i=0; i<n; i++) {
        j += used[i];
        if (table[i]) k++;
    }
    assert(j == k);

    return(0);
}

//*************************************************************************************
// add_dir - Adds a directory to the hash and makes it in LStore
//*************************************************************************************

int add_dir(hdfs_lstore_t *ctx, char *path)
{
    int err;

    err = lstore_mkdir(ctx, path);
    return(err);
}

//*************************************************************************************
// add_file - Adds a file to the hash and makes it in LStore
//*************************************************************************************

int add_file(hdfs_lstore_t *ctx, char *path)
{
    hdfsl_fd_t *fd;
    int err;

    fd = lstore_open(ctx, path, HDFSL_OPEN_WRITE);
    assert(fd != NULL);

    //**Dump some data to it
    err = lstore_write(fd, random_buffer, bufsize);

    //** Close it
    lstore_close(fd);

    return(err);
}

//*************************************************************************************
// rename_file - Renames a file to the hash and makes it in LStore
//*************************************************************************************

int rename_file(hdfs_lstore_t *ctx, char *src_path, char *dest_path)
{
    int err;

    err = lstore_rename(ctx, src_path, dest_path);
    return(err);
}

//*************************************************************************************
// rename_dir - Renames a directory to the hash and makes it in LStore
//*************************************************************************************

int rename_dir(hdfs_lstore_t *ctx, char *src_path, char *dest_path)
{
    int err;

    err = lstore_rename(ctx, src_path, dest_path);
    return(err);
}

//*************************************************************************************
// delete_file - Delete a file to the hash and makes it in LStore
//*************************************************************************************

int delete_file(hdfs_lstore_t *ctx, char *path)
{
    int err;

    err = lstore_delete(ctx, path, 0);
    return(err);
}

//*************************************************************************************
// delete_dir - Deletes a directory.  Doesn't mess with the hash
//   This is not a generic routine.  It assumes if you don't want to recurse that
//   a sub-driectory exists and so LStore should return an error which we flag as Ok.
//   Otherwise the error 
//*************************************************************************************

int delete_dir(hdfs_lstore_t *ctx, char *path, int recurse)
{
    return(lstore_delete(ctx, path, (recurse == 1) ? 100 : 0));
}

//*************************************************************************************
// my_dir_tests - Run the directory tests
//*************************************************************************************
int my_dir_tests(hdfs_lstore_t *ctx, char *prefix)
{
    char *table[100];
    char path[4096];
    char path2[4096];
    int err, n;

    //** Make the base tree for storing the directory structure
    err = 0;

    //** Make the base directory
    n = 1;
    table[n-1] = "/d1";
    snprintf(path, sizeof(path), "%s/d1", prefix);
    err += add_dir(ctx, path);
    walk_check(ctx, prefix, table, n);

    //** Create a couple of files
    n = 2;
    table[n-1] = "/d1/f1";
    snprintf(path, sizeof(path), "%s/d1/f1", prefix);
    err += add_file(ctx, path);
    walk_check(ctx, prefix, table, n);
    n = 3;
    table[n-1] = "/d1/f2";
    snprintf(path, sizeof(path), "%s/d1/f2", prefix);
    err += add_file(ctx, path);
    walk_check(ctx, prefix, table, n);

    //** Force a mkdir -p directory
    n = 6;
    table[3] = "/d1/d2";
    table[4] = "/d1/d2/d3";
    table[5] = "/d1/d2/d3/d4";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4", prefix);
    err += add_dir(ctx, path);
    walk_check(ctx, prefix, table, n);

    //** add a few more files
    n = 7;
    table[n-1] = "/d1/d2/d3/d4/f41";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4/f41", prefix);
    err += add_file(ctx, path);
    walk_check(ctx, prefix, table, n);
    n = 8;
    table[n-1] = "/d1/d2/d3/d4/f42";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4/f42", prefix);
    err += add_file(ctx, path);
    walk_check(ctx, prefix, table, n);

    //** rename a file in the same directory
    table[6] = "/d1/d2/d3/d4/f41-new";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4/f41", prefix);
    snprintf(path2, sizeof(path2), "%s/d1/d2/d3/d4/f41-new", prefix);
    err += rename_file(ctx, path, path2);
    walk_check(ctx, prefix, table, n);

    //** Move it to a different directory
    table[6] = "/d1/d2/f21";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4/f41-new", prefix);
    snprintf(path2, sizeof(path2), "%s/d1/d2/f21", prefix);
    err += rename_file(ctx, path, path2);
    walk_check(ctx, prefix, table, n);

    //** and a directory
    table[5] = "/d1/d2/d4";
    table[7] = "/d1/d2/d4/f42";
    snprintf(path, sizeof(path), "%s/d1/d2/d3/d4", prefix);
    snprintf(path2, sizeof(path2), "%s/d1/d2/d4", prefix);
    err += rename_dir(ctx, path, path2);
    walk_check(ctx, prefix, table, n);

    //** delete a file
    table[1] = NULL;
    snprintf(path, sizeof(path), "%s/d1/f1", prefix);
    err += delete_file(ctx, path);
    walk_check(ctx, prefix, table, n);

    //** Try and delete a non-empty directory
    snprintf(path, sizeof(path), "%s/d1", prefix);
    assert(delete_dir(ctx, path, 0) != 0);
    walk_check(ctx, prefix, table, n);

    //** Recursively delete everything else
    err += delete_dir(ctx, path, 1);

    if (err == 0) {
        printf("PASSED DIR tests!\n");
    } else {
        printf("FAILED DIR tests!\n");
    }

    return(err);
}

//*************************************************************************************
//*************************************************************************************
//*************************************************************************************

int main(int argc, char **argv)
{
    hdfs_lstore_t *ctx;
    int i, start_option, err;
    char *prefix;
    char fname[4096];

    ctx = lstore_activate(&argc, &argv);
    prefix = "/tmp";
    i=1;

    do {
        start_option = i;
        if (strcmp(argv[i], "--prefix") == 0) { //** Recurse depth
            i++;
            prefix = argv[i];
            i++;
        }

    } while ((start_option < i) && (i<argc));


    //** Make the comparison and work buffers
    tbx_type_malloc_clear(random_buffer, char, bufsize);
    my_get_random(random_buffer, bufsize);

    //** Make the base directory
    snprintf(fname, sizeof(fname), "%s/working", prefix);
    assert(add_dir(ctx, fname) == 0);

    //** Run the R/W tests
    snprintf(fname, sizeof(fname), "%s/working/dummy", prefix);
    err = my_rw_tests(ctx, fname);

    snprintf(fname, sizeof(fname), "%s/working", prefix);
    err += my_dir_tests(ctx, fname);

    //cleanup
    assert(lstore_delete(ctx, fname, 0) == 0);

    lstore_deactivate(ctx);
    return(err);
}
