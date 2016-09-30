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

/** @file util.c
 *
 * Commonly-used helper functions
 */

#include <globus_gridftp_server.h>
#include <stdio.h>
#include <string.h>

#define ADVANCE_SLASHES(X) while ((X)[0] == '/' && (X)[1] == '/') { (X)++; }

const char *path_to_lstore(const char *prefix, const char *path) {
    if (!path) {
        return NULL;
    }
    ADVANCE_SLASHES(path);
    if (strncmp(path, prefix, strlen(prefix)) == 0) {
        path += strlen(prefix);
        ADVANCE_SLASHES(path);
        return path;
    }
    return NULL;
}

char *copy_path_to_lstore(const char *prefix, const char *path) {
    // Extract the LStore-specific path
    const char *lstore_path = path_to_lstore(prefix, path);
    if (!lstore_path) {
        return NULL;
    }
    char *path_copy = strdup(lstore_path);
    if (!path_copy) {
        return NULL;
    }
    return path_copy;
}
 
/*
 * Stolen from the Globus file DSI implementation
 */
void destroy_stat(globus_gfs_stat_t *stat_array, int stat_count)
{
    int i;
    GlobusGFSName(destroy_stat);

    for(i = 0; i < stat_count; i++) {
        if(stat_array[i].name != NULL) {
            globus_free(stat_array[i].name);
        }
        if(stat_array[i].symlink_target != NULL) {
            globus_free(stat_array[i].symlink_target);
        }
    }
    globus_free(stat_array);
}

void transfer_stat(globus_gfs_stat_t *stat_object,
                                        struct stat *fileInfo,
                                        const char *filename,
                                        const char *symlink_target)
{
    GlobusGFSName(transfer_stat);

    stat_object->mode     = (S_ISDIR(fileInfo->st_mode)) ? (S_IFDIR |
                            fileInfo->st_mode) :  (S_IFREG | fileInfo->st_mode);
    stat_object->nlink    = (S_ISDIR(fileInfo->st_mode)) ? 3 : 1;
    stat_object->uid = fileInfo->st_uid;
    stat_object->gid = fileInfo->st_gid;
    stat_object->size     = (S_ISDIR(fileInfo->st_mode)) ? 4096 : fileInfo->st_size;
    stat_object->mtime    = fileInfo->st_mtime;
    stat_object->atime    = fileInfo->st_atime;
    stat_object->ctime    = fileInfo->st_ctime;
    stat_object->dev      = fileInfo->st_dev;
    stat_object->ino      = fileInfo->st_ino;

    stat_object->name = NULL;
    if(filename && *filename) {
        const char * real_filename = filename;
        while (strchr(real_filename, '/')) {
            if (*(real_filename+1) != '\0') {
                real_filename++;
            } else {
                break;
            }
        }
        stat_object->name = strdup(real_filename);
    }
    if(symlink_target && (strlen(symlink_target) != 0)) {
        stat_object->symlink_target = strdup(symlink_target);
    } else {
        stat_object->symlink_target = NULL;
    }
}
