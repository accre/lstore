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

#define _log_module_index 207

#include <assert.h>
#include <leveldb/c.h>
#include <apr_pools.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include <tbx/string_token.h>

#include <lio/authn.h>
#include <lio/ds.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>

#include "warmer_helpers.h"

//*************************************************************************
// warmer_query_inode - Generate list based on inode
//*************************************************************************

void warmer_query_inode(leveldb_t *inode_db, int mode, int fonly)
{
    leveldb_readoptions_t *opt;
    leveldb_iterator_t *it;
    char *buf;
    size_t nbytes;
    int we;
    int state, nfailed;
    char *errstr;
    char *name;

    //** Create the iterator
    opt = leveldb_readoptions_create();
    it = leveldb_create_iterator(inode_db, opt);
    leveldb_iter_seek_to_first(it);

    while (leveldb_iter_valid(it) > 0) {
        buf = (char *)leveldb_iter_value(it, &nbytes);
        if (nbytes == 0) { goto next; }

        if (warm_parse_inode(buf, nbytes, &state, &nfailed, &name) != 0) { goto next; }

        if ((state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", name);
            } else {
                we = ((state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|%d|%d\n", name, nfailed, we);
            }
        }

        free(name);
next:
        leveldb_iter_next(it);

        errstr = NULL;
        leveldb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }
    }

    //** Cleanup
    leveldb_iter_destroy(it);
    leveldb_readoptions_destroy(opt);
}


//*************************************************************************
// warmer_query_rid - Generate list based on the RID
//*************************************************************************

void warmer_query_rid(char *rid_key, leveldb_t *inode_db, leveldb_t *rid_db, int mode, int fonly, ex_off_t total_bytes)
{
    leveldb_readoptions_t *opt;
    leveldb_iterator_t *it;
    ex_off_t bytes_found;
    size_t nbytes;
    ex_off_t bsize;
    char *buf;
    int we, n;
    int state, nfailed;
    ex_id_t inode;
    char *errstr, *match, *rec_rid, *drid, *last, *name;
    const char *rid;

    //** Create the iterator
    n = strlen(rid_key) + 1 + 1 + 1;
    tbx_type_malloc(match, char, n);
    n = sprintf(match, "%s|0", rid_key) + 1;

    opt = leveldb_readoptions_create();
    it = leveldb_create_iterator(rid_db, opt);
    leveldb_iter_seek(it, match, n);

    bytes_found = 0;
    while (leveldb_iter_valid(it) > 0) {
        rid = leveldb_iter_key(it, &nbytes);
        drid = strdup(rid);
        rec_rid = tbx_stk_string_token(drid, "|", &last, &n);
        sscanf(tbx_stk_string_token(NULL, "|", &last, &n), XIDT, &inode);
        if (strcmp(rec_rid, rid_key) != 0) { //** Kick out
            free(drid);
            break;
        }

        buf = (char *)leveldb_iter_value(it, &nbytes);
        if (warm_parse_rid(buf, nbytes, &inode, &bsize, &state) != 0) { goto next; }
        buf = (char *)leveldb_get(inode_db, opt, (const char *)&inode, sizeof(ex_id_t), &nbytes, &errstr);
        if (nbytes == 0) { goto next; }

        if (warm_parse_inode(buf, nbytes, &state, &nfailed, &name) != 0) { goto next; }

        if ((state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", name);
            } else {
                we = ((state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|" XOT "|%d|%d\n", name, bsize, nfailed, we);
            }
        }

        free(name);
next:
        free(drid);
        leveldb_iter_next(it);

        errstr = NULL;
        leveldb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }

        //** Accumulate the space and see if we kick out
        bytes_found += bsize;
        if (total_bytes > 0) {
            if (bytes_found > total_bytes) break;
        }

    }

    //** Cleanup
    leveldb_iter_destroy(it);
    leveldb_readoptions_destroy(opt);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    int fonly, mode;
    char *db_base = "/lio/log/warm";
    char *rid_key;
    leveldb_t *inode_db, *rid_db;
    ex_off_t total_bytes;

    total_bytes = -1;  //** Default to print all files

    if (argc < 2) {
        printf("\n");
        printf("warmer_query -db DB_input_dir [-fonly] [-r rid_key [-b size]] [-w] [-s] [-f]\n");
        printf("    -db DB_input_dir   - Input directory for the DBes. DEfault is %s\n", db_base);
        printf("    -fonly             - Only print the filename\n");
        printf("    -r  rid_key        - Use RID for filtering matches instead of all files\n");
        printf("    -b  size           - Print enough files using this amount of space on the RID.\n");
        printf("                         Units are supported. The default is to print all files.\n");
        printf("    -w                 - Print files containing write errors\n");
        printf("    -s                 - Print files that were sucessfully warmed\n");
        printf("    -f                 - Print files that failed warming\n");
        printf("\n");
        printf("If no secondary modifier is provided all files matching the initial filter are printed\n");
        return(1);
    }

    i=1;
    fonly = 0;
    mode = 0;
    rid_key = NULL;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db") == 0) { //** DB output base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-r") == 0) { //** Use the RID name for selection
            i++;
            rid_key = argv[i];
            i++;
        } else if (strcmp(argv[i], "-b") == 0) { //** Use the RID name for selection
            i++;
            total_bytes = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-w") == 0) { //** Filter further by printing only files with write errors
            i++;
            mode |= WFE_WRITE_ERR;
        } else if (strcmp(argv[i], "-s") == 0) { //** Further filter by printing successfully warmed files
            i++;
            mode |= WFE_SUCCESS;
        } else if (strcmp(argv[i], "-f") == 0) { //** Only print failed files
            i++;
            mode |= WFE_FAIL;
        } else if (strcmp(argv[i], "-fonly") == 0) { //** Only print the file name
            i++;
            fonly = 1;
        }

    } while ((start_option < i) && (i<argc));

    //** Default is to print everything
    if (mode == 0) mode = WFE_SUCCESS|WFE_FAIL|WFE_WRITE_ERR;

    //** Open the DBs
    if (open_warm_db(db_base, &inode_db, &rid_db) != 0) {
        printf("ERROR:  Failed opening the warmer DB!\n");
        return(1);
    }

    if (rid_key == NULL) {
        warmer_query_inode(inode_db, mode, fonly);
    } else {
        warmer_query_rid(rid_key, inode_db, rid_db, mode, fonly, total_bytes);
    }

    //** Close the DBs
    close_warm_db(inode_db, rid_db);

    return(0);
}


