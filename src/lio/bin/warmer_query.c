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

__attribute__((unused)) static int warm_put_inode(leveldb_t *db, ex_id_t inode, int state, int nfailed, char *name);
__attribute__((unused)) static int warm_put_rid(leveldb_t *db, char *rid, ex_id_t inode, int state);
__attribute__((unused)) static void create_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db);
#include "warmer_helpers.h"

//*************************************************************************
// warmer_query_inode - Generate list based on inode
//*************************************************************************

void warmer_query_inode(leveldb_t *inode_db, int mode, int fonly)
{
    leveldb_readoptions_t *opt;
    leveldb_iterator_t *it;
    db_inode_entry_t *r;
    size_t nbytes;
    int we;
    char *errstr;

    //** Create the iterator
    opt = leveldb_readoptions_create();
    it = leveldb_create_iterator(inode_db, opt);
    leveldb_iter_seek_to_first(it);

    while (leveldb_iter_valid(it) > 0) {
        r = (db_inode_entry_t *)leveldb_iter_value(it, &nbytes);
        if (nbytes == 0) { goto next; }

        if ((r->state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", r->name);
            } else {
                we = ((r->state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|%d|%d\n", r->name, r->nfailed, we);
            }
        }

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

void warmer_query_rid(char *rid_key, leveldb_t *inode_db, leveldb_t *rid_db, int mode, int fonly)
{
    leveldb_readoptions_t *opt;
    leveldb_iterator_t *it;
    size_t nbytes;
    db_inode_entry_t *ir;
    int we, n;
    ex_id_t inode;
    char *errstr, *match, *rec_rid, *drid, *last;
    const char *rid;

    //** Create the iterator
    n = strlen(rid_key) + 1 + 1 + 1;
    tbx_type_malloc(match, char, n);
    n = sprintf(match, "%s|0", rid_key) + 1;

    opt = leveldb_readoptions_create();
    it = leveldb_create_iterator(rid_db, opt);
    leveldb_iter_seek(it, match, n);

    while (leveldb_iter_valid(it) > 0) {
        rid = leveldb_iter_key(it, &nbytes);
        drid = strdup(rid);
        rec_rid = tbx_stk_string_token(drid, "|", &last, &n);
        sscanf(tbx_stk_string_token(NULL, "|", &last, &n), XIDT, &inode);
        if (strcmp(rec_rid, rid_key) != 0) { //** Kick out
            free(drid);
            break;
        }

        ir = (db_inode_entry_t *)leveldb_get(inode_db, opt, (const char *)&inode, sizeof(inode), &nbytes, &errstr);
        if (nbytes == 0) { goto next; }

        if ((ir->state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", ir->name);
            } else {
                we = ((ir->state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|%d|%d\n", ir->name, ir->nfailed, we);
            }
        }

next:
        free(drid);
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
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    int fonly, mode;
    char *db_base = "/lio/log/warm";
    char *rid_key;
    leveldb_t *inode_db, *rid_db;

    if (argc < 2) {
        printf("\n");
        printf("warmer_query -db DB_input_dir [-fonly] [-r rid_key] [-w] [-s] [-f]\n");
        printf("    -db DB_input_dir   - Input directory for the DBes. DEfault is %s\n", db_base);
        printf("    -fonly             - Only print the filename\n");
        printf("    -r  rid_key        - Use RID for filtering matches instead of all files\n");
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
        warmer_query_rid(rid_key, inode_db, rid_db, mode, fonly);
    }

    //** Close the DBs
    close_warm_db(inode_db, rid_db);

    return(0);
}


