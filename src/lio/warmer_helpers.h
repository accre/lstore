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
//***********************************************************************
// Warmer helper definitions
//***********************************************************************


#ifndef _WARMER_H_
#define _WARMER_H_

#ifdef __cplusplus
extern "C" {
#endif

#define WFE_SUCCESS   1
#define WFE_FAIL      2
#define WFE_WRITE_ERR 4

typedef struct {
    int  state;
    int  nfailed;
    char name[1];
} __attribute__((packed)) db_inode_entry_t;

typedef struct {
    ex_id_t inode;
    int  state;
} __attribute__((packed)) db_rid_entry_t;

//*************************************************************************
// warm_put_inode - Puts an entry in the inode DB
//*************************************************************************

static int warm_put_inode(leveldb_t *db, ex_id_t inode, int state, int nfailed, char *name)
{
    leveldb_writeoptions_t *wopt;
    db_inode_entry_t *r;
    char *errstr = NULL;
    int n;

    n = strlen(name) + sizeof(db_inode_entry_t);
    r = malloc(n);
    r->state = state;
    r->nfailed = nfailed;
    strcpy(r->name, name);

    wopt = leveldb_writeoptions_create();
    leveldb_put(db, wopt, (const char *)&inode, sizeof(ex_id_t), (const char *)r, n, &errstr);
    leveldb_writeoptions_destroy(wopt);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    free(r);

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// warm_put_rid - Puts an entry in the RID DB
//*************************************************************************

static int warm_put_rid(leveldb_t *db, char *rid, ex_id_t inode, int state)
{
    leveldb_writeoptions_t *wopt;
    db_rid_entry_t r;
    char *errstr = NULL;
    char *key;
    int n;

    n = strlen(rid) + 1 + 20 + 1;
    tbx_type_malloc(key, char, n);
    n = sprintf(key, "%s|" XIDT, rid, inode) + 1;

    r.inode = inode;
    r.state = state;

    wopt = leveldb_writeoptions_create();
    leveldb_put(db, wopt, key, n, (const char *)&r, sizeof(db_rid_entry_t), &errstr);
    leveldb_writeoptions_destroy(wopt);

    free(key);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// create_a_db - Creates  a LevelDB database using the given path
//*************************************************************************

static leveldb_t *create_a_db(char *db_path)
{
    leveldb_t *db;
    leveldb_options_t *opt_exists, *opt_create, *opt_none;
    char *errstr = NULL;

    opt_exists = leveldb_options_create(); leveldb_options_set_error_if_exists(opt_exists, 1);
    opt_create = leveldb_options_create(); leveldb_options_set_create_if_missing(opt_create, 1);
    opt_none = leveldb_options_create();

    db = leveldb_open(opt_exists, db_path, &errstr);
    if (errstr != NULL) {  //** It already exists so need to remove it first
        free(errstr);
        errstr = NULL;

        //** Remove it
        leveldb_destroy_db(opt_none, db_path, &errstr);
        if (errstr != NULL) {  //** Got an error so just kick out
            fprintf(stderr, "ERROR: Failed removing %s for fresh DB. ERROR:%s\n", db_path, errstr);
            exit(1);
        }

        //** Try opening it again
        db = leveldb_open(opt_create, db_path, &errstr);
        if (errstr != NULL) {  //** An ERror occured
            fprintf(stderr, "ERROR: Failed creating %s. ERROR:%s\n", db_path, errstr);
            exit(1);
        }
    }

    leveldb_options_destroy(opt_none);
    leveldb_options_destroy(opt_exists);
    leveldb_options_destroy(opt_create);

    return(db);
}

//*************************************************************************
//  create_warm_db - Creates the DBs using the given base directory
//*************************************************************************

static void create_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db)
{
    char *db_path;

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = create_a_db(db_path);
    sprintf(db_path, "%s/rid", db_base); *rid_db = create_a_db(db_path);
    free(db_path);
}

//*************************************************************************
// open_a_db - Opens a LevelDB database
//*************************************************************************

static leveldb_t *open_a_db(char *db_path)
{
    leveldb_t *db;
    leveldb_options_t *opt;
    char *errstr = NULL;

    opt = leveldb_options_create();

    db = leveldb_open(opt, db_path, &errstr);
    if (errstr != NULL) {  //** It already exists so need to remove it first
        fprintf(stderr, "ERROR: Failed creating %s. ERROR:%s\n", db_path, errstr);
        free(errstr);
        errstr = NULL;
    }

    leveldb_options_destroy(opt);

    return(db);
}

//*************************************************************************
//  open_warm_db - Opens the warmer DBs
//*************************************************************************

static int open_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db)
{
    char *db_path;

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = open_a_db(db_path);
    if (inode_db == NULL) { free(db_path); return(1); }

    sprintf(db_path, "%s/rid", db_base); *rid_db = open_a_db(db_path);
    if (rid_db == NULL) { free(db_path); return(2); }

    free(db_path);
    return(0);
}

//*************************************************************************
//  close_warm_db - Closes the DBs
//*************************************************************************

static void close_warm_db(leveldb_t *inode, leveldb_t *rid)
{
    leveldb_close(inode);
    leveldb_close(rid);
}

#ifdef __cplusplus
}
#endif

#endif

