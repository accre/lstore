#include "assert.h"
#include "string.h"
#include <stdlib.h>
#include <stdio.h>

#define type_malloc_clear(var, type, count) type_malloc(var, type, count); type_memclear(var, type, count)

#define type_malloc(var, type, count) var = (type *)malloc(sizeof(type)*(count)); assert(var != NULL)
#define type_realloc(var, type, count) var = (type *)realloc(var, sizeof(type)*(count)); assert(var != NULL)
#define type_memclear(var, type, count) memset(var, 0, sizeof(type)*(count))

//***********************************************************************
// lio_*_attrs - Get/Set LIO attribute routines
//***********************************************************************

typedef char creds_t;

typedef struct {
    int op_status;
    int error_code;
} op_status_t;

typedef struct {
    int num;
    char path;
    double d;
} lio_config_t;

typedef struct {
    lio_config_t *lc;
    creds_t *creds;
    char *path;
    char *id;
    char **mkeys;
    char **mvals;
    int *mv_size;
    char *skey;
    void *sval;
    int *sv_size;
    int n_keys;
} lio_attrs_op_t;

//***********************************************************************
// lio_get_multiple_attrs
//***********************************************************************


//***********************************************************************
// lio_get_multiple_attrs
//***********************************************************************

int lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, char **val, int *v_size, int n_keys)
{
    int err, serr;

    printf("path=%s id=%s %s=%s %s=%s\n", path, id, key[0],val[0], key[1], val[1]);

    return(0);
}

//***********************************************************************

op_status_t lio_get_multiple_attrs_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    op_status_t status;
    int err;

    status.op_status = 0;
    status.error_code = 0;
    lio_get_multiple_attrs(op->lc, op->creds, op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
    return(status);
}

//***********************************************************************

void gop_lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, char **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    (*op) = (lio_attrs_op_t) {
        lc, creds, path, id, key, val, v_size, NULL, NULL, NULL, n_keys
    };
//  op->lc = lc;
//  op->creds = creds;
//  op->path = path;
//  op->id = id;
//  op->mkeys = key;
//  op->mvals = val;
//  op->mv_size = v_size;
//  op->n_keys = n_keys;

    lio_get_multiple_attrs_fn(op, 1);
}


#define GOP_WRAPPER(fn, optype, ...) \
  op_status_t fn ## _fn(void *arg, int id) \
  {                                        \
    optype *op = (optype *)arg;            \
    op_status_t status;                    \
    int err;                               \
                                           \
    status.op_status = 0;                  \
    status.error_code = 0;                 \
    lio_get_multiple_attrs(op->lc, op->creds, op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys); \
    return(status);                        \
  }                                        \
                                           \
  void gop_ ## fns(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, char **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    type_malloc_clear(op, lio_attrs_op_t, 1);

    (*op) = (lio_attrs_op_t) {
        lc, creds, path, id, key, val, v_size, NULL, NULL, NULL, n_keys
    };
//  op->lc = lc;
//  op->creds = creds;
//  op->path = path;
//  op->id = id;
//  op->mkeys = key;
//  op->mvals = val;
//  op->mv_size = v_size;
//  op->n_keys = n_keys;

    lio_get_multiple_attrs_fn(op, 1);
}



int main(int argc, char **argv)
{
    lio_config_t lc;
    char *path = "path";
    char *key[2];
    char *val[2];
    int v_size[2];
    int n_keys = 2;
    creds_t *creds = "creds";
    char *id = "my_id";

    n_keys = 2;
    key[0] = "key0";
    val[0] = "val0";
    v_size[0] = 0;
    key[1] = "key1";
    val[1] = "val1";
    v_size[1] = 1;

    lio_get_multiple_attrs(&lc, creds, path, id, key, val, v_size, n_keys);
    gop_lio_get_multiple_attrs(&lc, creds, path, id, key, val, v_size, n_keys);

    return(0);
}
