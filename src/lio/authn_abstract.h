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
// Generic Authentication service
//***********************************************************************

#include "service_manager.h"
#include "iniparse.h"

#ifndef _AUTHN_ABSTRACT_H_
#define _AUTHN_ABSTRACT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define AUTHN_AVAILABLE "authn_available"

#define AUTHN_INDEX_SHARED_HANDLE 0

typedef struct creds_s creds_t;

struct creds_s {
    void *priv;
    void *handle;
    char *id;
    void (*handle_destroy)(void *);
    char *(*get_type)(creds_t *creds);
    void *(*get_type_field)(creds_t *creds, int index, int *len);
    char *(*get_id)(creds_t *creds);
    void (*set_id)(creds_t *creds, char *id);
    void *(*get_private_handle)(creds_t *creds);
    void (*set_private_handle)(creds_t *creds, void *handle, void (*destroy)(void *));
    void (*destroy)(creds_t *creds);
};

creds_t *cred_default_create();

#define an_cred_get_type(c) (c)->get_type(c)
#define an_cred_get_type_field(c, index, len) (c)->get_type_field(c, index, len)
#define an_cred_get_id(c) (c)->get_id(c)
#define an_cred_set_id(c, id) (c)->set_id(c, id)
#define an_cred_get_private_handle(c) (c)->get_private_handle(c)
#define an_cred_set_private_handle(c, handle, destroy) (c)->set_private_handle(c, handle, destroy)
#define an_cred_destroy(c) (c)->destroy(c)


typedef struct authn_s authn_t;

struct authn_s {
    void *priv;
    creds_t *(*cred_init)(authn_t *an, int type, void **args);
    void (*destroy)(authn_t *an);
};

typedef authn_t *(authn_create_t)(service_manager_t *ess, inip_file_t *ifd, char *section);

#define authn_cred_init(an, type, args) (an)->cred_init(an, type, args)
#define authn_destroy(an) (an)->destroy(an)

//int install_authn_service(char *type, authn_t *(*create)(char *fname));
//authn_t *create_authn_service(char *type, char *fname);

#ifdef __cplusplus
}
#endif

#endif

