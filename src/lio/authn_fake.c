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
// Dummy/Fake AuthN service.  Always returns success!
//***********************************************************************

#define _log_module_index 185

#include "fmttypes.h"
#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "authn_abstract.h"

extern char *_lio_exe_name;  //** This is set by lio_init long before we would ever be called.

typedef struct {
  char *handle;
  int len;
} authn_fake_priv_t;

//***********************************************************************
// authn_fake_get_type - Returns the type
//***********************************************************************

char *authn_fake_get_type(creds_t *c)
{
  return("FAKE");
}

//***********************************************************************
// authn_fake_get_type_filed - Returns the requested type field
//***********************************************************************

void *authn_fake_get_type_field(creds_t *c, int index, int *len)
{
  authn_fake_priv_t *a = (authn_fake_priv_t *)c->priv;

  if (index == AUTHN_INDEX_SHARED_HANDLE) {
     *len = a->len;
     return(a->handle);
  }

  *len = 0;
  return(NULL);
}

//***********************************************************************
// authn_fake_set_id - Sets the user ID and also makes the shared handle.
//  In this case the shared handle is really just string with the format
//     id:pid:userid@hostname
//***********************************************************************

void authn_fake_set_id(creds_t *c, char *id)
{
  authn_fake_priv_t *a = (authn_fake_priv_t *)c->priv;
  char buffer[1024], buf2[256], buf3[512];
  uint64_t pid;
  int err;

  c->id = strdup(id);

  pid = getpid();
  err = getlogin_r(buf2, sizeof(buf2));
  if (err != 0) snprintf(buf2, sizeof(buf2), "ERROR(%d)", err);
  gethostname(buf3, sizeof(buf3));
  snprintf(buffer, sizeof(buffer), "%s:" LU ":%s:%s:%s", id, pid, buf2, buf3, _lio_exe_name);
  a->handle = strdup(buffer);
  log_printf(5, "handle=%s\n", a->handle);
  a->len = strlen(a->handle)+1;

  return;
}


//***********************************************************************
// authn_fake_cred_destroy - Destroy the fake credentials
//***********************************************************************

void authn_fake_cred_destroy(creds_t *c)
{
  authn_fake_priv_t *a = (authn_fake_priv_t *)c->priv;

  if (a->handle != NULL) free(a->handle);
  free(a);

  if (c->handle_destroy != NULL) c->handle_destroy(c);
  if (c->id != NULL) free(c->id);
  free(c);
}

//***********************************************************************
// authn_fake_cred_init - Creates a Fake AuthN credential
//***********************************************************************

creds_t *authn_fake_cred_init(authn_t *an, int type, void **args)
{
  creds_t *c;

  c = cred_default_create();

  type_malloc_clear(c->priv, authn_fake_priv_t, 1);
  c->get_type = authn_fake_get_type;
  c->get_type_field = authn_fake_get_type_field;
  c->set_id = authn_fake_set_id;
  c->destroy = authn_fake_cred_destroy;

  return(c);
}

//***********************************************************************
// authn_fake_destroy - Destroys the FAke AuthN service
//***********************************************************************

void authn_fake_destroy(authn_t *an)
{
  free(an);
}

//***********************************************************************
// authn_fake_create - Create a Fake AuthN service
//***********************************************************************

authn_t *authn_fake_create(service_manager_t *ess, inip_file_t *ifd, char *section)
{
  authn_t *an;

  type_malloc(an, authn_t, 1);

  an->cred_init = authn_fake_cred_init;
  an->destroy = authn_fake_destroy;

  return(an);
}
