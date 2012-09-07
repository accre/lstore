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

#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "authn_abstract.h"

//***********************************************************************
// authn_fake_get_type - Returns the type
//***********************************************************************

char *authn_fake_get_type(creds_t *c)
{
  return("FAKE");
}

//***********************************************************************
// authn_fake_cred_init - Creates a Fake AuthN credential
//***********************************************************************

creds_t *authn_fake_cred_init(authn_t *an, int type, void **args)
{
  creds_t *c;

  c = cred_default_create();
  c->get_type = authn_fake_get_type;

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

authn_t *authn_fake_create(char *fname, char *section)
{
  authn_t *an;

  type_malloc(an, authn_t, 1);

  an->cred_init = authn_fake_cred_init;
  an->destroy = authn_fake_destroy;

  return(an);
}
