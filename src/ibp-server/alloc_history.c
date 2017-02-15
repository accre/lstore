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

#include "resource.h"
#include "allocation.h"
#include "cap_timestamp.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>

//** These are just dummies and are really only needed for a DB implementation
int create_history_table(Resource_t *r) { return(0); }
int mount_history_table(Resource_t *r) { return(0); }
void umount_history_table(Resource_t *r) { return; }


//****************************************************************************
// get_history_table - Retreives the history table fro the provided allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int fd_get_history_table(Resource_t *r, osd_fd_t *fd, Allocation_history_t *h)
{
  int n;

  n = osd_read(r->dev, fd, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
  if (n == sizeof(Allocation_history_t)) n = 0;

//log_printf(15, "get_history: r->name=%s id=" LU "\n", r->name, id);
log_printf(15, "fd_get_history: r=%s fd=%p h.id=" LU " ws=%d rs=%d ms=%d\n", r->name, fd, h->id, h->write_slot, h->read_slot, h->manage_slot); tbx_log_flush();
//fprintf(stderr, "get_history: id=" LU "\n", id); fflush(stderr);
//fprintf(stderr, "get_history:  write_slot=%d n=%d\n", h->write_slot, n);
//fprintf(stderr, "get_history: r=%s id=" LU " h.id=" LU " write_slot=%d n=%d\n", r->name, id, h->id, h->write_slot, n);

  return(n);
}

//****************************************************************************
// get_history_table - Retreives the history table fro the provided allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int get_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h)
{
   int err;
   osd_fd_t *fd = osd_open(r->dev, id, OSD_READ_MODE);
   if (fd == NULL) {
      log_printf(0, "get_history_table: Error with open_allocation for res=%s id=" LU "\n", r->name, id);
      return(-1);
   }

   err = fd_get_history_table(r, fd, h);

   osd_close(r->dev, fd);

   return(err);
}

//****************************************************************************
// put_history_table - Stores the history table for the given allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int fd_put_history_table(Resource_t *r, osd_fd_t *fd, Allocation_history_t *h)
{
  int n;

//  if (id != h->id) {
//     if (h->id == 0) { 
//       h->id = id;
//     } else {
//       log_printf(0, " put_history_table: h->id=" LU" differs from given id=" LU "\n", h->id, id);
//     }
//  }

  n = osd_write(r->dev, fd, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
  if (n == sizeof(Allocation_history_t)) n = 0;

log_printf(15, "fd_put_history: r=%s fd=%p h.id=" LU " write_slot=%d\n", r->name, fd, h->id, h->write_slot); tbx_log_flush();

  return(n);
}

//****************************************************************************
// put_history_table - Stores the history table for the given allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int put_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h)
{
   int err;
   osd_fd_t *fd = osd_open(r->dev, id, OSD_WRITE_MODE);
   if (fd == NULL) {
      log_printf(0, "put_history_table: Error with open_allocation for res=%s id=" LU "\n", r->name, id);
      return(-1);
   }

   err = fd_put_history_table(r, fd, h);

   osd_close(r->dev, fd);

   return(err);
}

//****************************************************************************
// blank_history - Writes a blank history record
//****************************************************************************

int blank_history(Resource_t *r, osd_id_t id)
{
   Allocation_history_t h;
   int err;

   if ((r->enable_read_history==0) && (r->enable_write_history==0) && (r->enable_manage_history==0)) return(0);

   memset(&h, 0, sizeof(h));
   h.id = id;

   err = put_history_table(r, id, &h);
   if (err != 0) {
      log_printf(0, "blank_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      return(err);
   }

//Allocation_history_t h1;
//get_history_table(r, id, &h1);
//log_printf(0, "blank_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h1.id, h1.write_slot);
//tbx_log_flush();

   return(0);
}


//****************************************************************************
// update_read_history - Updates the read history table for the allocation
//****************************************************************************

void update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;
   osd_fd_t *fd;

   if (r->enable_read_history == 0) return;
   if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

   fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
   if (fd == NULL) {
      log_printf(0, "update_read_history: Error with open_allocation for res=%s id=" LU "\n", r->name, id);
      return;
   }
   err = fd_get_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_read_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }

   set_read_timestamp(&h, add, offset, size, pid);

   err = fd_put_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_read_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }

   osd_close(r->dev, fd);
}

//****************************************************************************
// update_write_history - Updates the write history table for the allocation
//****************************************************************************

void update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;
   osd_fd_t *fd;

   if (r->enable_write_history == 0) return;
   if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

   fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
   if (fd == NULL) {
      log_printf(0, "update_write_history: Error with open_allocation for res=%s id=" LU "\n", r->name, id);
      return;
   }
   err = fd_get_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_write_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }
//log_printf(0, "update_write_history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", r->name, id, h.id, h.write_slot);
//tbx_log_flush();
   set_write_timestamp(&h, add, offset, size, pid);

   err = fd_put_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_write_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }

   osd_close(r->dev, fd);
}

//****************************************************************************
// update_manage_history - Updates the manage history table for the allocation
//****************************************************************************

void update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add, int cmd, int subcmd, int reliability, uint32_t expiration, uint64_t size, osd_id_t pid)
{
   Allocation_history_t h;
   int err;
   osd_fd_t *fd;

   if (r->enable_manage_history == 0) return;
   if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

   fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
   if (fd == NULL) {
      log_printf(0, "update_manage_history: Error with open_allocation for res=%s id=" LU "\n", r->name, id);
      return;
   }

   err = fd_get_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_manage_history: Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }

   set_manage_timestamp(&h, add, cmd, subcmd, reliability, expiration, size, pid);

   err = fd_put_history_table(r, fd, &h);
   if (err != 0) {
      log_printf(0, "update_manage_history: Error putting history for res=%s id=" LU " err=%d\n", r->name, id, err);
      osd_close(r->dev, fd);
      return;
   }

   osd_close(r->dev, fd);
}


