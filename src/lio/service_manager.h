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
// Generic service manager header file
//***********************************************************************

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_hash.h>

#ifndef _SERVICE_MANAGER_H_
#define _SERVICE_MANAGER_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    apr_pool_t *pool;
    apr_thread_mutex_t *lock;
    apr_hash_t *table;
} service_manager_t;

service_manager_t *clone_service_manager(service_manager_t *sm);
service_manager_t *create_service_manager();
void destroy_service_manager(service_manager_t *sm);
void *lookup_service(service_manager_t *sm, char *service_section, char *service_name);
int add_service(service_manager_t *sm, char *service_section, char *service_name, void *service);
int remove_service(service_manager_t *sm, char *service_section, char *service_name);
//int set_service_type_arg(service_manager_t *sm, int sm_type, void *arg);
//void *get_service_type_arg(service_manager_t *sm, int sm_type);

#ifdef __cplusplus
}
#endif

#endif

