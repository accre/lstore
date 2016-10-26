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

#include <ibp-server/ibp_server.h>
#include <tbx/log.h>
#include <tbx/append_printf.h>

//***************************************************************************
// generate_command_acl - Fills in the command ACL list for a new connection
//***************************************************************************

void generate_command_acl(char *peer_name, int *acl)
{
  int i;
  command_t *command;

  log_printf(15, "generate_command_acl: peer_name=%s\n", peer_name);

  for (i=0; i<COMMAND_TABLE_MAX+1; i++) {
     command = &(global_config->command[i]);
     if (command->used == 1) {
        acl[i] = subnet_list_validate(command->subnet, peer_name);
     } else {
        acl[i] = 0;
     }
  }  
}


//*************************************************************************
// add_command - Adds a command to the vec table
//  If a collision occurs with a command occurs the program aborts.
//*************************************************************************

void add_command(int cmd, const char *cmd_keyword, tbx_inip_file_t *kf,  
   void (*load_config)(tbx_inip_file_t *keyfile), 
   void (*init)(void),
   void (*destroy)(void),
   int (*print_cmd)(char *buffer, int *used, int nbytes),
   int (*parse)(ibp_task_t *task, char **bstate),
   int (*execute)(ibp_task_t *task)) {

   char *list;

   if ((cmd < 0) || (cmd > COMMAND_TABLE_MAX)) {
      printf("add_command: Invalid commands slot. Requested %d.  Should be between 0 and %d\n", cmd, COMMAND_TABLE_MAX);
      fflush(stdout);
      abort();
   }

   command_t *command = &(global_config->command[cmd]);

   //** check for a collision
   if (command->used != 0) {
      printf("add_command: Command collision for slot %d!\n", cmd);
      fflush(stdout);
      abort();     
   }

   //*** Ok now install it in the table ***
   command->used = 1;
   strncpy(command->name, cmd_keyword, sizeof(command->name));  command->name[sizeof(command->name)-1] = '\0';
   command->command = cmd;
   command->load_config = load_config;
   command->init = init;
   command->destroy = destroy;
   command->print = print_cmd;
   command->read = parse;
   command->execute = execute;
   command->acl = tbx_inip_get_string(kf, "access_control", cmd_keyword, NULL);

   //*** Load the ip table filter ***
   list = command->acl;
   if (list == NULL) {
       log_printf(0, "add_command: Using default ACL for command %s!\n", cmd_keyword);
       list = global_config->server.default_acl;
       if (list == NULL) {
          log_printf(0, "add_command: No default access_control flag found!\n");
          abort();
       }
   }

//log_printf(15, "add_command: initial acl=%s\n", list[0]);

   command->subnet = new_subnet_list(list);

   //** Lastly load the config
   if (load_config != NULL) command->load_config(kf);
}

//*************************************************************************
//  print_command_config - Prints the config for all commands
//*************************************************************************

int print_command_config(char *buffer, int *used, int nbytes) 
{

  int i;
  command_t *cmd;

  for (i=0; i<=COMMAND_TABLE_MAX; i++) {
     cmd = &(global_config->command[i]);
     if (cmd->used == 1) {
        if (cmd->print != NULL) {
           cmd->print(buffer, used, nbytes);
           tbx_append_printf(buffer, used, nbytes, "\n");
        }
     }
  }

  //** Now print the access_control information
  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "[access_control]\n");
  if (global_config->server.default_acl != NULL) {
    tbx_append_printf(buffer, used, nbytes, "default = %s\n", global_config->server.default_acl);
  }
  
  for (i=0; i<=COMMAND_TABLE_MAX; i++) {
     cmd = &(global_config->command[i]);
     if (cmd->used == 1) {
//log_printf(15, "print_command_config: cmd=%s\n", cmd->name);
        if (cmd->acl != NULL) {
           tbx_append_printf(buffer, used, nbytes, "%s = %s\n", cmd->name, cmd->acl);
        }
     }
  }

  i = tbx_append_printf(buffer, used, nbytes, "\n");
  return(i);
}

//*************************************************************************
// initialize_commands - Executes each commands init routine
//*************************************************************************

void initialize_commands() {
  int i;
  command_t *cmd;
  
  for (i=0; i<=COMMAND_TABLE_MAX; i++) {
     cmd = &(global_config->command[i]);
     if (cmd->used == 1) {
        if (cmd->init != NULL) cmd->init();
     }
  }
}


//*************************************************************************
// destroy_commands - Executes each commands destroy routine
//*************************************************************************

void destroy_commands() {
  int i;
  command_t *cmd;
  
  for (i=0; i<=COMMAND_TABLE_MAX; i++) {
     cmd = &(global_config->command[i]);
     if (cmd->used == 1) {
        if (cmd->destroy != NULL) cmd->destroy();
        if (cmd->acl != NULL) free(cmd->acl);
        destroy_subnet_list(cmd->subnet);
     }
  }  
}
