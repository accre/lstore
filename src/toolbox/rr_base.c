/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012) Copyright (c) 2012, Advanced Computing Center for Research and Education, Vanderbilt University, All rights reserved.  This Work is the sole and exclusive property of the Advanced Computing Center for Research and Education department at Vanderbilt University.  No right to disclose or otherwise disseminate any of the information contained herein is granted by virtue of your possession of this software except in accordance with the terms and conditions of a separate License Agreement entered into with Vanderbilt University.  
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

//*************************************************************************
//*************************************************************************

#include "rr_base.h"

//************************************************************************
// rr_create_socket - Creates zmq sockets based on mode
//************************************************************************

void *rr_create_socket(zctx_t *ctx, int mode, int sync_st, int asyn_st)
{
    void *socket;

    if (mode == SYNC_MODE) {
        socket = zsocket_new(ctx, sync_st);
    } else if (mode == ASYNC_MODE) {
        socket = zsocket_new(ctx, asyn_st);
    }

    return socket;
}

//*************************************************************************
// rr_set_mode_tm - Sets mode and timeout
//*************************************************************************

void rr_set_mode_tm(inip_file_t *keyfile, char *section, int *mode, int *timeout)
{
   char *mode_str= inip_get_string(keyfile, section, "mode", NULL);
    if (strcmp(mode_str, "sync") == 0) {
        *mode = SYNC_MODE;
    } else if (strcmp(mode_str, "async") == 0) {
        *mode = ASYNC_MODE;
    } else {
        log_printf(0, "Unknown Mode: %s.\n", mode_str);
        exit(1);
    }

    *timeout = inip_get_integer(keyfile, section, "timeout", TIMEOUT_DFT);

    free(mode_str);
}

//*************************************************************************
// rr_dump - Prints buf neatly
//*************************************************************************

void rr_dump(void *buf, int len)
{
    puts ("----------------------------------------");
    //  Dump the buffer as text or binary
    char *data = buf;
    int is_text = 1;
    int char_nbr;
    for (char_nbr = 0; char_nbr < len; char_nbr++)
        if ((unsigned char) data [char_nbr] < 32
        ||  (unsigned char) data [char_nbr] > 127)
            is_text = 0;

    printf ("[%03d] ", len);
    for (char_nbr = 0; char_nbr < len; char_nbr++) {
        if (is_text)
            printf ("%c", data [char_nbr]);
        else
            printf ("%02X", (unsigned char) data [char_nbr]);
    }
    printf ("\n");
}
