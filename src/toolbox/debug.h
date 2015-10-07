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

//**********************************************************
//**********************************************************


#ifndef __DEBUG_H_
#define __DEBUG_H_

#ifdef _ENABLE_DEBUG

#include <stdio.h>
#include <log.h>

//extern FILE *_debug_fd;   //**Default all I/O to stdout
//extern int _debug_level;

#define debug_code(a) a
#define debug_printf(n, ...) log_printf(n, __VA_ARGS__)
#define set_debug_level(n) set_log_level(n)
#define flush_debug() flush_log()
#define debug_level() log_level()

#else

#define debug_code(a) 
#define debug_printf(n, ...)
#define set_debug_level(n) 
#define flush_debug() 
#define debug_level()

#endif
#endif
