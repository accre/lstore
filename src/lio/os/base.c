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
// Routines for managing the segment loading framework
//***********************************************************************

#define _log_module_index 154

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <errno.h>
#include <libgen.h>
#include <regex.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include <unistd.h>

#include "os.h"

apr_thread_mutex_t *_path_parse_lock = NULL;
apr_pool_t *_path_parse_pool = NULL;
tbx_atomic_int_t _path_parse_counter = 0;

//***********************************************************************
// lio_os_glob2regex - Converts a string in shell glob notation to regex
//***********************************************************************

char *lio_os_glob2regex(char *glob)
{
    char *reg;
    int i, j, n, n_regex;

    n = strlen(glob);
    n_regex = 2*n + 10;
    tbx_type_malloc(reg, char, n_regex);

    j = 0;
    reg[j] = '^';
    j++;

//log_printf(15, "glob=%s\n", glob);

    switch (glob[0]) {
    case ('.') :
        reg[j] = '\\';
        reg[j+1] = '.';
        j+=2;
        break;
    case ('*') :
        reg[j] = '.';
        reg[j+1] = '*';
        j+=2;
        break;
    case ('?') :
        reg[j] = '.';
        j++;
        break;
    default  :
        reg[j] = glob[0];
        j++;
        break;
    }

    for (i=1; i<n; i++) {
        switch (glob[i]) {
        case ('.') :
            reg[j] = '\\';
            reg[j+1] = '.';
            j+=2;
            break;
        case ('*') :
            if (glob[i-1] == '\\') {
                reg[j] = '*';
                j++;
            } else {
                reg[j] = '.';
                reg[j+1] = '*';
                j+=2;
            }
            break;
        case ('?') :
            reg[j] = (glob[i-1] == '\\') ? '?' : '.';
            j++;
            break;
        default    :
            reg[j] = glob[i];
            j++;
            break;
        }

        if (j>= n_regex-2) {
            n_regex *= 2;
            reg = realloc(reg, n_regex);
        }

    }

    reg[j] = '$';
    reg[j+1] = 0;
    j += 2;

    return(realloc(reg, j));
}

//***********************************************************************
// check_for_glob - Returns 1 if a glob char acter exists in the string
//***********************************************************************

int check_for_glob(char *glob)
{
    int i;

    if ((glob[0] == '*') || (glob[0] == '?') || (glob[0] == '[')) return(1);

    i = 1;
    while (glob[i] != 0) {
        if ((glob[i] == '*') || (glob[i] == '?') || (glob[i] == '[')) {
            if (glob[i-1] != '\\') return(1);
        }

        i++;
    }

    return(0);
}

//***********************************************************************
// lio_os_regex_is_fixed - Returns 1 if the regex is a fixed path otherwise 0
//***********************************************************************

int lio_os_regex_is_fixed(lio_os_regex_table_t *regex)
{

    if (regex->n == 0) return(1);
    if (regex->n == 1) return(regex->regex_entry[0].fixed);

    return(0);
}

//***********************************************************************
// lio_os_path_glob2regex - Converts a path glob to a regex table
//***********************************************************************

lio_os_regex_table_t *lio_os_path_glob2regex(char *path)
{
    lio_os_regex_table_t *table;
    char *bstate, *p2, *frag, *f2;
    int i, j, n, fin, err;

    p2 = strdup(path);

    //** Determine the max number of path fragments
    j = 1;
    n = strlen(p2);
    for (i=0; i<n; i++) {
        if (p2[i] == '/') j++;
    }

    log_printf(15, "START path=%s max_frags=%d\n", path, j);

    //** Make the  table space
    table = os_regex_table_create(n);


    //** Cycle through the fragments converting them
    i = 0;
    frag = tbx_stk_escape_string_token(p2, "/", '\\', 1, &bstate, &fin);
    fin = 0;
    table->regex_entry[0].expression = NULL;
    while (frag[0] != 0 ) {
        if (check_for_glob(frag) == 0) {
            if (table->regex_entry[i].expression != NULL) {
                n = strlen(table->regex_entry[i].expression);
                table->regex_entry[i].fixed_prefix = n;
                n = n + strlen(frag) + 2;
                tbx_type_malloc(f2, char, n);
                snprintf(f2, n, "%s/%s", table->regex_entry[i].expression, frag);
                free(table->regex_entry[i].expression);
                table->regex_entry[i].expression = f2;
            } else {
                table->regex_entry[i].fixed_prefix = 0;
                table->regex_entry[i].expression = strdup(frag);
            }

            table->regex_entry[i].fixed = 1;
        } else {
            if (table->regex_entry[i].fixed == 1) i++;

            table->regex_entry[i].expression = lio_os_glob2regex(frag);
            log_printf(15, "   i=%d glob=%s  regex=%s\n", i, frag, table->regex_entry[i].expression);
            err = regcomp(&(table->regex_entry[i].compiled), table->regex_entry[i].expression, REG_NOSUB|REG_EXTENDED);
            if (err != 0) {
                lio_os_regex_table_destroy(table);
                log_printf(0, "lio_os_path_glob2regex: Error with fragment %s err=%d tid=%d\n", table->regex_entry[i].expression, err, tbx_atomic_thread_id);
                free(p2);
                return(NULL);
            }

            i++;
        }

        frag = tbx_stk_escape_string_token(NULL, "/", '\\', 1, &bstate, &fin);
    }

    table->n = (table->regex_entry[i].fixed == 1) ? i+1 : i; //** Adjust the table size

    if (tbx_log_level() >= 15) {
        for (i=0; i<table->n; i++) {
            log_printf(15, "i=%d fixed=%d frag=%s fixed_prefix=%d\n", i, table->regex_entry[i].fixed, table->regex_entry[i].expression, table->regex_entry[i].fixed_prefix);
        }
    }
    free(p2);
    return(table);
}

//***********************************************************************
// os_regex_table_create - Creates a regex table
//***********************************************************************

lio_os_regex_table_t *os_regex_table_create(int n)
{
    lio_os_regex_table_t *table;

    tbx_type_malloc_clear(table, lio_os_regex_table_t, 1);
    if ( n> 0) tbx_type_malloc_clear(table->regex_entry, lio_os_regex_entry_t, n);
    table->n = n;

    return(table);
}

//***********************************************************************
// lio_os_regex2table - Creates a regex table from the regular expression
//***********************************************************************

lio_os_regex_table_t *lio_os_regex2table(char *regex)
{
    int err;
    lio_os_regex_table_t *table;

    table = os_regex_table_create(1);

    table->regex_entry[0].expression = strdup(regex);
    err = regcomp(&(table->regex_entry[0].compiled), table->regex_entry[0].expression, REG_NOSUB|REG_EXTENDED);
    if (err != 0) {
        lio_os_regex_table_destroy(table);
        log_printf(0, "Error with fragment %s err=%d tid=%d\n", table->regex_entry[0].expression, err, tbx_atomic_thread_id);
        return(NULL);
    }

    return(table);
}

//***********************************************************************
// lio_os_regex_table_destroy - Destroys a regex table
//***********************************************************************

void lio_os_regex_table_destroy(lio_os_regex_table_t *table)
{
    int i;
    lio_os_regex_entry_t *re;

    if (table->regex_entry != NULL) {
        for (i=0; i<table->n; i++) {
            re = &(table->regex_entry[i]);
            free(re->expression);
            if (re->fixed == 0) {
                regfree(&(re->compiled));
            }
        }

        free(table->regex_entry);
    }

    free(table);
}

//***********************************************************************
//  path_split - Splits the path into a basename and dirname
//***********************************************************************

void lio_os_path_split(const char *path, char **dir, char **file)
{
    char *ptr;

    log_printf(15, "path=%s\n", path);

    ptr = strdup(path);

    if (_path_parse_lock == NULL) {
        if (tbx_atomic_inc(_path_parse_counter) == 0) {   //** Only init if needed
            apr_pool_create(&_path_parse_pool, NULL);
            apr_thread_mutex_create(&_path_parse_lock, APR_THREAD_MUTEX_DEFAULT, _path_parse_pool);
            tbx_atomic_set(_path_parse_counter, 1000000);
        } else {
            while (tbx_atomic_get(_path_parse_counter) != 1000000) {
                usleep(100);
            }
        }
    }

    apr_thread_mutex_lock(_path_parse_lock);

    *dir = strdup(dirname(ptr));

    free(ptr);
    ptr = strdup(path);
    *file = strdup(basename(ptr));
    free(ptr);

    apr_thread_mutex_unlock(_path_parse_lock);

    log_printf(15, "path=%s dir=%s file=%s\n", path, *dir, *file);

}

//***********************************************************************
// os_regex_table_pack - Packs a regex table into the buffer and returns
//   the number of chars used or a negative value representing the needed space
//***********************************************************************

int os_regex_table_pack(lio_os_regex_table_t *regex, unsigned char *buffer, int bufsize)
{
    int i, err, n, bpos, len;

    if (regex == NULL) {
        n = tbx_zigzag_encode(0, buffer);
        return(n);
    }

    err = 0;
    bpos = 0;
    n = -1;
    if ((bpos + 4) < bufsize) n = tbx_zigzag_encode(regex->n, &(buffer[bpos]));
    if (n < 0) {
        err = 1;
        n=4;
    }
    bpos += n;

    for (i=0; i<regex->n; i++) {
        log_printf(5, "level=%d fixed=%d fixed_prefix=%d expression=%s bpos=%d\n", i, regex->regex_entry[i].fixed, regex->regex_entry[i].fixed_prefix, regex->regex_entry[i].expression, bpos);
        n = -1;
        if ((bpos + 4) < bufsize) n = tbx_zigzag_encode(regex->regex_entry[i].fixed, &(buffer[bpos]));
        if (n < 0) {
            err = 1;
            n=4;
        }
        bpos += n;

        n = -1;
        if ((bpos + 4) < bufsize) n = tbx_zigzag_encode(regex->regex_entry[i].fixed_prefix, &(buffer[bpos]));
        if (n < 0) {
            err = 1;
            n=4;
        }
        bpos += n;

        len = strlen(regex->regex_entry[i].expression);
        n = -1;
        if ((bpos + 4) < bufsize) n = tbx_zigzag_encode(len, &(buffer[bpos]));
        if (n < 0) {
            err = 1;
            n=4;
        }
        bpos += n;
        if ((bpos+len) < bufsize) {
            memcpy(&(buffer[bpos]), regex->regex_entry[i].expression, len);
        } else {
            log_printf(5, "ERROR buffer to small!  exp=%s bpos=%d bufsize=%d exp_len=%d\n", regex->regex_entry[i].expression, bpos, bufsize, len);
            err = 1;
        }
        bpos += len;
    }

    if (err == 1) bpos = -bpos;

    log_printf(5, "nbytes used=%d\n", bpos);

    return(bpos);
}

//***********************************************************************
// os_regex_table_unpack - UnPacks a regex table from the buffer and returns
//   the regex table and also the number of characters used
//***********************************************************************

lio_os_regex_table_t *os_regex_table_unpack(unsigned char *buffer, int bufsize, int *used)
{
    int i, err, n, bpos;
    int64_t value;
    lio_os_regex_table_t *regex;

    bpos = 0;
    n = tbx_zigzag_decode(&(buffer[bpos]), bufsize, &value);
    log_printf(5, "n_levels=" I64T " n=%d bufsize=%d\n", value, n, bufsize);

    if (n < 0) {
        *used = 0;
        return(NULL);
    }
    if (value == 0) {
        *used = n;
        return(NULL);
    }
    bpos += n;
    bufsize -= n;

    regex = os_regex_table_create(value);

    for (i=0; i<regex->n; i++) {
        n = tbx_zigzag_decode(&(buffer[bpos]), bufsize, &value);
        log_printf(5, "i=%d n=%d fixed=" I64T "\n", i, n, value);
        if (n < 0) goto fail;
        regex->regex_entry[i].fixed = value;
        bpos += n;
        bufsize -= n;

        n = tbx_zigzag_decode(&(buffer[bpos]), bufsize, &value);
        log_printf(5, "i=%d n=%d fixed_prefix=" I64T "\n", i, n, value);
        if (n < 0) goto fail;
        regex->regex_entry[i].fixed_prefix = value;
        bpos += n;
        bufsize -= n;

        n = tbx_zigzag_decode(&(buffer[bpos]), bufsize, &value);
        log_printf(5, "i=%d n=%d exp_len=" I64T " bufsize=%d bpos=%d\n", i, n, value, bufsize, bpos);
        if (n < 0) goto fail;
        tbx_type_malloc(regex->regex_entry[i].expression, char, value+1);
        bpos += n;
        bufsize -= n;

        if (value <= bufsize) {
            memcpy(regex->regex_entry[i].expression, &(buffer[bpos]), value);
            regex->regex_entry[i].expression[value] = 0;
            if (regex->regex_entry[i].fixed == 0) {
                err = regcomp(&(regex->regex_entry[i].compiled), regex->regex_entry[i].expression, REG_NOSUB|REG_EXTENDED);
                if (err != 0) goto fail;
            }
        } else {
            log_printf(5, "ERROR! Buffer short! level=%d fixed=%d fixed_prefix=%d expression_len=" I64T " bpos=%d bufsize=%d\n", i, regex->regex_entry[i].fixed, regex->regex_entry[i].fixed_prefix, value, bpos, bufsize);
            goto fail;
        }
        bpos += value;

        log_printf(5, "level=%d fixed=%d fixed_prefix=%d expression=%s bpos=%d\n", i, regex->regex_entry[i].fixed, regex->regex_entry[i].fixed_prefix, regex->regex_entry[i].expression, bpos);
    }

    *used = bpos;
    return(regex);

fail:
    lio_os_regex_table_destroy(regex);
    *used = bpos;
    return(NULL);
}

//***********************************************************************
// os_local_filetype_stat - Determines the file type and also returns
//     the stat of the object and also what it's linked to if requested.
//
//     NOTE: It's safe to have both stat objects be the same struct
//           stat_link is ONLY used if the path is a symlink!
//***********************************************************************

int os_local_filetype_stat(char *path, struct stat *stat_link, struct stat *stat_object)
{
    int err, ftype;

    ftype = 0;
    err = lstat(path, stat_object);  //** Assume it's not a symlink
    if (err == 0) {
        if (S_ISLNK(stat_object->st_mode)) {
            *stat_link = *stat_object;  //** Got a symlink so track it
            err = stat(path, stat_object);
            ftype |= OS_OBJECT_SYMLINK_FLAG;
        }

        if (err == 0) {
            if (S_ISREG(stat_object->st_mode)) {
                ftype |= OS_OBJECT_FILE_FLAG;
                if (stat_object->st_nlink > 1) ftype |= OS_OBJECT_HARDLINK_FLAG;
            } else if (S_ISDIR(stat_object->st_mode)) {
                ftype |= OS_OBJECT_DIR_FLAG;
            }
        } else {
            ftype |= OS_OBJECT_FILE_FLAG|OS_OBJECT_BROKEN_LINK_FLAG;  //** Broken link so flag it as a file anyhow

        }
    } else {
        log_printf(1, "lstat error!  fname=%s errno=%d\n", path, errno);
    }

    return(ftype);
}

//***********************************************************************
// lio_os_local_filetype - Determines the file type
//***********************************************************************

int lio_os_local_filetype(char *path)
{
    struct stat s;

    return(os_local_filetype_stat(path, &s, &s));
}
