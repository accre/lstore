#define _log_module_index 109

//#define _DISABLE_LOG 1

#include <apr_time.h>
#include <limits.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tbx/assert_result.h"
#include "tbx/append_printf.h"
#include "tbx/atomic_counter.h"
#include "tbx/fmttypes.h"
#include "tbx/iniparse.h"
#include "tbx/log.h"
#include "tbx/stack.h"
#include "tbx/string_token.h"
#include "tbx/type_malloc.h"

#define BUFMAX 8192
#define PARAMS "_parameters"
#define VAR_DECLARE '$'

char *hint_ops[] = { "--ini-hint-add", "--ini-hint-remove", "--ini-hint-replace", "--ini-hint-default" };

typedef struct {
    FILE *fd;
    char buffer[BUFMAX];
    int used;
    char *text;
    char *text_pos;
} bfile_entry_t;

typedef struct {  //** Used for Reading the ini file
    bfile_entry_t *curr;
    tbx_stack_t *stack;
    tbx_stack_t *include_paths;
    int error;
} bfile_t;

struct tbx_inip_element_t {  //** Key/Value pair
    int substitution_check;
    char *key;
    char *value;
    struct tbx_inip_element_t *next;
};

struct tbx_inip_group_t {  //** Group
    int substitution_check;
    int n_kv_substitution_check;
    char *group;
    tbx_inip_element_t *list;
    struct tbx_inip_group_t *next;
};

struct tbx_inip_file_t {  //File
    tbx_inip_group_t *tree;
    int n_groups;
    int n_substitution_checks;
    tbx_atomic_int_t ref_count;
};

struct tbx_inip_hint_t {  //** Overriding hint
    char *section;
    char *key;
    char *value;
    int op;
    int section_rank;
    int key_rank;
};

void apply_params(tbx_inip_file_t  *fd);

// Accessors
tbx_inip_file_t *tbx_inip_dup(tbx_inip_file_t *ifd) {
    tbx_atomic_inc(ifd->ref_count);
    return(ifd);
}
tbx_inip_element_t * tbx_inip_ele_first(tbx_inip_group_t *group) {
    return group->list;
}

char *tbx_inip_ele_get_key(tbx_inip_element_t *ele) {
    return ((ele) == NULL) ? NULL : (ele)->key;
}

char *tbx_inip_ele_get_value(tbx_inip_element_t *ele) {
    return ((ele) == NULL) ? NULL : (ele)->value;
}

tbx_inip_element_t *tbx_inip_ele_next(tbx_inip_element_t *ele) {
    return ((ele) == NULL) ? NULL : (ele)->next;
}
tbx_inip_group_t *tbx_inip_group_first(tbx_inip_file_t *inip) {
    if (inip->n_substitution_checks > 0) apply_params(inip);  //** Apply the params if needed
    return inip->tree;
}
char *tbx_inip_group_get(tbx_inip_group_t *g) {
    return g->group;
}
tbx_inip_group_t *tbx_inip_group_next(tbx_inip_group_t *g) {
    return ((g) == NULL) ? NULL : (g)->next;
}
int tbx_inip_group_count(tbx_inip_file_t *inip) {
    return inip->n_groups;
}
void tbx_inip_group_free(tbx_inip_group_t *g) {
    free(g->group);
}
void tbx_inip_group_set(tbx_inip_group_t *ig, char *value) {
    ig->group = value;
}

//***********************************************************************
// substitute_params - Performs the parameter substitution. If a
//     substitution occurred a pointer to a newly malloced string is
//     returned.  Otherwise NULL is returns.
//***********************************************************************

char *substitute_params(tbx_inip_file_t *fd, char *text)
{
    char *start, *end, *last, *dest, *c, *value, *newtext, *textend;
    char param[1024], subtext[1024];
    int n, ndest, nmax, changed;

    nmax = sizeof(subtext);
    textend = text + strlen(text);
    dest = subtext;
    last = text;
    changed = 0;
    ndest = 0;
    do {
        value = NULL;
        start = index(last, VAR_DECLARE);
        if (start == NULL) {
            if (ndest == 0) return(NULL);  //** Nothing done
            start = textend;
            end = textend-1;
            goto finished;
        }
        if (start > text) { //** check if we have an escape char
            if (start[-1] == '\\') {
                end = start++;
                goto finished;
            }
        }

        //** Do another substitution
        //** Next character should be '{'
        if (start[1] != '{') {
            fprintf(stderr, "ERROR: Missing { from substitution: %s\n", text);
            start = textend;
            end = textend-1;
            goto finished;
        }

        //** Now scan for the '}'
        end = NULL;
        n = 0;
        for (c=start+2; c[0] != 0; c++) {
            if (c[0] == '}') {
                end = c;
                break;
            }
            n++;
        }
        if (end == NULL) {
            fprintf(stderr, "ERROR: Missing } from substitution: %s\n", text);
            start = textend;
            end = textend-1;
            goto finished;
        } else if (end[0] != '}') {
            fprintf(stderr, "ERROR: Missing } from substitution: %s\n", text);
            start = textend;
            end = textend-1;
            goto finished;
        }

        //** Copy the parameter over
        strncpy(param, start+2, n);
        param[n] = 0;

        //** Look up the parameter
        value = tbx_inip_get_string(fd, PARAMS, param, "oops!arg!is!missing");

finished:
        n = ndest + start - last + 1;
        if (value) n += strlen(value);
        if (n > nmax) {
            nmax = 2*n;
            dest = realloc(dest, nmax);
        }
        //** Copy the prefix
        n = start - last;
        strncpy(dest+ndest, last, n);
        ndest += n;
        dest[ndest] = 0;

        //** Do the substitution
        if (value) {
            //** And the substitution
            strncpy(dest+ndest, value, nmax-ndest);
            ndest += strlen(value);
            dest[ndest] = 0;
            free(value);
            changed = 1;
        }

        last = end+1;
    } while (last[0] != 0);

    newtext = (changed) ? strdup(dest) : NULL;
    if (dest != subtext) free(dest);

    return(newtext);
}

//***********************************************************************
// resolve_params - Resolves the parameters
//***********************************************************************

void resolve_params(tbx_inip_file_t  *fd)
{
    int loop, n;
    char *str;
    tbx_inip_group_t  *g;
    tbx_inip_element_t  *ele;

    //** Find the params section
    for (g = tbx_inip_group_first(fd); g != NULL; g = tbx_inip_group_next(g)) {
        if (strcmp(g->group, PARAMS) == 0) break;
    }

    //** We need to loop over the params repeatedly reapplying the substitions until
    //** nothing is done or we encounter a loop.  Fixed the number of iterations
    //** is an easy way to detect a loop:)
    for (loop=0; loop<20; loop++) {
        n = 0;
        for (ele = tbx_inip_ele_first(g); ele != NULL; ele = tbx_inip_ele_next(ele)) {
            if ((str = substitute_params(fd, ele->key)) != NULL) {
                n++;
                free(ele->key);
                ele->key = str;
            }
            if ((str = substitute_params(fd, ele->value)) != NULL) {
                n++;
               free(ele->value);
               ele->value = str;
            }
        }

        if (n == 0) break;  //** Nothing done so kick out
    }
}


//***********************************************************************
// apply_params - Applies the parameters to all the groups and elements
//***********************************************************************

void apply_params(tbx_inip_file_t  *fd)
{
    tbx_inip_group_t  *g;
    tbx_inip_element_t  *ele;
    char *str;

    fd->n_substitution_checks = 0;  //** Flag that we've applied the param substitutions

    resolve_params(fd);

    for (g = tbx_inip_group_first(fd); g != NULL; g = tbx_inip_group_next(g)) {
        if (g->substitution_check) {
            if ((str = substitute_params(fd, g->group)) != NULL) {
                free(g->group);
                g->group = str;
            }
        }

        if (g->n_kv_substitution_check) {
            for (ele = tbx_inip_ele_first(g); ele != NULL; ele = tbx_inip_ele_next(ele)) {
                if (ele->substitution_check) {
                    if ((str = substitute_params(fd, ele->key)) != NULL) {
                        free(ele->key);
                        ele->key = str;
                    }
                    if ((str = substitute_params(fd, ele->value)) != NULL) {
                        free(ele->value);
                        ele->value = str;
                    }
                }
            }
        }
    }

    fd->n_substitution_checks = 0;  //** No need to run this again
}

//***********************************************************************
// bfile_fopen - Opens the requested file scanning the include paths
//     if the file doesn't exist in the CWD and is not an absolute path.
//     IF the file cannot be found NULL is returned.
//***********************************************************************

FILE *bfile_fopen(tbx_stack_t *include_paths, char *fname)
{
    FILE *fd;
    char fullpath[BUFMAX];
    char *path;

    if (fname[0] == '/') { //** Absolute path so just try and open it
        return(fopen(fname, "r"));
    }

    //** Relative path so cycle through the prefixes
    tbx_stack_move_to_top(include_paths);
    while ((path = tbx_stack_get_current_data(include_paths)) != NULL) {
        snprintf(fullpath, BUFMAX, "%s/%s", path, fname);
        fd = fopen(fullpath, "r");
        log_printf(15, "checking path=%s fname=%s fullpath=%s fd=%p\n", path, fname, fullpath, fd);
        if (fd != NULL) return(fd);   //** Found it so kick out

        tbx_stack_move_down(include_paths);
    }

    return(NULL);
}

//***********************************************************************
// bfile_cleanup - Clean's up and closes all the open files on error
//***********************************************************************

void bfile_cleanup(tbx_stack_t *stack)
{
    bfile_entry_t *entry;

    if (!stack) return;

    while ((entry = (bfile_entry_t *)tbx_stack_pop(stack)) != NULL) {
        if (entry->fd) fclose(entry->fd);
        free(entry);
    }
}

//***********************************************************************
//  _fetch_text - Fetches the next line of text.
//***********************************************************************

char *_fetch_text(char *buf, int bsize, bfile_entry_t *entry)
{
    char *c = entry->text_pos;
    int i;

    if (entry->text_pos == NULL) return(NULL);

    for (i=0; i<bsize-1; i++) {
        buf[i] = *c;
        if (*c == 0) {
           entry->text_pos = NULL;
           return(buf);
        } else if  (*c == '\n') {
           buf[i+1] = 0;  //** Make sure to NULL terminate
           c++;  //** Skip over the '\n'
           entry->text_pos = c;  //** Update the position
           return(buf);
        }

        c++;
    }

    return(NULL);
}

//***********************************************************************
// _get_line - Reads a line of text from the file
//***********************************************************************

char * _get_line(bfile_t *bfd, int *err)
{
    bfile_entry_t *entry;
    char *comment;
    char *fname, *last;
    int fin;

    *err = 0;
    if (bfd->curr == NULL) return(NULL);

    if (bfd->curr->used == 1) return(bfd->curr->buffer);

again:
    comment = (bfd->curr->text) ? _fetch_text(bfd->curr->buffer, BUFMAX, bfd->curr) : fgets(bfd->curr->buffer, BUFMAX, bfd->curr->fd);
    log_printf(15, "_get_line: fgets=%s\n", comment);

    if (comment == NULL) {  //** EOF or error
        if (bfd->curr->fd) fclose(bfd->curr->fd);
        free(bfd->curr);

        bfd->curr = (bfile_entry_t *) tbx_stack_pop(bfd->stack);
        if (bfd->curr == NULL) {
            return(NULL);   //** EOF or Error
        } else {
            return(_get_line(bfd, err));
        }
    }

    //** Remove any comments
    comment = tbx_stk_escape_strchr('\\', bfd->curr->buffer, '#');
    if (comment != NULL) comment[0] = '\0';

    if (strncmp(bfd->curr->buffer, "%include ", 9) == 0) { //** In include so open and recurse
        fname = tbx_stk_string_token(&(bfd->curr->buffer[8]), " \n", &last, &fin);
        log_printf(10, "_get_line: Opening include file %s\n", fname);

        tbx_type_malloc(entry, bfile_entry_t, 1);
        entry->fd = bfile_fopen(bfd->include_paths, fname);
        if (entry->fd == NULL) {  //** Can't open the file
            log_printf(-1, "_get_line: Problem opening include file !%s!\n", fname);
            fprintf(stderr, "_get_line: Problem opening include file !%s!\n", fname);
            free(entry);
            *err = -1;
            bfd->error = -1;
            return(NULL);
        }
        entry->used = 0;
        tbx_stack_push(bfd->stack, (void *)bfd->curr);
        bfd->curr = entry;

        return(_get_line(bfd, err));
    } else if (strncmp(bfd->curr->buffer, "%include_path ", 14) == 0) { //** Add an include path to the search list
        fname = tbx_stk_string_token(&(bfd->curr->buffer[13]), " \n", &last, &fin);
        log_printf(10, "_get_line: Adding include path %s\n", fname);
        tbx_stack_move_to_bottom(bfd->include_paths);
        tbx_stack_insert_below(bfd->include_paths, strdup(fname));
        goto again;  //** Loop back and get another line
    }

    log_printf(15, "_get_line: buffer=%s\n", bfd->curr->buffer);
    bfd->curr->used = 1;
    return(bfd->curr->buffer);
}


//***********************************************************************
// new_ele -Makes a new Key/value element
//***********************************************************************
tbx_inip_element_t *new_ele(char *key, char *val)
{
    tbx_inip_element_t *ele;

    tbx_type_malloc(ele,  tbx_inip_element_t, 1);
    ele->substitution_check = (index(key, VAR_DECLARE) == NULL) ? 0 : 1;
    ele->substitution_check += (index(val, VAR_DECLARE) == NULL) ? 0 : 1;
    ele->key = key;
    ele->value = val;;
    ele->next = NULL;
    return(ele);
}

//***********************************************************************
//  _parse_ele - Parses the element
//***********************************************************************

tbx_inip_element_t *_parse_ele(bfile_t *bfd)
{
    char *text, *key, *val, *last, *isgroup;
    int fin, err;
    tbx_inip_element_t *ele;

    while ((text = _get_line(bfd, &err)) != NULL) {
        isgroup = strchr(text, '[');  //** Start of a new group
        if (isgroup != NULL) {
            log_printf(15, "_parse_ele: New group! text=%s\n", text);
            return(NULL);
        }

        bfd->curr->used = 0;

        key = tbx_stk_string_token(text, " =\r\n", &last, &fin);
        log_printf(15, "_parse_ele: key=!%s!\n", key);
        if (fin == 0) {
            val = tbx_stk_string_token(NULL, " =\r\n", &last, &fin);

            ele = new_ele(strdup(key), (val ? strdup(val) : NULL));
            log_printf(15, "_parse_ele: key=%s value=%s\n", ele->key, ele->value);
            return(ele);
        }
    }

    log_printf(15, "_parse_ele: END text=%s\n", text);

    return(NULL);

}

//***********************************************************************
//  _parse_group - Parses the group
//***********************************************************************

void _parse_group(bfile_t *bfd, tbx_inip_group_t *group)
{
    tbx_inip_element_t *ele, *prev;

    ele = _parse_ele(bfd);
    group->n_kv_substitution_check += ele->substitution_check;
    prev = ele;
    group->list = ele;
    ele = _parse_ele(bfd);
    while (ele != NULL) {
        group->n_kv_substitution_check += ele->substitution_check;
        prev->next = ele;
        prev = ele;
        ele = _parse_ele(bfd);
    }
}

//***********************************************************************
// new_group
//***********************************************************************

tbx_inip_group_t *new_group(char *name)
{
    tbx_inip_group_t *g;

    tbx_type_malloc(g, tbx_inip_group_t, 1);
    g->substitution_check = (index(name, VAR_DECLARE) == NULL) ? 0 : 1;
    g->n_kv_substitution_check = g->substitution_check;
    g->group = name;
    g->list = NULL;
    g->next = NULL;
    return(g);
}

//***********************************************************************
//  _next_group - Retreives the next group from the input file
//***********************************************************************

tbx_inip_group_t *_next_group(bfile_t *bfd)
{
    char *text, *start, *end;
    tbx_inip_group_t *g;
    int err;

    while ((text = _get_line(bfd, &err)) != NULL) {
        bfd->curr->used = 0;

        start = strchr(text, '[');
        if (start != NULL) {   //** Got a match!
            end = strchr(start, ']');
            if (end == NULL) {
                printf("_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                fprintf(stderr, "_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                log_printf(0, "_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                bfd->error = 1;
                return(NULL);
            }

            end[0] = '\0';  //** Terminate the ending point
            start++;  //** Move the starting point to the next character

            text = tbx_stk_string_trim(start); //** Trim the whitespace
            g = new_group(strdup(text));
            log_printf(15, "_next_group: group=%s\n", g->group);
            _parse_group(bfd, g);
            return(g);
        }
    }

    return(NULL);
}

//***********************************************************************
//  _free_element - Frees an individual key/valure pair
//***********************************************************************

void _free_element(tbx_inip_element_t *ele)
{
    free(ele->key);
    free(ele->value);
    free(ele);
}

//***********************************************************************
//  _free_list - Frees memory associated with the list
//***********************************************************************

void _free_list(tbx_inip_element_t *ele)
{
    tbx_inip_element_t *next;

    while (ele != NULL) {
        next = ele->next;
        log_printf(15, "_free_list:   key=%s value=%s\n", ele->key, ele->value);
        _free_element(ele);
        ele = next;
    }

}


//***********************************************************************
//  _free_group - Frees the memory associated with a group structure
//***********************************************************************

void _free_group(tbx_inip_group_t *group)
{
    log_printf(15, "_free_group: group=%s\n", group->group);
    _free_list(group->list);
    free(group->group);
    free(group);
}

//***********************************************************************
// inip_destroy - Destroys an inip structure
//***********************************************************************

void tbx_inip_destroy(tbx_inip_file_t *inip)
{
    tbx_inip_group_t *group, *next;

    if (inip == NULL) return;

    //** See if we are the last reference before closing
    if (tbx_atomic_get(inip->ref_count) > 1) {
        tbx_atomic_dec(inip->ref_count);
        return;
    }

    group = inip->tree;
    while (group != NULL) {
        next = group->next;
        _free_group(group);
        group = next;
    }

    free(inip);

    return;
}

//***********************************************************************
//  _find_key - Scans the group for the given key and returns the last
//       matching element
//***********************************************************************

tbx_inip_element_t *_find_key(tbx_inip_group_t *group, const char *name)
{
    tbx_inip_element_t *ele, *found;

    if (group == NULL) return(NULL);

    found = NULL;
    for (ele = group->list; ele != NULL; ele = ele->next) {
        if (strcmp(ele->key, name) == 0) found = ele;
    }

    return(found);
}

//***********************************************************************
//  inip_find_key - Scans the group for the given key and returns the value
//***********************************************************************

char *tbx_inip_find_key(tbx_inip_group_t *group, const char *name)
{
    tbx_inip_element_t *ele;

    ele = _find_key(group, name);
    if (ele == NULL) return(NULL);

    return(ele->value);
}


//***********************************************************************
//  inip_find_group - Scans the tree for the given group and returns
//      the last one encountered
//***********************************************************************

tbx_inip_group_t *tbx_inip_group_find(tbx_inip_file_t *inip, const char *name)
{
    tbx_inip_group_t *group, *found;

    found = NULL;
    for (group = tbx_inip_group_first(inip); group != NULL; group = tbx_inip_group_next(group)) {
        if (strcmp(group->group, name) == 0) found = group;
    }

    return(found);
}

//***********************************************************************
//  _find_group_key - Returns the element pointer for the given group/key
//       combination
//***********************************************************************

tbx_inip_element_t *_find_group_key(tbx_inip_file_t *inip, const char *group_name, const char *key)
{
    log_printf(15, "_find_group_key: Looking for group=%s key=%s!\n", group_name, key);

    tbx_inip_group_t *g = tbx_inip_group_find(inip, group_name);
    if (g == NULL) {
        log_printf(15, "_find_group_key: group=%s key=%s Can't find group!\n", group_name, key);
        return(NULL);
    }

    log_printf(15, "_find_group_key: Found group=%s\n", group_name);

    tbx_inip_element_t *ele = _find_key(g, key);

    if (ele != NULL) {
        log_printf(15, "_find_group_key: group=%s key=%s value=%s\n", group_name, key, ele->value);
    }

    return(ele);
}

//***********************************************************************
// inip_get_* - Routines for getting group/key values
//***********************************************************************

int64_t tbx_inip_get_integer(tbx_inip_file_t *inip, const char *group, const char *key, int64_t def)
{
    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    return(tbx_stk_string_get_integer(ele->value));
}

uint64_t inip_get_unsigned_integer(tbx_inip_file_t *inip, const char *group, const char *key, uint64_t def)
{
    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    uint64_t n;
    sscanf(ele->value, LU, &n);
    return(n);
}

//***********************************************************************

double tbx_inip_get_double(tbx_inip_file_t *inip, const char *group, const char *key, double def)
{
    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    return(tbx_stk_string_get_double(ele->value));
}

//***********************************************************************

apr_time_t tbx_inip_get_time(tbx_inip_file_t *inip, const char *group, const char *key, char *def)
{
    char *value = def;
    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele != NULL) value = ele->value;

    return(tbx_stk_string_get_time(value));
}

//***********************************************************************

char *tbx_inip_get_string(tbx_inip_file_t *inip, const char *group, const char *key, char *def)
{
    char *sub = NULL;
    char *value = def;
    char *ret;

    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele != NULL) {
        value = ele->value;
    } else if (def != NULL) {
        sub = substitute_params(inip, def);
        value = (sub) ? sub : def;
    }

    if (value == NULL) return(NULL);
    ret = strdup(value);
    if (sub) free(sub);

    return(ret);
}


//***********************************************************************
//  inip_load - Loads the .ini file pointed to by the file descriptor
//    fd - File descriptor containing data to parse
//    text - Parse the data contained in the character array
//
//    NOTE:  Either fd or text showld be non-NULL but not both.
//***********************************************************************

tbx_inip_file_t *inip_load(FILE *fd, const char *text, const char *prefix)
{
    tbx_inip_file_t *inip;
    tbx_inip_group_t *group, *prev;
    bfile_t bfd;
    bfile_entry_t *entry;

    tbx_type_malloc_clear(entry, bfile_entry_t, 1);
    entry->fd = fd;
    entry->text = (char *)text;
    entry->text_pos = (char *)text;

    if (fd) rewind(fd);

    entry->used = 0;
    bfd.error = 0;
    bfd.curr = entry;
    bfd.stack = tbx_stack_new();
    bfd.include_paths = tbx_stack_new();
    tbx_stack_push(bfd.include_paths, strdup("."));  //** By default always look in the CWD 1st
    if (prefix) tbx_stack_push(bfd.include_paths, strdup(prefix));  //** By default always look in the CWD 1st

    tbx_type_malloc(inip, tbx_inip_file_t, 1);

    group = _next_group(&bfd);
    inip->tree = NULL;
    inip->n_groups = 0;
    tbx_atomic_set(inip->ref_count, 1);

    prev = NULL;
    while (group != NULL) {
        inip->n_substitution_checks += group->n_kv_substitution_check;
        if (inip->tree == NULL) inip->tree = group;
        if (prev != NULL) {
            prev->next = group;
        }
        prev = group;
        inip->n_groups++;

        group = _next_group(&bfd);
    }

    if (bfd.error != 0) {  //** Got an internal parsing error
        if (bfd.curr) tbx_stack_push(bfd.stack, bfd.curr);
        bfile_cleanup(bfd.stack);
        tbx_inip_destroy(inip);
        inip = NULL;
    }

    tbx_stack_free(bfd.stack, 1);
    tbx_stack_free(bfd.include_paths, 1);

    return(inip);
}

//***********************************************************************
//  tbx_inip_file_read - Reads a .ini file from disk
//***********************************************************************

tbx_inip_file_t *tbx_inip_file_read(const char *fname)
{
    FILE *fd;
    char *prefix;
    int i;

    log_printf(15, "Parsing file %s\n", fname);
    if(!strcmp(fname, "-")) {
        fd = stdin;
    } else {
        fd = fopen(fname, "r");
    }
    if (fd == NULL) {  //** Can't open the file
        log_printf(-1, "Problem opening file %s, errorno: %d\n", fname, errno);
        return(NULL);
    }

    prefix = realpath(fname, NULL);
    if (prefix != NULL) {
        for (i=strlen(prefix); i>0; i--) {
            if (prefix[i] == '/') {
                prefix[i] = '\0';
                break;
            }
        }
    }

    tbx_inip_file_t *ret = inip_load(fd, NULL, prefix);

    if (prefix) free(prefix);
    return ret;
}

//***********************************************************************
//  tbx_inip_string_read - Loads a string and parses it into a INI structure
//***********************************************************************
tbx_inip_file_t *tbx_inip_string_read(const char *text)
{
    return(inip_load(NULL, text, NULL));
}

//***********************************************************************
// inip_convert2string - Loads the file and resolves all dependencies
//     and converts it to a string.
//***********************************************************************

int inip_convert2string(FILE *fd_in, const char *text_in, char **text_out, int *nbytes, char *prefix)
{
    bfile_t bfd;
    bfile_entry_t *entry;
    char *text, *line;
    int n_total, n_max, n, err;

    tbx_type_malloc_clear(entry, bfile_entry_t, 1);
    entry->fd = fd_in;
    entry->text = (char *)text_in;
    entry->text_pos = (char *)text_in;

    if (fd_in) rewind(fd_in);

    entry->used = 0;
    bfd.curr = entry;
    bfd.stack = tbx_stack_new();
    bfd.error = 0;
    bfd.include_paths = tbx_stack_new();
    tbx_stack_push(bfd.include_paths, strdup("."));  //** By default always look in the CWD 1st
    if (prefix) tbx_stack_push(bfd.include_paths, strdup(prefix));  //** By default always look in the CWD 1st


    n_max = 10*1024;
    tbx_type_malloc(text, char, n_max);
    n_total = 0;
    while ((line = _get_line(&bfd, &err)) != NULL) {
        bfd.curr->used = 0;
        n = strlen(line);
        if ((n_total+n) > n_max) {
            n_max = 1.5*n_max;
            tbx_type_realloc(text, char, n_max);
        }

        memcpy(text + n_total, line, n);
        n_total += n;
    }

    if (bfd.error != 0) {  //** Got an internal parsing error
        if (bfd.curr) tbx_stack_push(bfd.stack, bfd.curr);
        bfile_cleanup(bfd.stack);
    }

    tbx_stack_free(bfd.stack, 1);
    tbx_stack_free(bfd.include_paths, 1);

    tbx_type_realloc(text, char, n_total+1);
    text[n_total] = '\0';
    if (err != 0) {
        free(text);
        text = NULL;
        n_total = 0;
    }

    *nbytes = n_total;
    *text_out = text;
    return(err);
}

//***********************************************************************
//  inip_file2string - Loads a INI file resolving dependencies and
//     converts it ot a string.
//***********************************************************************

int tbx_inip_file2string(const char *fname, char **text_out, int *nbytes)
{
    FILE *fd;
    char *prefix;
    int i;

    log_printf(15, "Parsing file %s\n", fname);

    *text_out = NULL; *nbytes = 0;
    if(!strcmp(fname, "-")) {
        fd = stdin;
    } else {
        fd = fopen(fname, "r");
    }
    if (fd == NULL) {  //** Can't open the file
        log_printf(-1, "Problem opening file %s, errorno: %d\n", fname, errno);
        return(-1);
    }

    prefix = realpath(fname, NULL);
    if (prefix != NULL) {
        for (i=strlen(prefix); i>0; i--) {
            if (prefix[i] == '/') {
                prefix[i] = '\0';
                break;
            }
        }
    }

    i = inip_convert2string(fd, NULL, text_out, nbytes, prefix);
    if (prefix) free(prefix);

    return(i);
}

//***********************************************************************
//  inip_text2string - Loads a INI file resolving dependencies and
//     converts it ot a string.
//***********************************************************************

int tbx_inip_text2string(const char *text, char **text_out, int *nbytes)
{
    return(inip_convert2string(NULL, text, text_out, nbytes, NULL));
}

//***********************************************************************
//  tbx_inip_serialize - Converts the INI file structure to a string
//***********************************************************************

char *tbx_inip_serialize(tbx_inip_file_t *fd)
{
    tbx_inip_group_t *group;
    tbx_inip_element_t *ele;
    char *text;
    int used, nbytes;

    used = 0;
    nbytes = 10*1024;
    tbx_type_malloc(text, char, nbytes);

    group = fd->tree;
    while (group != NULL) {
        tbx_alloc_append_printf(&text, &used, &nbytes, "[%s]\n", group->group);
        ele = group->list;
        while (ele) {
            tbx_alloc_append_printf(&text, &used, &nbytes, "%s=%s\n", ele->key, ele->value);
            ele = ele->next;
        }

        tbx_alloc_append_printf(&text, &used, &nbytes, "\n");
        group = group->next;
    }

    return(text);
}

//-----------------------------------------------------------------------
//           ------- Hint routines are below --------
//-----------------------------------------------------------------------

//***********************************************************************
// tbx_inip_hint_new - Creates a hint
//***********************************************************************

tbx_inip_hint_t *tbx_inip_hint_new(int op, char *section, int section_rank, char *key, int key_rank, char *value)
{
    tbx_inip_hint_t *h;

    tbx_type_malloc(h, tbx_inip_hint_t, 1);
    h->op = op;
    h->section = (section) ? strdup(section) : NULL;
    h->section_rank = section_rank;
    h->key = (key) ? strdup(key) : NULL;
    h->key_rank = key_rank;
    h->value = (value) ? strdup(value) : NULL;

    return(h);
}

//***********************************************************************
// tbx_inip_hint_destroy - Destroys a hint
//***********************************************************************

void tbx_inip_hint_destroy(tbx_inip_hint_t *h)
{
    if (h->section) free(h->section);
    if (h->key) free(h->key);
    if (h->value) free(h->value);
    free(h);
}

//***********************************************************************
// tbx_inip_hint_list_destroy - Destroys a hint list
//***********************************************************************

void tbx_inip_hint_list_destroy(tbx_stack_t *list)
{
    tbx_inip_hint_t *h;

    for (h = tbx_stack_top_first(list); h != NULL; h = tbx_stack_next_down(list)) {
        tbx_inip_hint_destroy(h);
    }

    tbx_stack_free(list, 0);
}

//***********************************************************************
// tbx_inip_hint_parse - Parses a hint string and creates a new hint
//    Format: section:rank/key:rank=value
//            section:rank
//***********************************************************************

tbx_inip_hint_t *tbx_inip_hint_parse(int op, char *text)
{
    tbx_inip_hint_t *h;
    char *base, *section, *key, *value, *bstate, *bs2, *tmp, *s;
    int srank, krank;

    base = strdup(text);

    h = 0;
    bstate = bs2 = NULL;  //** Compiler thinks this may be used uninitialized
    section = key = value = NULL;
    srank = krank = 0;
    if (index(text, '/') == NULL) {  //** section:rank
        section = strtok_r(base, ":", &bstate);
        if (!section) {
            log_printf(-1, "(1)ERROR parsing INI hint: %s\n", text);
            goto error;
        }
        tmp = strtok_r(NULL, ":", &bstate);
        srank = (tmp) ? atol(tmp) : 0;
    } else {
        s = strtok_r(base, "/", &bstate);
        if (index(s, ':') != NULL) {
            section = strtok_r(s, ":", &bs2);
            tmp = strtok_r(NULL, "/", &bs2);
            srank = (tmp) ? atol(tmp) : 0;
        } else {
            section = strdup(s);
        }

        if (!section) {
            log_printf(-1, "(2)ERROR parsing INI hint: %s\n", text);
            goto error;
        }

        //** Now process the key:rank=value
        s = strtok_r(NULL, "=", &bstate);
        if (index(s, ':') != NULL) {
            key = strtok_r(s, ":", &bs2);
            tmp = strtok_r(NULL, "=", &bs2);
            krank = (tmp) ? atol(tmp) : 0;
            value = strtok_r(NULL, "=", &bstate);
        } else {
            key = s;
            value = strtok_r(NULL, "=", &bstate);
        }
    }

    h = tbx_inip_hint_new(op, section, srank, key, krank, value);
error:
    if (section) free(section);
    free(base);
    return(h);
}

//***********************************************************************
// tbx_inip_hint_options_parse - Sift through the provided argv/argc
//    and pluck out the hint options compacting the argv array and adjust argc
//***********************************************************************

void tbx_inip_hint_options_parse(tbx_stack_t *list, char **argv, int *argc)
{
    int i, op, n, hit;
    tbx_inip_hint_t *h;

    n = 0;
    i = 0;
    while (i<*argc) {  //** Loop over all the args
        hit = 0;
        for (op=0; op<4; op++) {  //** See if we find a match
            if (strcmp(hint_ops[op], argv[i]) == 0) {  //** If so store it
                h = tbx_inip_hint_parse(op, argv[i+1]);
                tbx_stack_move_to_bottom(list);
                tbx_stack_insert_below(list, h);
                hit = 1;
                break;
            }
        }

        if (hit == 1) {  //** Got a hit
            i = i + 2;   //** so only update the arg index
        } else {  //** No hit
            if (n != i) argv[n] = argv[i];  //** Slide the args down if needed
            n++;   //** Update the new running total of args
            i++;   //** and the global arg position
        }
    }

    *argc = n;  //** Record the new arg count
    return;
}


//***********************************************************************
// hint_add - Applies the add hint
//***********************************************************************

int hint_add(tbx_inip_file_t *fd, tbx_inip_hint_t *h)
{
    int n, gr_match, kr_match;
    tbx_inip_group_t *g, *group, *pg, *gprev;
    tbx_inip_element_t *k, *key, *pk, *kprev;

    gr_match = kr_match = 0;

    //** Find the section
    group = pg = gprev = NULL;
    n = 0;
    for (g = fd->tree; g != NULL; g = g->next) {
        if (strcmp(g->group, h->section) == 0) {
            n++;
            group = g;
            gprev = pg;
            if ((n == h->section_rank) || (0 == h->section_rank)) {
                gr_match = 1;
                break;
            }
        }
        pg = g;
    }

    if (h->section_rank == -1) {
        gr_match = 1;
    }
    if (h->key == NULL) {
        if (((h->section_rank-1) == n) || (h->section_rank == -1)) {
             gr_match = 1;
             gprev = group;
             group = group->next;
        }
    }
    if ((gr_match == 0) && (h->section_rank == 0)) {  //** No match is Ok if an ADD operation
        gr_match = 1;
        group = NULL;
        gprev = pg;
    }

    if (gr_match == 0) return(1);  //** No match so kick out

    //** And the key
    key = pk = kprev = NULL;
    if ((h->key) && (group)) {
        n = 0;
        for (k = group->list; k != NULL; k = k->next) {
            if (strcmp(k->key, h->key) == 0) {
                n++;
                key = k;
                kprev = pk;
                if ((n == h->key_rank) || (0 == h->key_rank)) {
                    kr_match = 1;
                    break;
                }
            }
            pk = k;
        }
        if (kr_match == 0) {
            if (h->key_rank == 0) {  //** No match is Ok if an ADD operation
                kr_match = 1;
                key = NULL;
                kprev = pk;
            } else if (((h->key_rank-1) == n) || (h->key_rank == -1)) {
                kr_match = 1;
                kprev = key;
                key = key->next;
            }
        }

        if (kr_match == 0) return(1);  //** No match so kick out

    }

    if (!group) { //** No group so add it
        group = new_group(strdup(h->section));
        if (gprev) {
            gprev->next = group;
        } else {
            fd->tree = group;
        }
    } else if (!h->key) {
        g = new_group(strdup(h->section));
        gprev->next = g;
        g->next = group;
    }

    //** Make the new key
    if (h->key) {
        k = new_ele(strdup(h->key), (h->value ? strdup(h->value) : NULL));
        if (kprev) {
            kprev->next = k;
        } else {
            group->list = k;
        }
        k->next = key;
    }

    return(0);
}


//***********************************************************************
// hint_remove - Applies the remove hint
//***********************************************************************

int hint_remove(tbx_inip_file_t *fd, tbx_inip_hint_t *h, int check_only)
{
    int n, gr_match, kr_match;
    tbx_inip_group_t *g, *group, *pg, *gprev;
    tbx_inip_element_t *k, *key, *pk, *kprev;

    gr_match = kr_match = 0;

    //** Find the section
    group = pg = gprev = NULL;
    n = 0;
    for (g = fd->tree; g != NULL; g = g->next) {
        if (strcmp(g->group, h->section) == 0) {
            n++;
            group = g;
            gprev = pg;
            if ((n == h->section_rank) || (0 == h->section_rank)) {
                gr_match = 1;
                break;
            }
        }
        pg = g;
    }

    //** We found a match but maybe not the one we want
    if ((group) && (gr_match == 0) && (h->section_rank == -1)) gr_match = 1;

    if (gr_match == 0) return(1);  //** No match so kick out

    //** And the key
    key = pk = kprev = NULL;
    if ((h->key) && (group)) {
        n = 0;
        for (k = group->list; k != NULL; k = k->next) {
            if (strcmp(k->key, h->key) == 0) {
                n++;
                key = k;
                kprev = pk;
                if ((n == h->key_rank) || (0 == h->key_rank)) {
                    kr_match = 1;
                    break;
                }
            }
            pk = k;
        }
        if ((kr_match == 0) && (h->key_rank == -1) && (key)) kr_match = 1;

        if (kr_match == 0) return(1);  //** No match so kick out
    }

    if (check_only) return(0);

    if (h->key) {  //** Just delete the key
        if (kprev) {
            kprev->next = key->next;
        } else {
            group->list = key->next;
        }
        _free_element(key);
    } else {  //** Delete the entire group
        if (gprev) {
            gprev->next = group->next;
        } else {
            fd->tree = group->next;
        }
        _free_group(group);
    }

    return(0);
}

//***********************************************************************
// tbx_inip_hint_apply - Applies the hint to the current INI
//    file descriptor
//***********************************************************************

int tbx_inip_hint_apply(tbx_inip_file_t *fd, tbx_inip_hint_t *h)
{
    int n, err;

    //** Hints should all be applied before any queries on he INI file are done.
    //** This means the hints could contain parameters so save the state and
    //** fake the application
    n = fd->n_substitution_checks;
    fd->n_substitution_checks = 0;

    err = 1;
    switch(h->op) {
        case TBX_INIP_HINT_ADD:
            err = hint_add(fd, h);
            break;
        case TBX_INIP_HINT_REMOVE:
            err = hint_remove(fd, h, 0);
            break;
        case TBX_INIP_HINT_REPLACE:
            hint_remove(fd, h, 0);
            err = hint_add(fd,h);
            break;
        case TBX_INIP_HINT_DEFAULT:
            err = (hint_remove(fd, h, 1) != 0) ? hint_add(fd, h) : 0;
            break;
    }

    fd->n_substitution_checks = n;

    return(err);
}

//***********************************************************************
// tbx_inip_hint_list_apply - Applies the hint list to the current INI
//    file descriptor
//***********************************************************************

int tbx_inip_hint_list_apply(tbx_inip_file_t *fd, tbx_stack_t *list)
{
    tbx_inip_hint_t *h;
    int n;

    n = 0;
    for (h=tbx_stack_top_first(list); h!=NULL; h=tbx_stack_next_down(list)) {
        n += tbx_inip_hint_apply(fd, h);
    }
    return(n);
}

//***********************************************************************
// tbx_inip_print_hint_options - Prints the hint options format to the FD
//***********************************************************************

void tbx_inip_print_hint_options(FILE *fd)
{
    fprintf(fd, "    INI_OPTIONS - Override configuration options. Multiple options are allowed and applied in the order given.\n");
    fprintf(fd, "       --ini-hint-add  INI_ARG    - Add the option\n");
    fprintf(fd, "       --ini-hint-remove  INI_ARG - Remove the option\n");
    fprintf(fd, "       --ini-hint-replace INI_ARG - Replace the option if it exists otherwise add it\n");
    fprintf(fd, "       --ini-hint-default INI_ARG - Only add the option if it doesn't exist\n");
    fprintf(fd, "\n");
    fprintf(fd, "       INI_ARG format: section[:index]/key[:index]=value\n");
    fprintf(fd, "          section - INI Section name\n");
    fprintf(fd, "          key     - Key within the INI section\n");
    fprintf(fd, "          value   - Key value\n");
    fprintf(fd, "          index   - Which section or key to target. Since sections and keys are not unique\n");
    fprintf(fd, "                    this allows you select a specific one. The special value -1 insures the\n");
    fprintf(fd, "                    last option is selected with numbering starting at 1.\n");
    fprintf(fd, "\n");
}
