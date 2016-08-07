#define _log_module_index 109

//#define _DISABLE_LOG 1

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tbx/assert_result.h"
#include "tbx/fmttypes.h"
#include "tbx/iniparse.h"
#include "tbx/log.h"
#include "tbx/stack.h"
#include "tbx/string_token.h"
#include "tbx/type_malloc.h"

#define BUFMAX 8192

typedef struct {
    FILE *fd;
    char buffer[BUFMAX];
    int used;
} bfile_entry_t;

typedef struct {  //** Used for Reading the ini file
    bfile_entry_t *curr;
    tbx_stack_t *stack;
    tbx_stack_t *include_paths;
} bfile_t;

struct tbx_inip_element_t {  //** Key/Value pair
    char *key;
    char *value;
    struct tbx_inip_element_t *next;
};

struct tbx_inip_group_t {  //** Group
    char *group;
    tbx_inip_element_t *list;
    struct tbx_inip_group_t *next;
};

struct tbx_inip_file_t {  //File
    tbx_inip_group_t *tree;
    int  n_groups;
};

// Accessors
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
// _get_line - Reads a line of text from the file
//***********************************************************************

char * _get_line(bfile_t *bfd)
{
    bfile_entry_t *entry;
    char *comment;
    char *fname, *last;
    int fin;

    if (bfd->curr == NULL) return(NULL);

    if (bfd->curr->used == 1) return(bfd->curr->buffer);

    comment = fgets(bfd->curr->buffer, BUFMAX, bfd->curr->fd);
    log_printf(15, "_get_line: fgets=%s\n", comment);

    if (comment == NULL) {  //** EOF or error
        fclose(bfd->curr->fd);
        free(bfd->curr);

        bfd->curr = (bfile_entry_t *) tbx_stack_pop(bfd->stack);
        if (bfd->curr == NULL) {
            return(NULL);   //** EOF or Error
        } else {
            return(_get_line(bfd));
        }
    }

    //** Remove any comments
    comment = tbx_stk_escape_strchr('\\', bfd->curr->buffer, '#');
    if (comment != NULL) comment[0] = '\0';

    if (strncmp(bfd->curr->buffer, "%include ", 9) == 0) { //** In include them open and recurse
        fname = tbx_stk_string_token(&(bfd->curr->buffer[8]), " \n", &last, &fin);
        log_printf(10, "_get_line: Opening include file %s\n", fname);

        tbx_type_malloc(entry, bfile_entry_t, 1);
        entry->fd = bfile_fopen(bfd->include_paths, fname);
        if (entry->fd == NULL) {  //** Can't open the file
            log_printf(-1, "_get_line: Problem opening include file !%s!\n", fname);
            FATAL_UNLESS(entry->fd != NULL);
        }
        entry->used = 0;
        tbx_stack_push(bfd->stack, (void *)bfd->curr);
        bfd->curr = entry;

        return(_get_line(bfd));
    } else if (strncmp(bfd->curr->buffer, "%include_path ", 14) == 0) { //** Add an include path to the search list
        fname = tbx_stk_string_token(&(bfd->curr->buffer[13]), " \n", &last, &fin);
        log_printf(10, "_get_line: Adding include path %s\n", fname);
        tbx_stack_move_to_bottom(bfd->include_paths);
        tbx_stack_insert_below(bfd->include_paths, strdup(fname));
    }

    log_printf(15, "_get_line: buffer=%s\n", bfd->curr->buffer);
    bfd->curr->used = 1;
    return(bfd->curr->buffer);
}

//***********************************************************************
//  _parse_ele - Parses the element
//***********************************************************************

tbx_inip_element_t *_parse_ele(bfile_t *bfd)
{
    char *text, *key, *val, *last, *isgroup;
    int fin;
    tbx_inip_element_t *ele;

    while ((text = _get_line(bfd)) != NULL) {
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

            tbx_type_malloc(ele,  tbx_inip_element_t, 1);

            ele->key = strdup(key);
            ele->value = (val == NULL) ? NULL : strdup(val);
            ele->next = NULL;

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
    prev = ele;
    group->list = ele;
    ele = _parse_ele(bfd);
    while (ele != NULL) {
        prev->next = ele;
        prev = ele;
        ele = _parse_ele(bfd);
    }
}

//***********************************************************************
//  _next_group - Retreives the next group from the input file
//***********************************************************************

tbx_inip_group_t *_next_group(bfile_t *bfd)
{
    char *text, *start, *end;
    tbx_inip_group_t *g;

    while ((text = _get_line(bfd)) != NULL) {
        bfd->curr->used = 0;

        start = strchr(text, '[');
        if (start != NULL) {   //** Got a match!
            end = strchr(start, ']');
            if (end == NULL) {
                printf("_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                fprintf(stderr, "_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                log_printf(0, "_next_group: ERROR: missing ] for group heading.  Parsing line: %s\n", text);
                abort();
                //return(NULL);
            }

            end[0] = '\0';  //** Terminate the ending point
            start++;  //** Move the starting point to the next character

            text = tbx_stk_string_trim(start); //** Trim the whitespace
//        text = tbx_stk_string_token(start, " ", &last, &i);
            tbx_type_malloc(g, tbx_inip_group_t, 1);
            g->group = strdup(text);
            g->list = NULL;
            g->next = NULL;
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
//  _find_key - Scans the group for the given key and returns the element
//***********************************************************************

tbx_inip_element_t *_find_key(tbx_inip_group_t *group, const char *name)
{
    tbx_inip_element_t *ele;

    if (group == NULL) return(NULL);

    for (ele = group->list; ele != NULL; ele = ele->next) {
        if (strcmp(ele->key, name) == 0) return(ele);
    }

    return(NULL);
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
//  inip_find_group - Scans the tree for the given group
//***********************************************************************

tbx_inip_group_t *tbx_inip_group_find(tbx_inip_file_t *inip, const char *name)
{
    tbx_inip_group_t *group;

    for (group = inip->tree; group != NULL; group = group->next) {
        if (strcmp(group->group, name) == 0) return(group);
    }

    return(NULL);
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

char *tbx_inip_get_string(tbx_inip_file_t *inip, const char *group, const char *key, char *def)
{
    char *value = def;
    tbx_inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele != NULL) value = ele->value;

    if (value == NULL) return(NULL);

    return(strdup(value));
}


//***********************************************************************
//  inip_read_fd - Loads the .ini file pointed to by the file descriptor
//***********************************************************************

tbx_inip_file_t *inip_read_fd(FILE *fd)
{
    tbx_inip_file_t *inip;
    tbx_inip_group_t *group, *prev;
    bfile_t bfd;
    bfile_entry_t *entry;

    tbx_type_malloc_clear(entry, bfile_entry_t, 1);
    entry->fd = fd;

    rewind(fd);

    entry->used = 0;
    bfd.curr = entry;
    bfd.stack = tbx_stack_new();
    bfd.include_paths = tbx_stack_new();
    tbx_stack_push(bfd.include_paths, strdup("."));  //** By default always look in the CWD 1st

    tbx_type_malloc(inip, tbx_inip_file_t, 1);

    group = _next_group(&bfd);
    inip->tree = NULL;
    inip->n_groups = 0;
    prev = NULL;
    while (group != NULL) {
        if (inip->tree == NULL) inip->tree = group;
        if (prev != NULL) {
            prev->next = group;
        }
        prev = group;
        inip->n_groups++;

        group = _next_group(&bfd);
    }



    if (bfd.curr != NULL) {
        fclose(bfd.curr->fd);
        free(bfd.curr);
    }

    while ((entry = (bfile_entry_t *) tbx_stack_pop(bfd.stack)) != NULL) {
        fclose(entry->fd);
        free(entry);
    }

    tbx_stack_free(bfd.stack, 1);
    tbx_stack_free(bfd.include_paths, 1);

    return(inip);
}

//***********************************************************************
//  inip_read - Reads a .ini file
//***********************************************************************

tbx_inip_file_t *tbx_inip_file_read(const char *fname)
{
    FILE *fd;
    bool is_stdin;

    log_printf(15, "Parsing file %s\n", fname);
    if(!strcmp(fname, "-")) {
        fd = stdin;
        is_stdin = true;
    } else {
        fd = fopen(fname, "r");
        is_stdin = false;
    }
    if (fd == NULL) {  //** Can't open the file
        log_printf(1, "Problem opening file %s\n", fname);
        return(NULL);
    }
    tbx_inip_file_t *ret = inip_read_fd(fd);
    if (!is_stdin) {
        //fclose(fd);
    }
    return ret;
}

//***********************************************************************
//  inip_read_text - Converts a character array into a .ini file
//***********************************************************************

tbx_inip_file_t *tbx_inip_string_read(const char *text)
{
    /* POSIX requires mkstemp sets the permissions of the resulting file to
     * 0600. Coverity assumes the much broader stance that mkstemp is influenced
     * by the current umask, so it complains that you need to set umask to 0000
     * before calling mkstemp. It doesn't really make sense anyway, you can't
     * safely set umask in a multithreaded application. Sinec it's only obscure
     * versions of libc that have the other behavior, just tell coverity to
     * ignore it
     */
    const char template[] = "tbx_inip_XXXXXX";
    char *template_copy = strdup(template);
    if (!template_copy) {
        goto error0;
    }
    // coverity[secure_temp]
    int file_temp = mkstemp(template_copy);
    if (file_temp == -1) {
        goto error1;
    }
    FILE *fd = fdopen(file_temp, "w+");
    if (!fd) {
        goto error2;
    }
    fprintf(fd, "%s\n", text);

    tbx_inip_file_t *ret = inip_read_fd(fd);
    // apparently inip_read_fd does frees on its own?
    //fclose(fd);
    free(template_copy);
    return ret;

error2:
    close(file_temp);
error1:
    free(template_copy);
error0:
    return NULL;
}
