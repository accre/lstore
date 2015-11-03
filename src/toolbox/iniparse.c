#define _log_module_index 109

//#define _DISABLE_LOG 1

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "assert_result.h"
#include "fmttypes.h"
#include "type_malloc.h"
#include "iniparse.h"
#include "string_token.h"
#include "log.h"
#include "stack.h"

#define BUFMAX 8192

typedef struct {
    FILE *fd;
    char buffer[BUFMAX];
    int used;
} bfile_entry_t;

typedef struct {  //** Used for Reading the ini file
    bfile_entry_t *curr;
    Stack_t *stack;
    Stack_t *include_paths;
} bfile_t;

//***********************************************************************
// bfile_fopen - Opens the requested file scanning the include paths
//     if the file doesn't exist in the CWD and is not an absolute path.
//     IF the file cannot be found NULL is returned.
//***********************************************************************

FILE *bfile_fopen(Stack_t *include_paths, char *fname)
{
    FILE *fd;
    char fullpath[BUFMAX];
    char *path;

    if (fname[0] == '/') { //** Absolute path so just try and open it
        return(fopen(fname, "r"));
    }

    //** Relative path so cycle through the prefixes
    move_to_top(include_paths);
    while ((path = get_ele_data(include_paths)) != NULL) {
        snprintf(fullpath, BUFMAX, "%s/%s", path, fname);
        fd = fopen(fullpath, "r");
        log_printf(15, "checking path=%s fname=%s fullpath=%s fd=%p\n", path, fname, fullpath, fd);
        if (fd != NULL) return(fd);   //** Found it so kick out

        move_down(include_paths);
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
    log_printf(15, "_get_line: fgets=%s", comment);

    if (comment == NULL) {  //** EOF or error
        fclose(bfd->curr->fd);
        free(bfd->curr);

        bfd->curr = (bfile_entry_t *)pop(bfd->stack);
        if (bfd->curr == NULL) {
            return(NULL);   //** EOF or Error
        } else {
            return(_get_line(bfd));
        }
    }

    //** Remove any comments
    comment = escape_strchr('\\', bfd->curr->buffer, '#');
    if (comment != NULL) comment[0] = '\0';

    if (strncmp(bfd->curr->buffer, "%include ", 9) == 0) { //** In include them open and recurse
        fname = string_token(&(bfd->curr->buffer[8]), " \n", &last, &fin);
        log_printf(10, "_get_line: Opening include file %s\n", fname);

        type_malloc(entry, bfile_entry_t, 1);
        entry->fd = bfile_fopen(bfd->include_paths, fname);
        if (entry->fd == NULL) {  //** Can't open the file
            log_printf(1, "_get_line: Problem opening include file !%s!\n", fname);
            free(entry);
            abort();
        }
        entry->used = 0;
        push(bfd->stack, (void *)bfd->curr);
        bfd->curr = entry;

        return(_get_line(bfd));
    } else if (strncmp(bfd->curr->buffer, "%include_path ", 14) == 0) { //** Add an include path to the search list
        fname = string_token(&(bfd->curr->buffer[13]), " \n", &last, &fin);
        log_printf(10, "_get_line: Adding include path %s\n", fname);
        move_to_bottom(bfd->include_paths);
        insert_below(bfd->include_paths, strdup(fname));
    }

    log_printf(15, "_get_line: buffer=%s\n", bfd->curr->buffer);
    bfd->curr->used = 1;
    return(bfd->curr->buffer);
}

//***********************************************************************
//  _parse_ele - Parses the element
//***********************************************************************

inip_element_t *_parse_ele(bfile_t *bfd)
{
    char *text, *key, *val, *last, *isgroup;
    int fin;
    inip_element_t *ele;

    while ((text = _get_line(bfd)) != NULL) {
        isgroup = strchr(text, '[');  //** Start of a new group
        if (isgroup != NULL) {
            log_printf(15, "_parse_ele: New group! text=%s\n", text);
            return(NULL);
        }

        bfd->curr->used = 0;

        key = string_token(text, " =\r\n", &last, &fin);
        log_printf(15, "_parse_ele: key=!%s!\n", key);
        if (fin == 0) {
            val = string_token(NULL, " =\r\n", &last, &fin);

            type_malloc(ele,  inip_element_t, 1);

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

void _parse_group(bfile_t *bfd, inip_group_t *group)
{
    inip_element_t *ele, *prev;

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

inip_group_t *_next_group(bfile_t *bfd)
{
    char *text, *start, *end;
    inip_group_t *g;

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

            text = string_trim(start); //** Trim the whitespace
//        text = string_token(start, " ", &last, &i);
            type_malloc(g, inip_group_t, 1);
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

void _free_element(inip_element_t *ele)
{
    free(ele->key);
    free(ele->value);
    free(ele);
}

//***********************************************************************
//  _free_list - Frees memory associated with the list
//***********************************************************************

void _free_list(inip_element_t *ele)
{
    inip_element_t *next;

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

void _free_group(inip_group_t *group)
{
    log_printf(15, "_free_group: group=%s\n", group->group);
    _free_list(group->list);
    free(group->group);
    free(group);
}

//***********************************************************************
// inip_destroy - Destroys an inip structure
//***********************************************************************

void inip_destroy(inip_file_t *inip)
{
    inip_group_t *group, *next;
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

inip_element_t *_find_key(inip_group_t *group, const char *name)
{
    inip_element_t *ele;

    if (group == NULL) return(NULL);

    for (ele = group->list; ele != NULL; ele = ele->next) {
        if (strcmp(ele->key, name) == 0) return(ele);
    }

    return(NULL);
}

//***********************************************************************
//  inip_find_key - Scans the group for the given key and returns the value
//***********************************************************************

char *inip_find_key(inip_group_t *group, const char *name)
{
    inip_element_t *ele;

    ele = _find_key(group, name);
    if (ele == NULL) return(NULL);

    return(ele->value);
}


//***********************************************************************
//  inip_find_group - Scans the tree for the given group
//***********************************************************************

inip_group_t *inip_find_group(inip_file_t *inip, const char *name)
{
    inip_group_t *group;

    for (group = inip->tree; group != NULL; group = group->next) {
        if (strcmp(group->group, name) == 0) return(group);
    }

    return(NULL);
}

//***********************************************************************
//  _find_group_key - Returns the element pointer for the given group/key
//       combination
//***********************************************************************

inip_element_t *_find_group_key(inip_file_t *inip, const char *group_name, const char *key)
{
    log_printf(15, "_find_group_key: Looking for group=%s key=%s!\n", group_name, key);

    inip_group_t *g = inip_find_group(inip, group_name);
    if (g == NULL) {
        log_printf(15, "_find_group_key: group=%s key=%s Can't find group!\n", group_name, key);
        return(NULL);
    }

    log_printf(15, "_find_group_key: Found group=%s\n", group_name);

    inip_element_t *ele = _find_key(g, key);

    if (ele != NULL) {
        log_printf(15, "_find_group_key: group=%s key=%s value=%s\n", group_name, key, ele->value);
    }

    return(ele);
}

//***********************************************************************
// inip_get_* - Routines for getting group/key values
//***********************************************************************

int64_t inip_get_integer(inip_file_t *inip, const char *group, const char *key, int64_t def)
{
    inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    return(string_get_integer(ele->value));
}

uint64_t inip_get_unsigned_integer(inip_file_t *inip, const char *group, const char *key, uint64_t def)
{
    inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    uint64_t n;
    sscanf(ele->value, LU, &n);
    return(n);
}

//***********************************************************************

double inip_get_double(inip_file_t *inip, const char *group, const char *key, double def)
{
    inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele == NULL) return(def);

    return(string_get_double(ele->value));
}

//***********************************************************************

char *inip_get_string(inip_file_t *inip, const char *group, const char *key, char *def)
{
    char *value = def;
    inip_element_t *ele = _find_group_key(inip, group, key);
    if (ele != NULL) value = ele->value;

    if (value == NULL) return(NULL);

    return(strdup(value));
}


//***********************************************************************
//  inip_read_fd - Loads the .ini file pointed to by the file descriptor
//***********************************************************************

inip_file_t *inip_read_fd(FILE *fd)
{
    inip_file_t *inip;
    inip_group_t *group, *prev;
    bfile_t bfd;
    bfile_entry_t *entry;

    type_malloc_clear(entry, bfile_entry_t, 1);
    entry->fd = fd;

    rewind(fd);

    entry->used = 0;
    bfd.curr = entry;
    bfd.stack = new_stack();
    bfd.include_paths = new_stack();
    push(bfd.include_paths, strdup("."));  //** By default always look in the CWD 1st

    type_malloc(inip, inip_file_t, 1);

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

    while ((entry = (bfile_entry_t *)pop(bfd.stack)) != NULL) {
        fclose(entry->fd);
        free(entry);
    }

    free_stack(bfd.stack, 1);
    free_stack(bfd.include_paths, 1);

    return(inip);
}

//***********************************************************************
//  inip_read - Reads a .ini file
//***********************************************************************

inip_file_t *inip_read(const char *fname)
{
    FILE *fd;

    log_printf(15, "Parsing file %s\n", fname);

    fd = fopen(fname, "r");
    if (fd == NULL) {  //** Can't open the file
        log_printf(1, "Problem opening file %s\n", fname);
        return(NULL);
    }

    return(inip_read_fd(fd));
}

//***********************************************************************
//  inip_read_text - Converts a character array into a .ini file
//***********************************************************************

inip_file_t *inip_read_text(const char *text)
{
    FILE *fd = tmpfile();
    fprintf(fd, "%s\n", text);

    return(inip_read_fd(fd));
}
