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

#define _log_module_index 179

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "assert_result.h"
#include <jerasure/cauchy.h>
#include <jerasure/liberation.h>
#include <jerasure/reed_sol.h>
#include <jerasure/jerasure.h>
#include "raid4.h"
#include "erasure_tools.h"
#include "log.h"


const char *JE_method[N_JE_METHODS] = {"reed_sol_van", "reed_sol_r6_op", "cauchy_orig", "cauchy_good", "blaum_roth", "liberation", "liber8tion", "raid4"};

#define BLANK_CHAR '0'


//***************************************************************************
//  nearest_prime - Finds the nearest prime.  "Which" controls the prime returned.
//     if which == 0 the nearest prime to w is returned.
//     if which > 0 the prime is >= w.
//     if which < 0 the prime <= w.
//
//     NOTE: Adapted from the is_prime() included in Jerasure.
//           Only works for primes less than 257!
//***************************************************************************

int nearest_prime(int w, int which)
{
    int prime55[] = {2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,
                     73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,
                     181,191,193,197,199,211,223,227,229,233,239,241,251,257
                    };
    int i, d1, d2;

    for (i = 1; i < 55; i++) {
        if (w <= prime55[i]) {
            if (which > 0) {
                return(prime55[i]);
            } else if (which < 0) {
                return(prime55[i-1]);
            } else {
                d1 = w - prime55[i-1];
                d2 = prime55[i] - w;
                if (d1<d2) {
                    return(prime55[i-1]);
                } else {
                    return(prime55[i]);
                }
            }
        }
    }

    return(prime55[54]);
}

//***************************************************************************
// bread - Same as fread except if bytes are 0'ed if reach EOF
//***************************************************************************

int bread(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    int i;
    size_t n = fread(ptr, size, nmemb, stream);

    if (n != nmemb) {
        i = (nmemb - n)*size;
        memset(ptr + n*size, BLANK_CHAR, i);
    }

    return(nmemb);
}


//***************************************************************************
//===========================================================================
// Form coding matrix routines
//===========================================================================
//***************************************************************************

int dummy_coding_matrix(erasure_plan_t *plan)
{
    return(0);
}

int reed_sol_r6_op_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_matrix = reed_sol_r6_coding_matrix(plan->data_strips, plan->w);

    return(0);
}

//***************************************************************************

int reed_sol_van_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_matrix = reed_sol_vandermonde_coding_matrix(plan->data_strips, plan->parity_strips, plan->w);

    if (plan->encode_matrix == NULL) return(-1);

    return(0);
}

//***************************************************************************

int cauchy_orig_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_matrix = cauchy_original_coding_matrix(plan->data_strips, plan->parity_strips, plan->w);
    plan->encode_bitmatrix = jerasure_matrix_to_bitmatrix(plan->data_strips, plan->parity_strips, plan->w, plan->encode_matrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int cauchy_good_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_matrix = cauchy_good_general_coding_matrix(plan->data_strips, plan->parity_strips, plan->w);
    plan->encode_bitmatrix = jerasure_matrix_to_bitmatrix(plan->data_strips, plan->parity_strips, plan->w, plan->encode_matrix);

//log_printf(15, "plan->encode_bitmatrix=%p\n", plan->encode_bitmatrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int blaum_roth_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_bitmatrix = blaum_roth_coding_bitmatrix(plan->data_strips, plan->w);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}


//***************************************************************************

int liberation_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_bitmatrix = liberation_coding_bitmatrix(plan->data_strips, plan->w);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int liber8tion_form_coding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    plan->encode_bitmatrix = liber8tion_coding_bitmatrix(plan->data_strips);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}



//***************************************************************************
//===========================================================================
// Form encoding matrix routines
//===========================================================================
//***************************************************************************

int cauchy_orig_form_encoding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    cauchy_orig_form_coding_matrix(plan);

    plan->encode_schedule = jerasure_smart_bitmatrix_to_schedule(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int cauchy_good_form_encoding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  != NULL) return(0);  //** Already formed so skip step

    cauchy_good_form_coding_matrix(plan);
    plan->encode_schedule = jerasure_smart_bitmatrix_to_schedule(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix);

//log_printf(15, "plan->encode_schedule=%p\n", plan->encode_schedule);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int blaum_roth_form_encoding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    blaum_roth_form_coding_matrix(plan);
    plan->encode_schedule = jerasure_smart_bitmatrix_to_schedule(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}


//***************************************************************************

int liberation_form_encoding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    liberation_form_coding_matrix(plan);
    plan->encode_schedule = jerasure_smart_bitmatrix_to_schedule(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************

int liber8tion_form_encoding_matrix(erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->encode_bitmatrix  != NULL) return(0);  //** Already formed so skip step

    liber8tion_form_coding_matrix(plan);
    plan->encode_schedule = jerasure_smart_bitmatrix_to_schedule(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix);

    if (plan->encode_schedule == NULL) return(-1);

    return(0);
}

//***************************************************************************
//===========================================================================
// Routines that do the actual encoding
//===========================================================================
//***************************************************************************


void schedule_encode_block(erasure_plan_t *plan, char **ptr, int block_size)
{
    jerasure_schedule_encode(plan->data_strips, plan->parity_strips, plan->w, plan->encode_schedule,
                             ptr, &(ptr[plan->data_strips]), block_size, plan->packet_size);
}

//***************************************************************************

void matrix_encode_block(erasure_plan_t *plan, char **ptr, int block_size)
{
    jerasure_matrix_encode(plan->data_strips, plan->parity_strips, plan->w, plan->encode_matrix,
                           ptr, &(ptr[plan->data_strips]), block_size);
}

//***************************************************************************

void reed_sol_r6_op_encode_block(erasure_plan_t *plan, char **ptr, int block_size)
{
    reed_sol_r6_encode(plan->data_strips, plan->w,
                       ptr, &(ptr[plan->data_strips]), block_size);
}

//***************************************************************************

void raid4_op_encode_block(erasure_plan_t *plan, char **ptr, int block_size)
{
    raid4_encode(plan->data_strips, ptr, &(ptr[plan->data_strips]), block_size);
}

//***************************************************************************
// et_encode - Encodes the data using the given plan
//     fname - Input data file name
//     foffset - Starting offset for input
//     pname - Output parity file name
//     poffset - Starting offset for parity output
//     buffersize - Default buffersize.  If 0 it's automatically adjusted
//
//     Upon success returns 0 otherwise an error has occured.
//***************************************************************************

int et_encode(erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size)
{
    FILE *fd_file, *fd_parity;
    char **ptr, **data, **parity, *buffer;
    int i, j, block_size, bsize;
    long long int rpos, apos, bpos, ppos;

    //** Open the files
    fd_file = fopen(fname, "r");
    if (fd_file == NULL) {
        printf("et_encode: Error opening data file %s\n", fname);
        return(1);
    }

    fd_parity = fopen(pname, "r+");
    if (fd_parity == NULL) {
        fd_parity = fopen(pname, "w");  //** Doesn't exist so create it.
    }
    if (fd_parity == NULL) {
        printf("et_encode: Error opening parity file %s\n", pname);
        return(1);
    }

    plan->form_encoding_matrix(plan);  //** Form the matrix if needed

    //** Determine the buffersize
    i = (plan->data_strips+plan->parity_strips)*plan->w*plan->packet_size*plan->base_unit;
    if (buffer_size == 0) {
        buffer_size = 10*1024*1024;
    }

    j = buffer_size / i;
    if (j == 0) j = 1;  //** Buffer specified is too small so force it to be bigger
    buffer_size = j * i;
    block_size = buffer_size/(plan->data_strips + plan->parity_strips);

    //** allocate the buffer space
    ptr = (char **)malloc(sizeof(char *)*(plan->data_strips + plan->parity_strips));
    assert(ptr != NULL);

    buffer = (char *)malloc(sizeof(char)*buffer_size);
    assert(buffer != NULL);

    for (i=0; i < (plan->data_strips+plan->parity_strips); i++) {
        ptr[i] = &(buffer[i*block_size]);
    }
    data = &(ptr[0]);
    parity = &(ptr[plan->data_strips]);

    //** Perform the encoding
    apos = foffset;
    ppos = poffset;
    rpos = 0;
    log_printf(15, "et_encode: strip_size=%lld buffer_size=%d block_size=%d\n", plan->strip_size, buffer_size, block_size);
    while (rpos < plan->strip_size) {
        //** Adjust the block size if needed
        bsize = ((rpos+block_size) > plan->strip_size) ? plan->strip_size - rpos : block_size;

        log_printf(15, "et_encode: rpos = %lld strip_size=%lld buffer_size=%d block_size=%d bsize=%d\n", rpos, plan->strip_size, buffer_size, block_size, bsize);

        //** Read the data
        bpos = apos;
        for (i=0; i<plan->data_strips; i++) {
            fseek(fd_file, bpos, SEEK_SET);
            bread(data[i], 1, bsize, fd_file);
            bpos = bpos + plan->strip_size;
        }
        apos = apos + block_size;

        //** Perform the encoding
        plan->encode_block(plan, ptr, bsize);

        //** Store the parity
        bpos = ppos;
        for (i=0; i<plan->parity_strips; i++) {
            fseek(fd_parity, bpos, SEEK_SET);
            fwrite(parity[i], 1, bsize, fd_parity);
            bpos = bpos + plan->strip_size;
        }
        ppos = ppos + block_size;

        rpos = rpos + block_size;
    }

    //** Free the ptrs
    free(ptr);
    free(buffer);

    fclose(fd_file);
    fclose(fd_parity);

    return(0);
}

//***************************************************************************
//===========================================================================
// Routines that do the actual decoding
//===========================================================================
//***************************************************************************

int matrix_decode_block(erasure_plan_t *plan, char **ptr, int block_size, int *erasures)
{
    return(jerasure_matrix_decode(plan->data_strips, plan->parity_strips, plan->w, plan->encode_matrix, 1, erasures,
                                  ptr, &(ptr[plan->data_strips]), block_size));
}

//***************************************************************************

int schedule_decode_block(erasure_plan_t *plan, char **ptr, int block_size, int *erasures)
{
    return(jerasure_schedule_decode_lazy(plan->data_strips, plan->parity_strips, plan->w, plan->encode_bitmatrix,
                                         erasures, ptr, &(ptr[plan->data_strips]), block_size, plan->packet_size, 1));
}

//***************************************************************************

int raid4_op_decode_block(erasure_plan_t *plan, char **ptr, int block_size, int *erasures)
{
    return(raid4_decode(plan->data_strips, erasures, ptr, &(ptr[plan->data_strips]), block_size));
}

//***************************************************************************
// et_decode - Decodes the data using the given plan
//     fsize - Total file size
//     fname - Input data file name
//     foffset - Starting offset for input
//     pname - Output parity file name
//     poffset - Starting offset for parity output
//     buffersize - Default buffersize.  If 0 it's automatically adjusted
//     erasures   - Array listing the missing blocks.  Each slot corresponds
//         to a missing data or parity block.  The numbering assumes the data
//         blocks come first followed by the parity. An index of -1
//         terminates the list.
//
//     Upon success returns 0 otherwise an error has occured.
//***************************************************************************

int et_decode(erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures)
{
    FILE *fd_file, *fd_parity;
    char **ptr, **data, **parity, *buffer;
    int i, j, block_size, bsize, missing[plan->data_strips+plan->parity_strips];
    int *missing_data, *missing_parity;
    long long int rpos, apos, bpos, ppos;

    //** Make the missing tables
    memset(missing, 0, sizeof(long)*(plan->data_strips+plan->parity_strips));
    missing_data = missing;
    missing_parity = &(missing[plan->data_strips]);
    i = 0;
    while (erasures[i] != -1) {
        missing[erasures[i]] = 1;
        i++;
    }

    if (i == 0) return(0);  //**Nothing missing so exit

    //** Open the files
    fd_file = fopen(fname, "r+");
    if (fd_file == NULL) {
        printf("et_decode: Error opening data file %s\n", fname);
        return(1);
    }

    fd_parity = fopen(pname, "r+");
    if (fd_parity == NULL) {
        printf("et_decode: Error opening parity file %s\n", pname);
        return(1);
    }

    plan->form_decoding_matrix(plan);  //** Form the matrix if needed

    //** Determine the buffersize
    i = (plan->data_strips+plan->parity_strips)*plan->w*plan->packet_size*plan->base_unit;
    if (buffer_size == 0) {
        buffer_size = 10*1024*1024;
    }

    j = buffer_size / i;
    if (j == 0) j = 1;  //** Buffer specified is too small so force it to be bigger
    buffer_size = j * i;
    block_size = buffer_size/(plan->data_strips + plan->parity_strips);

    //** allocate the buffer space
    ptr = (char **)malloc(sizeof(char *)*(plan->data_strips + plan->parity_strips));
    assert(ptr != NULL);

    buffer = (char *)malloc(sizeof(char)*buffer_size);
    assert(buffer != NULL);

    for (i=0; i < (plan->data_strips+plan->parity_strips); i++) {
        ptr[i] = &(buffer[i*block_size]);
    }
    data = &(ptr[0]);
    parity = &(ptr[plan->data_strips]);

    //** Perform the decoding
    apos = foffset;
    ppos = poffset;
    rpos = 0;
    log_printf(1, "et_decode: strip_size=%lld buffer_size=%d block_size=%d\n", plan->strip_size, buffer_size, block_size);
    while (rpos < plan->strip_size) {
        //** Adjust the block size if needed
        bsize = ((rpos+block_size) > plan->strip_size) ? plan->strip_size - rpos : block_size;

        log_printf(1, "et_decode: rpos = %lld strip_size=%lld buffer_size=%d block_size=%d bsize=%d\n", rpos, plan->strip_size, buffer_size, block_size, bsize);

        //** Read the data
        bpos = apos;
        for (i=0; i<plan->data_strips; i++) {
            if (missing_data[i] == 0) {
                fseek(fd_file, bpos, SEEK_SET);
                bread(data[i], 1, bsize, fd_file);
            }
            bpos = bpos + plan->strip_size;
        }

        bpos = ppos;   //**...and the parity
        for (i=0; i<plan->parity_strips; i++) {
            if (missing_parity[i] == 0) {
                fseek(fd_parity, bpos, SEEK_SET);
                fread(parity[i], 1, bsize, fd_parity);
            }
            bpos = bpos + plan->strip_size;
        }

        //** Perform the decoding
        plan->decode_block(plan, ptr, bsize, erasures);

        //** Store the missing blocks
        bpos = apos;
        for (i=0; i<plan->data_strips-1; i++) {  //** Skip the last block
            if (missing_data[i] == 1) {
                fseek(fd_file, bpos, SEEK_SET);
                fwrite(data[i], 1, bsize, fd_file);
            }
            bpos = bpos + plan->strip_size;
        }
        //** Handle the last strip individually to handle truncation
        i = plan->data_strips-1;
        if (missing_data[i] == 1) {
            fseek(fd_file, bpos, SEEK_SET);
            bpos = (bpos + bsize) > fsize ? fsize - bpos : bsize;
            fwrite(data[i], 1, bpos, fd_file);
        }

//     bpos = ppos;
//     for (i=0; i<plan->parity_strips; i++) {
//         if (missing_parity[i] == 1) {
//            fseek(fd_parity, bpos, SEEK_SET);
//            fwrite(parity[i], 1, bsize, fd_parity);
//         }
//         bpos = bpos + plan->strip_size;
//     }

        //** Update the file positions
        ppos = ppos + block_size;
        apos = apos + block_size;
        rpos = rpos + block_size;
    }

    //** Free the ptrs
    free(ptr);
    free(buffer);

    fclose(fd_file);
    fclose(fd_parity);

    return(0);
}


//***************************************************************************
// et_new_plan - Creates a new erasure plan
//***************************************************************************

erasure_plan_t *et_new_plan(int method, long long int strip_size,
                            int data_strips, int parity_strips, int w, int packet_size, int base_unit)
{

    if (method >= N_JE_METHODS) {
        printf("et_new_plan: Invalid method!  method=%d\n", method);
        return(NULL);
    }

    erasure_plan_t *plan = (erasure_plan_t *)malloc(sizeof(erasure_plan_t));
    assert(plan != NULL);

    plan->method = method;
    plan->strip_size = strip_size;
    plan->data_strips = data_strips;
    plan->parity_strips = parity_strips;
    plan->w = w;
    plan->base_unit = base_unit;
    plan->packet_size = packet_size;
    plan->encode_matrix = NULL;
    plan->encode_bitmatrix = NULL;
    plan->encode_schedule = NULL;

    switch(method) {
    case REED_SOL_R6_OP:
        plan->form_encoding_matrix = reed_sol_r6_op_form_coding_matrix;
        plan->form_decoding_matrix = reed_sol_r6_op_form_coding_matrix;
        plan->encode_block = reed_sol_r6_op_encode_block;
        plan->decode_block = matrix_decode_block;
        break;
    case REED_SOL_VAN:
        plan->form_encoding_matrix = reed_sol_van_form_coding_matrix;
        plan->form_decoding_matrix = reed_sol_van_form_coding_matrix;
        plan->encode_block = matrix_encode_block;
        plan->decode_block = matrix_decode_block;
        break;
    case CAUCHY_ORIG:
        plan->form_encoding_matrix = cauchy_orig_form_encoding_matrix;
        plan->form_decoding_matrix = cauchy_orig_form_coding_matrix;
        plan->encode_block = schedule_encode_block;
        plan->decode_block = schedule_decode_block;
        break;
    case CAUCHY_GOOD:
//log_printf(15, "making cauchy_good plan\n");
        plan->form_encoding_matrix = cauchy_good_form_encoding_matrix;
        plan->form_decoding_matrix = cauchy_good_form_coding_matrix;
        plan->encode_block = schedule_encode_block;
        plan->decode_block = schedule_decode_block;
        break;
    case BLAUM_ROTH:
        plan->form_encoding_matrix = blaum_roth_form_encoding_matrix;
        plan->form_decoding_matrix = blaum_roth_form_coding_matrix;
        plan->encode_block = schedule_encode_block;
        plan->decode_block = schedule_decode_block;
        break;
    case LIBERATION:
        plan->form_encoding_matrix = liberation_form_encoding_matrix;
        plan->form_decoding_matrix = liberation_form_coding_matrix;
        plan->encode_block = schedule_encode_block;
        plan->decode_block = schedule_decode_block;
        break;
    case LIBER8TION:
        plan->form_encoding_matrix = liber8tion_form_encoding_matrix;
        plan->form_decoding_matrix = liber8tion_form_coding_matrix;
        plan->encode_block = schedule_encode_block;
        plan->decode_block = schedule_decode_block;
        break;
    case RAID4:
        plan->form_encoding_matrix = dummy_coding_matrix;
        plan->form_decoding_matrix = dummy_coding_matrix;
        plan->encode_block = raid4_op_encode_block;
        plan->decode_block = raid4_op_decode_block;
        break;
    default:
        printf("et_new_plan: invalid method!!!!!! method=%d\n", method);
        return(NULL);
    }

    return(plan);
}

//***************************************************************************
// et_destroy_plan - Destroys an erasure plan
//***************************************************************************

void et_destroy_plan(erasure_plan_t *plan)
{
    int i;

    if (plan->encode_matrix != NULL) free(plan->encode_matrix);
    if (plan->encode_bitmatrix != NULL) free(plan->encode_bitmatrix);

    if (plan->encode_schedule != NULL) {
        i=0;
        while (plan->encode_schedule[i][0] != -1) {
            free(plan->encode_schedule[i]);
            i++;
        }
        free(plan->encode_schedule[i]);

        free(plan->encode_schedule);
    }

    free(plan);
}

//***************************************************************************
//  et_method_type - Determines the corresponding method type for the string
//***************************************************************************

int et_method_type(char *meth)
{
    int i;

    for (i=0; i<N_JE_METHODS; i++) {
        if (strcasecmp(meth, JE_method[i]) == 0) return(i);
    }

    return(-1);
}

//***************************************************************************
//  et_generate_plan - Generates an erasure plan based on user supplied parameters
//      w, packet_low, or packet_high can be se3t to -1 and if so they parameters
//      are automatically set.
//***************************************************************************

erasure_plan_t *et_generate_plan(long long int file_size, int method,
                                 int data_strips, int parity_strips, int w, int packet_low, int packet_high)
{
    int i, j, d, base_unit = 8;
    int packet_size;
    int packet_best;
    int plow, phigh;
    long long int strip_size, new_size, rem, smallest_excess, best_size;
    float increase;

    //** Set the default w and packetsize


    if (w == -1) { //** Auto set w
        switch(method) {
        case REED_SOL_R6_OP:
        case REED_SOL_VAN:
        case CAUCHY_ORIG:
        case CAUCHY_GOOD:
            w = 8;
            break;
        case BLAUM_ROTH:
            w = nearest_prime(data_strips+1, 1) - 1;
            break;
        case LIBERATION:
            w = nearest_prime(data_strips, 1);
            break;
        case LIBER8TION:
            w = 8;
            break;
        case RAID4:
            w = 8;   //** This isn't used
            base_unit = 1;
            break;
        default:
            printf("et_generate_plan: invalid method!!!!!! method=%d\n", method);
            return(NULL);
        }
    }

    //** Tweak the packet search range based on the params
    strip_size = file_size / (w*base_unit*data_strips);
    log_printf(15, "Approximate max packet_size=%lld\n", strip_size);
    if (strip_size < 4*1024) {
        plow = strip_size/4;
        phigh = strip_size;
//     plow = 16; phigh = 256;
//  } else if (strip_size < 10*1024) {
//     plow = 256; phigh = 736;
    } else {
        plow = 512;
        phigh = 4096;
    }
//plow = strip_size/4;
//phigh = strip_size;

    if (packet_low < 0) packet_low = plow;
    if (packet_high < 0) packet_high = phigh;
    if (packet_low > packet_high) {
        printf("et_generate_plan: packet_low > packet_high!  packet_low=%d packet_high=%d\n", packet_low, packet_high);
        return(NULL);
    }


    //** Override w and packet_size if needed
    if (packet_low % base_unit != 0) packet_low = (packet_low /base_unit) * base_unit;
    if (packet_high % base_unit != 0) packet_high = (packet_high /base_unit) * base_unit;
    packet_size = packet_high;

    //** Validate w and packet size
    switch(method) {
    case REED_SOL_R6_OP:
        if (parity_strips != 2) {
            printf("et_generate_plan: parity_strips must equal 2 for %s.  Specified parity_strips=%d\n", JE_method[method], parity_strips);
            return(NULL);
        }
    case REED_SOL_VAN:
    case CAUCHY_ORIG:
    case CAUCHY_GOOD:
        if ((w != 8) && (w != 16) && (w != 32)) {
            printf("et_generate_plan: For method %s w must be equal to 8, 16, or 32.\n", JE_method[method]);
            return(NULL);
        }
        break;
    case BLAUM_ROTH:
        if (data_strips > w) {
            printf("et_generate_plan: data_strips must be less than or equal to w! Specified data_strips=%d w=%d\n", data_strips, w);
            return(NULL);
        }
        i = nearest_prime(w+1, 0);
        if (i != (w+1)) {
            printf("et_generate_plan: w must be greater than two and w+1 must be prime. Specified w=%d\n", w);
            return(NULL);
        }
        if ((packet_size % sizeof(long)) != 0) {
            d = sizeof(long);
            printf("et_generate_plan: packetsize must be a multiple of sizeof(long).  Specified packet_size=%d sizeof(long)=%d\n", packet_size, d);
            return(NULL);
        }
        break;
    case LIBERATION:
        if (data_strips > w) {
            printf("et_generate_plan: data_strips must be less than or equal to w! Specified data_strips=%d w=%d\n", data_strips, w);
            return(NULL);
        }
        i = nearest_prime(w, 0);
        if (i != w) {
            printf("et_generate_plan: w must be greater than two and w+1 must be prime. Specified w=%d\n", w);
            return(NULL);
        }
        if ((packet_size % sizeof(long)) != 0) {
            d = sizeof(long);
            printf("et_generate_plan: packetsize must be a multiple of sizeof(long).  Specified packet_size=%d sizeof(long)=%d\n", packet_size, d);
            return(NULL);
        }
        break;
    case LIBER8TION:
        if (w != 8) {
            printf("et_generate_plan: w must equal 8 for LIBER8TION.  Specified w=%d\n", w);
            return(NULL);
        }
        if (parity_strips != 2) {
            printf("et_generate_plan: parity_strips must equal 2 for %s.  Specified parity_strips=%d\n", JE_method[method], parity_strips);
            return(NULL);
        }
        if (data_strips > w) {
            printf("et_generate_plan: data_strips must be less than or equal to w! Specified data_strips=%d w=%d\n", data_strips, w);
            return(NULL);
        }
        break;
    case RAID4:
        if (parity_strips != 1) {
            printf("et_generate_plan: parity_strips must equal 1 for %s.  Specified parity_strips=%d\n", JE_method[method], parity_strips);
            return(NULL);
        }
        base_unit = 1;
        packet_low = 0;
        packet_high = packet_low + base_unit;
        break;
    default:
        printf("et_generate_plan: Invalid method! Specified %d.", method);
        return(NULL);
    }


    //** Determine strip size
    log_printf(15, "packet_low=%d packet_high=%d\n", packet_low, packet_high);
    smallest_excess = 10*file_size;
    packet_best = -1;

//  for (packet_size = packet_low; packet_size < packet_high; packet_size = packet_size + base_unit) {
    for (packet_size = packet_high; packet_size > packet_low; packet_size = packet_size - base_unit) {
        i = data_strips*w*packet_size*base_unit;
        new_size = file_size;
        rem = new_size % i;
        if (rem > 0) new_size = new_size + (i-rem);
        strip_size = new_size / data_strips;
        rem = new_size % (data_strips*w*packet_size*base_unit);
        if (rem > 0) printf("ERROR with new size!!!!! new_Size=%lld rem=%lld\n", new_size, rem);

        j = new_size - file_size;
        if (j <= smallest_excess) {
            smallest_excess = j;
            packet_best = packet_size;
            best_size = new_size;
            increase = (1.0*j) / file_size * 100;
            if (increase < 1) break;
        }

//     increase = (1.0*j) / file_size * 100;
//     log_printf(15, "Divisor=%d New size=%lld Orig size=%lld increase=%d (%f\%)\n", i, new_size, file_size, j, increase);
    }

    packet_size = packet_best;
    new_size = best_size;
    strip_size = new_size / data_strips;
    i = data_strips*w*packet_size*base_unit;
    j = new_size - file_size;
    increase = (1.0*j) / file_size * 100;
    log_printf(15, "Best Divisor=%d New size=%lld Orig size=%lld increase=%d (%f\%)\n", i, new_size, file_size, j, increase);

    //** Store plan
    return(et_new_plan(method, strip_size, data_strips, parity_strips, w, packet_size, base_unit));
}
