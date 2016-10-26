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

#define _log_module_index 107

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include "chksum.h"
#include "tbx/transfer_buffer.h"

/*! @brief Given a binary string, return the hexidecimal representation
 *
 * @param in_size Length of input string
 * @param in Input string
 * @param out String to write to
 * @returns Always returns zero
 **/
int tbx_chksum_bin2hex(int in_size, const unsigned char *in, char *out);

/*! @brief Initializes cs to a completely blank checksum
 * @param cs Checksum struct to initialize
 * @returns Always returns zero
 */
int tbx_blank_chksum_set(tbx_chksum_t *cs);


#define CHKSUM_SHA1_LEN   (2*SHA1_DIGEST_LENGTH)
#define CHKSUM_SHA256_LEN (2*SHA256_DIGEST_LENGTH)
#define CHKSUM_SHA512_LEN (2*SHA512_DIGEST_LENGTH)
#define CHKSUM_MD5_LEN    (2*MD5_DIGEST_LENGTH)

#if defined(__APPLE__) && defined(__MACH__)
#  define COMMON_DIGEST_FOR_OPENSSL
#  include <CommonCrypto/CommonDigest.h>
#else
#  include <openssl/md5.h>
#  include <openssl/sha.h>
#endif

char *_bin2hex =  "00" "01" "02" "03" "04" "05" "06" "07" "08" "09" "0a" "0b" "0c" "0d" "0e" "0f"
                  "10" "11" "12" "13" "14" "15" "16" "17" "18" "19" "1a" "1b" "1c" "1d" "1e" "1f"
                  "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "2a" "2b" "2c" "2d" "2e" "2f"
                  "30" "31" "32" "33" "34" "35" "36" "37" "38" "39" "3a" "3b" "3c" "3d" "3e" "3f"
                  "40" "41" "42" "43" "44" "45" "46" "47" "48" "49" "4a" "4b" "4c" "4d" "4e" "4f"
                  "50" "51" "52" "53" "54" "55" "56" "57" "58" "59" "5a" "5b" "5c" "5d" "5e" "5f"
                  "60" "61" "62" "63" "64" "65" "66" "67" "68" "69" "6a" "6b" "6c" "6d" "6e" "6f"
                  "70" "71" "72" "73" "74" "75" "76" "77" "78" "79" "7a" "7b" "7c" "7d" "7e" "7f"
                  "80" "81" "82" "83" "84" "85" "86" "87" "88" "89" "8a" "8b" "8c" "8d" "8e" "8f"
                  "90" "91" "92" "93" "94" "95" "96" "97" "98" "99" "9a" "9b" "9c" "9d" "9e" "9f"
                  "a0" "a1" "a2" "a3" "a4" "a5" "a6" "a7" "a8" "a9" "aa" "ab" "ac" "ad" "ae" "af"
                  "b0" "b1" "b2" "b3" "b4" "b5" "b6" "b7" "b8" "b9" "ba" "bb" "bc" "bd" "be" "bf"
                  "c0" "c1" "c2" "c3" "c4" "c5" "c6" "c7" "c8" "c9" "ca" "cb" "cc" "cd" "ce" "cf"
                  "d0" "d1" "d2" "d3" "d4" "d5" "d6" "d7" "d8" "d9" "da" "db" "dc" "dd" "de" "df"
                  "e0" "e1" "e2" "e3" "e4" "e5" "e6" "e7" "e8" "e9" "ea" "eb" "ec" "ed" "ee" "ef"
                  "f0" "f1" "f2" "f3" "f4" "f5" "f6" "f7" "f8" "f9" "fa" "fb" "fc" "fd" "fe" "ff";

char *_chksum_name[] = { "NONE", "SHA256", "SHA512", "SHA1", "MD5" };
char *_chksum_name_default = "DEFAULT";

//**********************************************************************
//
//  tbx_chksum_bin2hex - Converts a binary string to Hex. "out" should be at
//      least 2*in_size+1.
//
//**********************************************************************

int tbx_chksum_bin2hex(int in_size, const unsigned char *in, char *out)
{
    int i, j;

    j = 0;
    for (i=0; i<in_size; i++) {
        out[j] = _bin2hex[2*in[i]];
        out[j+1] = _bin2hex[2*in[i]+1];
        j = j + 2;
    }

    out[j] = '\0';

    return(0);

}

//**********************************************************************
// chksum_name_type - Returns the corresponding int chskum type
//    or -2 if invalid
//**********************************************************************

int tbx_chksum_type_name(const char *name)
{
    int i;

    for (i=0; i<CHKSUM_MAX_TYPE; i++) {
        if (strcasecmp(name, _chksum_name[i]) == 0) return(i);
    }

    if (strcasecmp(name, _chksum_name_default) == 0) return(i);

    return(-2);
}


//**********************************************************************
//  chksum_valid_type
//    Returns 1 if the type is valid and 0 otherwise
//**********************************************************************

int tbx_chksum_type_valid(tbx_chksum_type_t type)
{
    if ((type > 0) && (type < CHKSUM_MAX_TYPE)) return(1);

    return(0);
}


//**********************************************************************
//  openssl chksum macros
//**********************************************************************

#define _openssl_chksum(CIPHER, cipher) \
int cipher ## _reset(void *state) { return(CIPHER ## _Init((CIPHER ## _CTX *)state)); } \
                                            \
int cipher ## _size(void *state, tbx_chksum_digest_output_t type)  \
{                                           \
  int i = -1;                               \
                                            \
  switch (type) {                           \
    case CHKSUM_DIGEST_BIN:  i = CIPHER ## _DIGEST_LENGTH; break;       \
    case CHKSUM_DIGEST_HEX:  i = CHKSUM_ ## CIPHER ## _LEN; break;   \
  }                                         \
                                            \
  return(i);                                \
}                                           \
                                            \
int cipher ## _add(void *state, int nbytes, tbx_tbuf_t *data, int boff)   \
{                                                  \
  int i, err = -1;                                 \
  size_t nleft;                                    \
  tbx_iovec_t *iov;                                \
  tbx_tbuf_var_t *tbv;                             \
  tbv = (tbx_tbuf_var_t*) malloc(tbx_tbuf_var_size()); \
  if (!tbv) { \
      return -1; \
  } \
  tbx_tbuf_var_init(tbv);                          \
                                                   \
  nleft = nbytes;                                  \
  while (nbytes > 0) {                             \
     tbx_tbuf_var_nbytes_set(tbv, nleft);          \
     i = tbx_tbuf_next_block(data, boff, tbv);    \
     iov = tbx_tbuf_var_buffer_get(tbv);          \
     if (i != TBUFFER_OK) { \
         free(tbv); \
         return(0); \
     } \
     int n_iov = tbx_tbuf_var_n_iov_get(tbv);      \
     for (i=0; i<n_iov; i++) {                 \
        err = CIPHER ## _Update((CIPHER ## _CTX *)state, iov[i].iov_base, iov[i].iov_len); \
        nleft = nleft - iov[i].iov_len;            \
        boff = boff + iov[i].iov_len;              \
        if (nleft <= 0) { \
            free(tbv); \
            return(err);               \
        } \
    }                                              \
  }                                                \
  free(tbv);                                       \
  return(err);                                     \
}                                                  \
                                                   \
int cipher ## _get(void *state, tbx_chksum_digest_output_t type, char *data)         \
{                                                             \
  unsigned char md[CHKSUM_ ## CIPHER ## _LEN + 1];             \
  CIPHER ## _CTX s2;                        \
  int i = -1;                               \
                                            \
  memcpy(&s2, state, sizeof(CIPHER ## _CTX));\
                                            \
  switch (type) {                           \
    case CHKSUM_DIGEST_BIN:                 \
        i = CIPHER ## _Final((unsigned char *)data, &s2); \
        break;       \
    case CHKSUM_DIGEST_HEX:                 \
        i = CIPHER ## _Final((unsigned char *)md, &s2); \
        if (i) {                            \
            break;                          \
        }                                   \
        i = tbx_chksum_bin2hex(CIPHER ## _DIGEST_LENGTH, md, data);            \
        break;       \
  }                                         \
                                            \
  return(i);                                \
}                                                                 \
                                                                  \
int cipher ## _set(tbx_chksum_t *cs)                                  \
{                                                                 \
  int i = sizeof( CIPHER ## _CTX);                                \
  if (i > CHKSUM_STATE_SIZE) {                                    \
     printf( #cipher "_set: sizeof(" #CIPHER "_CTX)=%d and is bigger than CHKSUM_STATE_SIZE=%d!\n", i, CHKSUM_STATE_SIZE); \
     printf( #cipher "_set: Increase the size of CHKSUM_STATE_SIZE in chksum.h and recompile.\n"); \
     printf( #cipher "NOTE from AMM (4/26/2016) TERRIBLE IDEA - this breaks ABI\n"); \
     fflush(stdout);                                              \
     abort();                                                     \
  }                                                               \
  cs->reset = cipher ## _reset;                                   \
  cs->size = cipher ## _size;                                     \
  cs->add = cipher ## _add;                                       \
  cs->get = cipher ## _get;                                       \
  cs->type = CHKSUM_ ## CIPHER;                                   \
                                                                  \
  memset(cs->state, 0, CHKSUM_STATE_SIZE);                        \
  cs->reset(cs->state);                                           \
  return(0);                                                  \
}

//*************************************************************************
//  Define all the chksum Ciphers
//*************************************************************************

//** OpenSSL uses a slightly different naming scheme than SHA256 or SHA512
#define SHA1_CTX SHA_CTX
#define SHA1_DIGEST_LENGTH SHA_DIGEST_LENGTH
_openssl_chksum(SHA1, sha1)
_openssl_chksum(SHA256, sha256)
_openssl_chksum(SHA512, sha512)
_openssl_chksum(MD5, md5)

//*************************************************************************
// blank chksum dummy routines
//*************************************************************************

int blank_reset(void *state)
{
    return(0);
}
int blank_size(void *state, tbx_chksum_digest_output_t type)
{
    return(0);
}
int blank_get(void *state, tbx_chksum_digest_output_t type, char *value)
{
    value[0] = '\0';
    return(0);
}
int blank_add(void *state, int type, tbx_tbuf_t *data, int boff)
{
    return(0);
}

//*************************************************************************
//  blank_chksum_set - makes a blank chksum
//*************************************************************************

int tbx_blank_chksum_set(tbx_chksum_t *cs)
{
    cs->reset = blank_reset;
    cs->size = blank_size;
    cs->add = blank_add;
    cs->get = blank_get;
    cs->type = CHKSUM_NONE;
    cs->name = _chksum_name[CHKSUM_NONE];

    return(0);
}

//*************************************************************************
//
//  chksum_set - Configures a chksum for use
//
//*************************************************************************

int tbx_chksum_set(tbx_chksum_t *cs, tbx_chksum_type_t tbx_chksum_type)
{
    int i = -1;

    cs->type = CHKSUM_NONE;

    switch (tbx_chksum_type) {
    case CHKSUM_SHA1:
        i = sha1_set(cs);
        break;
    case CHKSUM_SHA256:
        i = sha256_set(cs);
        break;
    case CHKSUM_SHA512:
        i = sha512_set(cs);
        break;
    case CHKSUM_MD5:
        i = md5_set(cs);
        break;
    case CHKSUM_MAX_TYPE:
    case CHKSUM_DEFAULT:
    case CHKSUM_NONE:
        i = tbx_blank_chksum_set(cs);
        break;
    }

    cs->name = _chksum_name[cs->type];

    return(i);
}


