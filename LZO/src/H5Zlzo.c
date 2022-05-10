/*
 * The filter function  H5Z_filter_lzo was adopted from
 * PyTables http://www.pytables.org.
 * The plugin can be used with the HDF5 library vesrion 1.8.11+ to read
 * HDF5 datasets compressed with lzo created by PyTables.
 */

/*
 *
Copyright Notice and Statement for PyTables Software Library and Utilities:

Copyright (c) 2002-2004 by Francesc Alted
Copyright (c) 2005-2007 by Carabos Coop. V.
Copyright (c) 2008-2010 by Francesc Alted
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

a. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

b. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the
   distribution.

c. Neither the name of Francesc Alted nor the names of its
   contributors may be used to endorse or promote products derived
   from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "lzo/lzo1x.h"
#include <H5PLextern.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

static size_t H5Z_filter_lzo(unsigned int flags, size_t cd_nelmts,
                             const unsigned int cd_values[], size_t nbytes,
                             size_t *buf_size, void **buf);

#define H5Z_FILTER_LZO 305

size_t lzo_deflate(unsigned flags, size_t cd_nelmts, const unsigned cd_values[],
                   size_t nbytes, size_t *buf_size, void **buf);

const H5Z_class2_t H5Z_LZO[1] = {{
    H5Z_CLASS_T_VERS,             /* H5Z_class_t version */
    (H5Z_filter_t)H5Z_FILTER_LZO, /* Filter id number             */
    1,                            /* encoder_present flag (set to true) */
    1,                            /* decoder_present flag (set to true) */
    "HDF5 lzo filter; see http://www.hdfgroup.org/services/contributions.html",
    /* Filter name for debugging    */
    NULL,                      /* The "can apply" callback     */
    NULL,                      /* The "set local" callback     */
    (H5Z_func_t)(lzo_deflate), /* The actual filter function   */
}};

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_FILTER; }
const void *H5PLget_plugin_info(void) { return H5Z_LZO; }

size_t lzo_deflate(unsigned flags, size_t cd_nelmts, const unsigned cd_values[],
                   size_t nbytes, size_t *buf_size, void **buf) {
  size_t ret_value = 0;
  if (lzo_init() != LZO_E_OK) {
    fprintf(stderr, "Problems initializing LZO library\n");
    return 0; /* lib is not available */
  }
  void *outbuf = NULL, *wrkmem = NULL;
  int status;
  size_t nalloc = *buf_size;
  lzo_uint out_len = (lzo_uint)nalloc;
  /* max_len_buffer will keep the likely output buffer size
     after processing the first chunk */
  static unsigned int max_len_buffer = 0;
  /* int complevel = 1; */
#if (defined DEBUG)
  int object_version = 10; /* Default version 1.0 */
  int object_type = Table; /* Default object type */
#endif

  /* Check arguments */
  /* For Table versions < 20, there were no parameters */
  if (cd_nelmts == 1) {
    /* complevel = cd_values[0]; */ /* This do nothing right now */
  } else if (cd_nelmts == 2) {
    /* complevel = cd_values[0]; */ /* This do nothing right now */
#if (defined DEBUG)
    object_version = cd_values[1]; /* The table VERSION attribute */
#endif
  } else if (cd_nelmts == 3) {
    /* complevel = cd_values[0]; */ /* This do nothing right now */
#if (defined DEBUG)
    object_version = cd_values[1]; /* The table VERSION attribute */
    object_type = cd_values[2];    /* A tag for identifying the object
                                      (see tables.h) */
#endif
  }

#ifdef DEBUG
  printf("Object type: %d. ", object_type);
  printf("object_version:%d\n", object_version);
#endif

  if (flags & H5Z_FLAG_REVERSE) {
    /* Input */

/*     printf("Decompressing chunk with LZO\n"); */
#ifdef DEBUG
    printf("Compressed bytes: %d. Uncompressed bytes: %d\n", nbytes, nalloc);
#endif

    /* Only allocate the bytes for the outbuf */
    if (max_len_buffer == 0) {
      if (NULL == (outbuf = (void *)malloc(nalloc)))
        fprintf(stderr, "Memory allocation failed for lzo uncompression.\n");
    } else {
      if (NULL == (outbuf = (void *)malloc(max_len_buffer)))
        fprintf(stderr, "Memory allocation failed for lzo uncompression.\n");
      out_len = max_len_buffer;
      nalloc = max_len_buffer;
    }

    while (1) {

#ifdef DEBUG
      printf("nbytes -->%d\n", nbytes);
      printf("nalloc -->%d\n", nalloc);
      printf("max_len_buffer -->%d\n", max_len_buffer);
#endif /* DEBUG */

      /* The assembler version is a 10% slower than the C version with
         gcc 3.2.2 and gcc 3.3.3 */
      /*       status = lzo1x_decompress_asm_safe(*buf, (lzo_uint)nbytes,
       * outbuf, */
      /*                                          &out_len, NULL); */
      /* The safe and unsafe versions have the same speed more or less */
      status =
          lzo1x_decompress_safe(*buf, (lzo_uint)nbytes, outbuf, &out_len, NULL);

      if (status == LZO_E_OK) {
#ifdef DEBUG
        printf("decompressed %lu bytes back into %lu bytes\n", (long)nbytes,
               (long)out_len);
#endif
        max_len_buffer = out_len;
        break; /* done */
      } else if (status == LZO_E_OUTPUT_OVERRUN) {
        nalloc *= 2;
        out_len = (lzo_uint)nalloc;
        if (NULL == (outbuf = realloc(outbuf, nalloc))) {
          fprintf(stderr, "Memory allocation failed for lzo uncompression\n");
        }
      } else {
        /* this should NEVER happen */
        fprintf(stderr, "internal error - decompression failed: %d\n", status);
        ret_value = 0; /* fail */
        goto done;
      }
    }

#ifdef DEBUG
    printf("Checksum uncompressing...");
#endif

    free(*buf);
    *buf = outbuf;
    outbuf = NULL;
    *buf_size = nalloc;
    ret_value = out_len;
  } else {
    /*
     * Output; compress but fail if the result would be larger than the
     * input.  The library doesn't provide in-place compression, so we
     * must allocate a separate buffer for the result.
     */
    lzo_byte *z_src = (lzo_byte *)(*buf);
    lzo_byte *z_dst; /*destination buffer            */
    lzo_uint z_src_nbytes = (lzo_uint)(nbytes);
    /* The next was the original computation for worst-case expansion */
    /* I don't know why the difference with LZO1*. Perhaps some wrong docs in
       LZO package? */
    /*     lzo_uint z_dst_nbytes = (lzo_uint)(nbytes + (nbytes / 64) + 16 + 3);
     */
    /* The next is for LZO1* algorithms */
    /*     lzo_uint z_dst_nbytes = (lzo_uint)(nbytes + (nbytes / 16) + 64 + 3);
     */
    /* The next is for LZO2* algorithms. This will be the default */
    lzo_uint z_dst_nbytes = (lzo_uint)(nbytes + (nbytes / 8) + 128 + 3);

    if (NULL == (z_dst = outbuf = (void *)malloc(z_dst_nbytes))) {
      fprintf(stderr, "Unable to allocate lzo destination buffer.\n");
      ret_value = 0; /* fail */
      goto done;
    }

    /* Compress this buffer */
    wrkmem = malloc(LZO1X_1_MEM_COMPRESS);
    if (wrkmem == NULL) {
      fprintf(stderr, "Memory allocation failed for lzo compression\n");
      ret_value = 0;
      goto done;
    }

    status =
        lzo1x_1_compress(z_src, z_src_nbytes, z_dst, &z_dst_nbytes, wrkmem);

    free(wrkmem);
    wrkmem = NULL;

#ifdef DEBUG
    printf("Checksum compressing ...");
    printf("src_nbytes: %d, dst_nbytes: %d\n", z_src_nbytes, z_dst_nbytes);
#endif

    if (z_dst_nbytes >= nbytes) {
#ifdef DEBUG
      printf("The compressed buffer takes more space than uncompressed!.\n");
#endif
      ret_value = 0; /* fail */
      goto done;
    } else if (LZO_E_OK != status) {
      fprintf(stderr, "lzo library error in compression\n");
      ret_value = 0; /* fail */
      goto done;
    } else {
      free(*buf);
      *buf = outbuf;
      outbuf = NULL;
      *buf_size = z_dst_nbytes;
      ret_value = z_dst_nbytes;
    }
  }

done:
  if (outbuf)
    free(outbuf);

  return ret_value;
}
