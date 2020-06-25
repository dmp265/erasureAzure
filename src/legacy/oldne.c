/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/



 

/* ---------------------------------------------------------------------------

This file provides the implementation of multiple operations intended for
use by the MarFS MultiComponent DAL.

These include:   ne_read(), ne_write(), ne_health(), and ne_rebuild().

Additionally, each output file gets an xattr added to it (yes all 12 files
in the case of a 10+2 the xattr looks something like this:

   10 2 64 0 196608 196608 3304199718723886772 1717171

These fields, in order, are:

    N         is nparts
    E         is numerasure
    offset    is the starting position of the stripe in terms of part number
    chunksize is chunksize
    nsz       is the size of the part
    ncompsz   is the size of the part but might get used if we ever compress the parts
    totsz     is the total real data in the N part files.

Since creating erasure requires full stripe writes, the last part of the
file may all be zeros in the parts.  Thus, totsz is the real size of the
data, not counting the trailing zeros.

All the parts and all the erasure stripes should be the same size.  To fill
in the trailing zeros, this program uses truncate - punching a hole in the
N part files for the zeros.

In the case where libne is built to include support for S3-authentication,
and to use the libne sockets extensions (RDMA, etc) instead of files, then
the caller (for example, the MarFS sockets DAL) may acquire
authentication-information at program-initialization-time which we could
not acquire at run-time.  For example, access to authentication-information
may require escalated privileges, whereas fuse and pftool de-escalate
priviledges after start-up.  To support such cases, we must allow a caller
to pass cached credentials through the ne_etc() functions, to the
underlying skt_etc() functions.

--------------------------------------------------------------------------- */

#include "libne_auto_config.h"   /* HAVE_LIBISAL */

#define DEBUG 1
#define USE_STDOUT 1
#define LOG_PREFIX "ne_core"
#include "logging/logging.h"
#include "io/io.h"
#include "dal/dal.h"
#include "thread_queue/thread_queue.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>


// Some configurable values
#define QDEPTH 4

// NE context
typedef struct ne_ctxt_struct {
   // Max Block value
   int max_block;
   // DAL definitions
   DAL dal;
}* ne_ctxt;

struct handle {
   /* Reference back to our global context */
   ne_ctxt ctxt;

   /* Object Info */
   char* objID;
   ne_location loc;

   /* Erasure Info */
   ne_erasure epat;
   size_t versz;
   size_t blocksz;
   size_t totsz;

   /* Read/Write Info and Structures */
   ne_mode   mode;
   ioblock** iob;
   off_t     iob_datasz;
   off_t     iob_offset;
   ssize_t   sub_offset;

   /* Threading fields */
   thread_queue*  thread_queues;
   gthread_state* thread_states;
   unsigned int ethreads_running;

   /* Erasure Manipulation Structures */
   unsigned char  e_ready;
   unsigned char* prev_in_err;
   unsigned int   prev_err_cnt;
   unsigned char* encode_matrix;
   unsigned char* decode_matrix;
   unsigned char* invert_matrix;
   unsigned char* g_tbls;
   unsigned char* decode_index;


// UNNEEDED VARS?
   void *buffer_list[MAX_QDEPTH];
   void *block_buffs[MAX_QDEPTH][MAXPARTS];
   pthread_t threads[MAXPARTS];
   BufferQueue blocks[MAXPARTS];

   /* Used for rebuilds to restore the original ownership to the rebuilt file. */
   uid_t owner;
   gid_t group;

   /* path-printing technique provided by caller */
   SnprintfFunc   snprintf;
   void*          printf_state;        // caller-data to be provided to <snprintf>

   /* pass-through to RDMA/sockets impl */
   SktAuth        auth;

   /* run-time dispatch of sockets versus file implementation */
   const uDAL*    impl;

   /* optional timing/benchmarking */
   TimingData     timing_data;
   TimingData*    timing_data_ptr;  /* caller of ne_open() can provide their own TimingData
                                       (e.g. so data can survive ne_close()) */
};

static int gf_gen_decode_matrix_simple(unsigned char * encode_matrix,
                                       unsigned char * decode_matrix,
                                       unsigned char * invert_matrix,
                                       unsigned char * temp_matrix,
                                       unsigned char * decode_index, unsigned char * frag_err_list, int nerrs, int k,
                                       int m);



// ---------------------- INTERNAL HELPER FUNCTIONS ----------------------


/**
 * Clear/zero out existing ne_state information
 * @param ne_state* state : Reference to the state structure to clear
 */
void zero_state( ne_state* state, int num_blocks ) {
   if ( state == NULL ) { return; }
   state->blocksz = 0;
   state->versz = 0;
   state->totsz = 0;
   int block = 0;
   for( ; block < num_blocks; block++ ) {
      if ( state->meta_status ) { state->meta_status[block] = 0; }
      if ( state->data_status ) { state->data_status[block] = 0; }
      if ( state->csum ) { state->csum[block] = 0; }
   }
}


/**
 * Allocate a new ne_handle structure
 * @param int max_block : Maximum block value
 * @return ne_handle : Allocated handle or NULL
 */
ne_handle allocate_handle( ne_ctxt ctxt, const char* objID, ne_location loc, meta_info* consensus ) {
   // create the handle structure itself
   ne_handle handle = calloc( 1, sizeof( struct ne_handle_struct ) );
   if ( handle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a ne_handle structure!\n" );
      return NULL;
   }

   // set erasure struct info
   handle->epat->N = consensus.N;
   handle->epat->E = consensus.E;
   handle->epat->O = consensus.O;
   handle->epat->partsz = consensus.partsz;
   handle->epat->crcs = 0;
   if ( consensus.versz > 0 ) { handle->epat->crcs = 1; }

   // set data info values
   handle->versz = consensus.versz;
   handle->blocksz = consensus.blocksz;
   handle->totsz = consensus.totsz;

   // set some additional handle info
   handle->ctxt = ctxt;
   handle->objID = strcpy( objID );
   handle->loc.pod = loc.pod;
   handle->loc.cap = loc.cap;
   handle->loc.scatter = loc.scatter;
   handle->iob_offset = -1; // mark for initialization

   // allocate context elements
   int num_blocks = consensus.N + consensus.E;
//   handle->state.meta_status = calloc( num_blocks, sizeof(char) );
//   if ( handle->state.meta_status == NULL ) {
//      LOG( LOG_ERR, "Failed to allocate space for a meta_status array!\n" );
//      free( handle );
//      return NULL;
//   }
//   handle->state.data_status = calloc( num_blocks, sizeof(char) );
//   if ( handle->state.data_status == NULL ) {
//      LOG( LOG_ERR, "Failed to allocate space for a data_status array!\n" );
//      free( handle.state.meta_status );
//      free( handle );
//      return NULL;
//   }
   handle->iob = calloc( num_blocks, sizeof( ioblock* ) );
   if ( handle->iob == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for ioblock references!\n" );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->thread_queues = calloc( num_blocks, sizeof( thread_queue ) );
   if ( handle->thread_queues == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for thread_queues!\n" );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->thread_states = calloc( num_blocks, sizeof( struct global_state_struct ) );
   if ( handle->thread_states == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for global thread state structs!\n" );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->prev_in_err = calloc( num_blocks, sizeof( char ) );
   if ( handle->prev_in_err == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a prev_error array!\n" );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->decode_index = calloc( num_blocks, sizeof( char ) );
   if ( handle->decode_index == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a decode_index array!\n" );
      free( handle->prev_in_err );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   /* allocate matrices */
   handle->encode_matrix = calloc( num_blocks * consensus.N, sizeof(char) );
   if ( handle->encode_matrix == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for an encode_matrix!\n" );
      free( handle->decode_index );
      free( handle->prev_in_err );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->decode_matrix = calloc( num_blocks * consensus.N, sizeof(char) );
   if ( handle->decode_matrix == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a decode_matrix!\n" );
      free( handle->encode_matrix );
      free( handle->decode_index );
      free( handle->prev_in_err );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->invert_matrix = calloc( num_blocks * consensus.N, sizeof(char) );
   if ( handle->invert_matrix == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for an invert_matrix!\n" );
      free( handle->decode_matrix );
      free( handle->encode_matrix );
      free( handle->decode_index );
      free( handle->prev_in_err );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }
   handle->g_tbls = calloc( consensus.N * consensus.E * 32, sizeof(char) );
   if ( handle->g_tbls == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for g_tbls!\n" );
      free( handle->invert_matrix );
      free( handle->decode_matrix );
      free( handle->encode_matrix );
      free( handle->decode_index );
      free( handle->prev_in_err );
      free( handle->thread_states );
      free( handle->thread_queues );
      free( handle->iob );
      free( handle->objID );
      free( handle );
      return NULL;
   }

   int i;
   for ( i = 0; i < num_blocks; i++ ) {
      // assign values to thread states
      // object attributes
      handle->thread_states[i].objID = objID;
      handle->thread_states[i].location.pod = loc.pod;
      handle->thread_states[i].location.block = (i + consensus.O) % num_blocks;
      handle->thread_states[i].location.cap = loc.cap;
      handle->thread_states[i].location.scatter = loc.scatter;
      handle->thread_states[i].dal = ctxt->dal;
      handle->thread_states[i].offset = 0;
      // meta info values
      handle->thread_states[i].minfo.N = consensus.N;
      handle->thread_states[i].minfo.E = consensus.E;
      handle->thread_states[i].minfo.O = consensus.O;
      handle->thread_states[i].minfo.partsz = consensus.partsz;
      handle->thread_states[i].minfo.versz = consensus.versz;
      handle->thread_states[i].minfo.blocksz = consensus.blocksz;
      handle->thread_states[i].minfo.crcsum = 0;
      handle->thread_states[i].minfo.totsz = consensus.totsz;
      handle->thread_states[i].meta_error = 0;
      handle->thread_states[i].data_error = 0;
//      size_t iosz = consensus.versz;
//      if ( iosz <= 0 ) { iosz = handle->dal->io_size; }
//      handle->thread_states[i].ioq = create_ioqueue( iosz, consensus.partsz, mode );
//      if ( handle->thread_states[i].ioq == NULL ) {
//         LOG( LOG_ERR, "Failed to create an ioqueue for block %d!\n", i );
//         for ( i -= 1; i >= 0; i-- ) {
//            destroy_ioqueue( handle->thread_states[i].ioq );
//         }
//         free( handle->thread_states );
//         free( handle->thread_queues );
//         free( handle->iob );
//         free( handle.state.data_status );
//         free( handle.state.meta_status );
//         free( handle );
//         return NULL;
//      }
   }

   return handle;
}


/**
 * Free an allocated ne_handle structure
 * @param ne_handle handle : Handle to free
 */
void free_handle( ne_handle handle ) {
//   int i;
//   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
//      destroy_ioqueue( handle->thread_states[i].ioq );
//   }
   free( handle->g_tbls );
   free( handle->invert_matrix );
   free( handle->decode_matrix );
   free( handle->encode_matrix );
   free( handle->decode_index );
   free( handle->prev_in_err );
   free( handle->thread_states );
   free( handle->thread_queues );
   free( handle->iob );
   free( handle->objID );
   free( handle );
}


/**
 * This helper function is intended to identify the most common sensible values amongst all meta_buffers 
 * for a given number of read threads and return them in a provided read_meta_buffer struct.
 * If two numbers have the same number of instances, preference will be given to the first number ( the 
 * one with a lower block number ).
 * @param BufferQueue blocks[ MAXPARTS ] : Array of buffer queues for all threads
 * @param int num_threads : Number of threads with meta_info ready
 * @param read_meta_buffer ret_buf : Buffer to be populated with return values
 * @return int : Lesser of the counts of matching N/E values
 */
int check_matches( meta_info** minfo_structs, int num_blocks, int max_blocks, meta_info* ret_buf ) {
   // allocate space for ALL match arrays
   int* N_match = calloc( 7, sizeof(int) * num_blocks );
   if ( N_match == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for match count arrays!\n" );
      return -1;
   }
   int* E_match = ( N_match + num_blocks );
   int* O_match = ( E_match + num_blocks );
   int* partsz_match  = ( O_match + num_blocks );
   int* versz_match   = ( partsz_match + num_blocks );
   int* blocksz_match = ( versz_match + num_blocks );
   int* totsz_match   = ( blocksz_match + num_blocks );

   int i;
   for ( i = 0; i < num_blocks; i++ ) {
      int j;
      meta_info* minfo = minfo_structs[i];
// this macro is intended to produce counts of matching values at the index of their first appearance
#define COUNT_MATCH_AT_INDEX( VAL, MATCH_LIST, MIN_VAL, MAX_VAL ) \
if ( minfo->VAL >= MIN_VAL  &&  minfo->VAL <= MAX_VAL ) { \
   for ( j = 0; j < i; j++ ) { \
      if ( minfo_structs[j]->VAL == minfo->VAL ) { \
         break; \
      } \
   } \
   MATCH_LIST[j]++; \
}
      COUNT_MATCH_AT_INDEX( N, N_match, 1, max_blocks )
      COUNT_MATCH_AT_INDEX( E, E_match, 0, max_blocks - 1 )
      COUNT_MATCH_AT_INDEX( O, O_match, 0, max_blocks - 1 )
      COUNT_MATCH_AT_INDEX( partsz, partsz_match, 1, minfo->partsz ) // no maximum
      COUNT_MATCH_AT_INDEX( versz, versz_match, 0, minfo->versz ) // no maximum
      COUNT_MATCH_AT_INDEX( blocksz, blocksz_match, 0, minfo->blocksz ) // no maximum
      COUNT_MATCH_AT_INDEX( totsz, totsz_match, 0, minfo->totsz ) //no maximum
   }

   // find the value with the most matches
   int N_index = 0;
   int E_index = 0;
   int O_index = 0;
   int partsz_index  = 0;
   int versz_index   = 0;
   int blocksz_index = 0;
   int totsz_index   = 0;
   for ( i = 1; i < num_blocks; i++ ) {
      // For N/E: if two values are tied for matches, prefer the larger value (helps to avoid taking values from a single bad meta info)
      if ( N_match[i] > N_match[N_index]  ||  
            ( N_match[i] == N_match[N_index]  &&  
              minfo_structs[i]->N > minfo_structs[N_index]->N )
         )
         N_index = i;
      if ( E_match[i] > E_match[E_index]  ||  
            ( E_match[i] == E_match[E_index]  &&  
              minfo_structs[i]->E > minfo_structs[E_index]->E )
         )
         E_index = i;
      // For other values: if two values are tied for matches, prefer the first
      if ( O_match[i] > O_match[O_index] )
         O_index = i;
      if ( partsz_match[i] > partsz_match[partsz_index] )
         partsz_index = i;
      if ( versz_match[i] > versz_match[versz_index] )
         versz_index = i;
      if ( blocksz_match[i] > blocksz_match[blocksz_index] )
         blocksz_index = i;
      // For Totsz : if two values are tied for matches, prefer the smaller value (helps to avoid returning zero-fill as 'data')
      if ( totsz_match[i] > totsz_match[totsz_index]  ||  
            ( totsz_match[i] == totsz_match[totsz_index]  &&
              minfo_structs[i]->totsz < minfo_structs[totsz_index]->totsz )
         )
         totsz_index = i;
   }

   // assign appropriate values to our output struct
   // Note: we have to do a sanity check on the match count, to make sure 
   // we don't return an out-of-bounds value.
   if ( N_match[N_index] )
      ret_buf->N = minfo_structs[N_index]->N;
   else
      ret_buf->N = 0;

   if ( E_match[E_index] )
      ret_buf->E = minfo_structs[E_index]->E;
   else
      ret_buf->E = -1;

   if ( O_match[O_index] )
      ret_buf->O = minfo_structs[O_index]->O;
   else
      ret_buf->O = -1;

   if ( partsz_match[partsz_index] )
      ret_buf->partsz = minfo_structs[partsz_index]->partsz;
   else
      ret_buf->partsz = 0;

   if ( versz_match[versz_index] )
      ret_buf->versz = minfo_structs[versz_index]->versz;
   else
      ret_buf->versz = -1;

   if ( blocksz_match[blocksz_index] )
      ret_buf->blocksz = minfo_structs[blocksz_index]->blocksz;
   else
      ret_buf->blocksz = -1;

   if ( totsz_match[totsz_index] )
      ret_buf->totsz = minfo_structs[totsz_index]->totsz;
   else
      ret_buf->totsz = -1;

   return ( N_match[N_index] > E_match[E_index] ) ? E_match[E_index] : N_match[N_index];
}


/**
 * 
 * 
 */
int read_stripes( ne_handle handle ) {

   // get some useful reference values
   int     N = handle->epat.N;
   int     E = handle->epat.E;
   ssize_t partsz = handle->epat.partsz;
   size_t  stripesz = partsz * N;
   size_t  offset = ( handle->iob_offset * N ) + handle->sub_offset;
   unsigned int start_stripe = (unsigned int) ( offset / stripesz ); // get a stripe num based on offset

   // make sure our sub_offset is stripe aligned
   if ( handle->sub_offset % stripesz  ||  handle->sub_offset != handle->iob_datasz ) {
      LOG( LOG_ERR, "Called on handle with an inappropriate sub_offset (%zd)!\n", handle->sub_offset );
      return -1;
   }

   // update handle offset values
   handle->iob_offset += ( handle->sub_offset / stripesz ) * partsz; // sub_offset should be at the end of 
   handle->sub_offset = 0;

   // if we have previous block references, we'll need to release them
   int i;
   for ( i = 0; i < handle->epat.N + handle->epat.E  &&  handle->iob[i] != NULL; i++ ) {
      if ( release_ioblock( handle->thread_states[i].ioq ) ) {
         LOG( LOG_ERR, "Failed to release ioblock reference for block %d!\n", i );
         return -1;
      }
      handle->iob[i] = NULL; // NULL out this outdated reference
   }


// ---------------------- VERIFY INTEGRITY OF ALL BLOCKS IN STRIPE ----------------------

   // First, loop through all data buffers in the stripe, looking for errors.
   // Then, starup erasure threads as necessary to deal with errors.
   // Technically, it would theoretically be more efficient to limit this to 
   // only the blocks we expect to need.  However, if we hit any error in those 
   // blocks, we'll suddenly need the whole stripe.  I am hoping the reduced 
   // complexity of just checking them all will be worth what will probably 
   // only be the slightest of performance hits.  Besides, in the expected 
   // use case of reading a file start to finish, we'll eventually need this 
   // entire stripe regardless.
   int cur_block;
   int stripecnt = 0;
   int nstripe_errors = 0;
   for ( cur_block = 0; ( cur_block < (N + nstripe_errors)  ||  cur_block < (N + handle->ethreads_running) ) 
                        &&  cur_block < (N + E); cur_block++ ) {
      // check if we can even handle however many errors we've hit so far
      if ( nstripe_errors > E ) {
         LOG( LOG_ERR, "Stripe %d has too many data errors (%d) to be recovered\n", start_stripe, nstripe_errors );
         errno=ENODATA;
         return -1;
      }
      // if this thread isn't running, we need to start it
      if ( cur_block >= N + handle->ethreads_running ) {
         LOG( LOG_INFO, "Starting up erasure thread %d to cope with data errors in ioblocks for stripe %d\n", cur_block, start_stripe );
         handle->thread_states[cur_block].offset = handle->iob_offset; // set offset for this read thread
         if( tq_unset_flags( handle->thread_queues[cur_block], TQ_HALT ) ) {
            LOG( LOG_ERR, "Failed to clear PAUSE state for block %d!\n", cur_block );
            errno=EBADF;
            return -1;
         }
         handle->ethreads_running++;
      }
      // retrieve a new ioblock from this thread
      if ( tq_dequeue( handle->thread_queues[cur_block], TQ_NONE, &(handle->iob[cur_block]) ) < 0 ) {
         LOG( LOG_ERR, "Failed to retrieve new buffer for block %d!\n", cur_block );
         errno=EBADF;
         return -1;
      } 
      // check if this new ioblock will require a rebuild
      ioblock* cur_iob = handle->iob[cur_block];
      if ( cur_iob->error_end > 0 ) { nstripe_errors++; }
      // make sure our stripecnt is logical
      if ( stripecnt ) {
         if ( (cur_iob->datasz / partsz) != stripecnt ) {
            LOG( LOG_ERR, "Detected a ioblock of size %zd from block %d which conflicts with stripe count of %d!\n", 
                           cur_iob->datasz, cur_block, stripecnt );
            errno=EBADF;
            return -1;
         }
      }
      else { stripecnt = ( cur_iob->datasz / partsz ); } // or set it, if we haven't yet
   }

   int block_cnt = cur_block;

   // if we'er trying to avoid unnecessary reads, halt excess erasure threads
   if ( handle->mode == NE_RDONLY ) {
      // keep the greater of how many erasure threads we've needed in the last couple of stripes...
      if ( nstripe_errors > handle->prev_err_cnt )
         handle->prev_err_cnt = nstripe_errors;
      // ...and halt all others
      for ( cur_block = (N + nstripe_errors); cur_block < (N + handle->prev_err_cnt); cur_block++ ) {
         if ( tq_set_flags( handle->thread_queues[cur_block], TQ_HALT ) ) {
            // nothing to do besides complain
            LOG( LOG_ERR, "Failed to pause erasure thread for block %d!\n", cur_block );
         }
         else { handle->ethreads_running--; }
      }
      // need to reassign, in case the number of errors is decreasing
      handle->prev_err_cnt = nstripe_errors;
   }

   // If any errors were found, we need to try and reconstruct any missing data
   if ( nstripe_errors ) {
      // create some erasure structs
      unsigned char* stripe_in_err = calloc( N + E, sizeof( unsigned char ) );
      if ( stripe_in_err == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for a stripe_in_err array!\n" );
         return -1;
      }
      unsigned char* stripe_err_list = calloc( N + E, sizeof( unsigned char ) );
      if ( stripe_err_list == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for a stripe_err_list array!\n" );
         return -1;
      }

      // loop over each stripe in reverse order, fixing the ends of the buffers first
      // NOTE -- reconstructing in reverse allows us to continue using the error_end values appropriately
      int cur_stripe;
      for ( cur_stripe = stripecnt - 1; cur_stripe >= 0; cur_stripe-- ) {

         // loop over the blocks of the stripe, establishing error counts/positions
         off_t stripe_start = cur_stripe * partsz;
         off_t stripe_end   = (cur_stripe+1) * partsz;
         nstripe_errors = 0;
         int nsrcerr = 0;
         for ( cur_block = 0; cur_block < block_cnt; cur_block++ ) {
            // reset our error state
            stripe_err_list[cur_block] = 0; // as cur_block MUST be <= nstripe_errors at this point
            stripe_in_err[cur_block] = 0;

            // check for bad stripe data in this block
            if ( stripe_start < handle->iob[cur_block]->error_end ) {
               LOG( LOG_WARNING, "Detected bad data for block %d of stripe %d\n", cur_block, cur_stripe + start_stripe );
               stripe_err_list[nstripe_errors] = cur_block;
               nstripe_errors++;
               stripe_in_err[cur_block] = 1;
               // we just need to note the error, nothing to be done about it until we have all buffers ready
            }

            // check for any change in our error pattern, as that will require reinitializing erasure structs
            if ( handle->prev_in_err[ cur_block ] != stripe_in_err[ cur_block ] ) {
               handle->e_ready = 0;
               handle->prev_in_err[ cur_block ] = stripe_in_err[ cur_block ];
            }

            // make sure to note the number of data errors for later erasure use
            if ( cur_block == (N - 1) )
               nsrcerr = nstripe_errors;

         }

         if ( !(handle->e_ready) ) {

            // Generate an encoding matrix
            LOG( LOG_INFO, "Initializing erasure structs...\n");
            gf_gen_crs_matrix(handle->encode_matrix, N+E, N);

            // Generate g_tbls from encode matrix
            ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);

            unsigned char* tmpmatrix = calloc( (N+E) * (N+E), sizeof( unsigned char ) );
            if ( tmpmatrix == NULL ) {
               LOG( LOG_ERR, "Failed to allocate space for a tmpmatrix!\n" );
               free( tmpmatrix );
               free( stripe_in_err );
               free( stripe_err_list );
               return -1;
            }
            int ret_code = gf_gen_decode_matrix_simple( handle->encode_matrix, handle->decode_matrix,
                  handle->invert_matrix, tmpmatrix, handle->decode_index, stripe_err_list, 
                  nstripe_errors, N, N+E);

            free( tmpmatrix );

            if (ret_code != 0) {
               // this is the only error for which we will at least attempt to continue
               LOG( LOG_ERR, "Failure to generate decode matrix, errors may exceed erasure limits (%d)!\n", nstripe_errors);
               errno=ENODATA;
               free( stripe_in_err );
               free( stripe_err_list );
               // return the number of stripes we failed to regenerate
               return cur_stripe + 1;
            }


            LOG( LOG_INFO, "Initializing erasure tables ( nsrcerr = %d )\n", nsrcerr );
            ec_init_tables(N, nstripe_errors, handle->decode_matrix, handle->g_tbls);

            handle->e_ready = 1; //indicate that rebuild structures are initialized
         }

         // as this struct will change depending on the head position of our queues, we must generate here
         unsigned char** recov = calloc( N + E, sizeof( unsigned char* ) );
         if ( recov == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for a recovery array!\n" );
            free( stripe_in_err );
            free( stripe_err_list );
            return -1;
         }
         //unsigned char* recov[ MAXPARTS ];
         for (cur_block = 0; cur_block < N; cur_block++) {
            //BufferQueue* bq = &handle->blocks[handle->decode_index[cur_block]];
            //recov[cur_block] = bq->buffers[ bq->head ];
            recov[cur_block] = handle->iob[handle->decode_index[cur_block]]->buff;
         }

         unsigned char** temp_buffs = calloc( nstripe_errors, sizeof( unsigned char* ) );
         if ( temp_buffs == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for a temp_buffs array!\n" );
            free( recov );
            free( stripe_in_err );
            free( stripe_err_list );
            return -1;
         }
         //unsigned char* temp_buffs[ nstripe_errors ];
         for ( cur_block = 0; cur_block < nstripe_errors; cur_block++ ) {
            //BufferQueue* bq = &handle->blocks[stripe_err_list[ cur_block ]];
            //temp_buffs[ cur_block ] = bq->buffers[ bq->head ];

            // assign storage locations for the repaired buffers to be on top of the faulty buffers
            temp_buffs[ cur_block ] = handle->iob[stripe_err_list[cur_block]]->buff;
            // as we are regenerating over the bad buffer, mark it as usable from this point on
            handle->iob[stripe_err_list[cur_block]]->error_end = stripe_start;

            // as we are regenerating over the bad buffer, mark it as usable for future iterations
            //*(u32*)( temp_buffs[ cur_block ] + bsz ) = 1;
         }

         LOG( LOG_INFO, "Performing regeneration of stripe %d from erasure\n", cur_stripe + start_stripe );

         ec_encode_data(partsz, N, nstripe_errors, handle->g_tbls, recov, &temp_buffs[0]);

      } // end of per-stripe loop

      // free unneeded lists
      free( stripe_in_err );
      free( stripe_err_list );

   } // end of error regeneration logic


   return 0;
}



// ---------------------- CONTEXT CREATION/DESTRUCTION ----------------------


/**
 * Initializes an ne_ctxt with a default posix DAL configuration.
 * This fucntion is intended primarily for use with test utilities and commandline tools.
 * @param const char* path : The complete path template for the erasure stripe
 * @param ne_location max_loc : The maximum pod/cap/scatter values for this context
 * @return ne_ctxt : The initialized ne_ctxt or NULL if an error occurred
 */
ne_ctxt ne_path_init ( const char* path, ne_location max_loc, int max_block ) {
   // create a stand-in XML config
   char* configtemplate = "<DAL type=\"posix\"><dir_template>%s</dir_template></DAL>"
   int len = strlen( path ) + strlen( configtemplate );
   char* xml-config = malloc( len );
   if ( xml-config == NULL ) {
      LOG( LOG_ERR, "failed to allocate memory for the stand-in xml config!\n" );
      return NULL;
   }
   if ( ( len = snprintf( xml-config, len, configtemplate, path ) ) < 0 ) {
      free( xml-config );
      return NULL;
   }
   xmlDoc *config = NULL;
   xmlNode *root_elem = NULL;

   /* initialize libxml and check for potential version mismatches */
   LIBXML_TEST_VERSION

   /* parse the stand-in XML config */
   doc = xmlReadMemory( xml-config, len + 1, "noname.xml", NULL, XML_PARSE_NOBLANKS );
   if ( doc == NULL ) {
      free( xml-config );
      return NULL;
   }
   root_elem = xmlDocGetRootElement( doc );
   free( xml-config ); // done with the stand-in xml config

   // Initialize a posix dal instance
   DAL_location maxloc = { .pod = 1, .block = 1, .cap = 1, .scatter = 1 };
   DAL dal = init_dal( root_elem, maxloc );
   // free the xmlDoc and any parser global vars
   xmlFreeDoc( doc );
   xmlCleanupParser();
   // verify that dal initialization was successful
   if ( dal == NULL ) {
      return NULL;
   } 

   // allocate a context struct
   ne_ctxt ctxt = malloc( sizeof( struct ne_ctxt_struct ) );
   if ( ctxt == NULL ) {
      return NULL;
   }

   // fill in context elements
   ctxt->max_block = max_block;
   ctxt->dal = dal;

   // return the new ne_ctxt
   return ctxt;
}



/**
 * Initializes a new ne_ctxt
 * @param xmlNode* dal_root : Root of a libxml2 DAL node describing data access
 * @param ne_location max_loc : ne_location struct containing maximum allowable pod/cap/scatter 
 *                              values for this context
 * @param int max_block : Integer maximum block value ( N + E ) for this context
 * @return ne_ctxt : New ne_ctxt or NULL if an error was encountered
 */
ne_ctxt ne_init( xmlNode* dal_root, ne_location max_loc, int max_block ) {
   // Initialize a DAL instance
   DAL_location maxdal = { .pod = max_loc.pod, .block = max_block, .cap = max_loc.cap, .scatter = max_loc.scatter };
   DAL dal = init_dal( dal_root, maxdal );
   // Verify that the dal intialized successfully
   if ( dal == NULL ) {
      LOG( LOG_ERR, "DAL instance failed to properly initialize!\n" );
      return NULL;
   }

   // allocate a new context struct
   ne_ctxt ctxt = malloc( sizeof( struct ne_ctxt_struct ) );
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "failed to allocate memory for a new ne_ctxt struct!\n" );
      dal->cleanup( dal ); // cleanup our DAL context, ignoring errors
      return NULL;
   }

   // fill in context values and return
   ctxt->max_block = max_block;
   ctxt->dal = dal;

   return ctxt;
}



/**
 * Destroys and existing ne_ctxt
 * @param ne_ctxt ctxt : Reference to the ne_ctxt to be destroyed
 * @return int : Zero on a success, and -1 on a failure
 */
int ne_term ( ne_ctxt ctxt ) {
   // Cleanup the DAL context
   if ( ctxt->dal->cleanup( ctxt->dal ) != 0 ) {
      LOG( LOG_ERR, "failed to cleanup DAL context!\n" );
      return -1;
   }
   free( ctxt );
   return 0;
}



// ---------------------- PER-OBJECT FUNCTIONS ----------------------


/**
 * Delete a given object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @return int : Zero on success and -1 on failure
 */
int ne_delete( ne_ctxt ctxt, const char* objID, ne_location loc ){
   return -1;
}



// ---------------------- HANDLE CREATION FUNCTIONS ----------------------


/**
 * Determine the erasure structure of a given object and (optionally) produce a generic handle for it
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to stat
 * @param ne_location loc : Location of the object to stat
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_stat( ne_ctxt ctxt, const char* objID, ne_location loc ) {
   // allocate space for temporary error arrays
   char* tmp_meta_errs = calloc( ctxt->max_block * 2, sizeof(char) );
   if ( tmp_meta_errs == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for temporary error arrays!\n" );
      return NULL;
   }
   char* tmp_data_errs = tmp_meta_errs + ctxt->max_block;


   // allocate space for a full set of meta_info structs
   meta_info consensus;
   meta_info* minfo_list = calloc( ctxt->max_blocks, sizeof( meta_info_struct ) );
   if ( minfo_list == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a meta_info_struct list!\n" );
      return NULL;
   }

   // loop through all blocks, getting meta_info for each
   int curblock = 0;
   int maxblock = ctxt->max_block;
   for( ; curblock < maxblock; curblock++ ) {
   
      DAL_location dloc = { .pod = loc.pod, .block = curblock, .cap = loc.cap, .scatter = loc.scatter };
      char got_meta = 0;

      // first, we need to get a block reference
      BLOCK_CTXT dblock = ctxt->dal->open( ctxt->dal->ctxt, DAL_METAREAD, dloc, objID );
      if ( dblock == NULL ) {
         LOG( LOG_ERR, "Failed to open a DAL reference for block %d!\n", dloc.block );
         tmp_meta_errs[curblock] = 1;
      }
      else {
         // attempt to retrive meta info
         if ( dal_get_minfo( ctxt->dal, dblock, &minfo ) ) {
            tmp_meta_errs[curblock] = 1;
         }
         else {
            // get new consensus values, including this info
            int match_count = check_matches( minfo_list, curblock + 1, ctxt->max_blocks, &consensus );
            // if we have sufficient agreement, update our maxblock value and save us some time
            if ( match_count > MIN_MD_CONSENSUS ) {
               maxblock = consensus.N + consensus.E;
            }
         }
      }

      // verify that data exists for this block
      if ( ctxt->dal->stat( ctxt->dal->ctxt, dloc, objID ) ) {
         tmp_data_errs[curblock] = 1;
      }

   }

   // create a handle structure
   ne_handle handle = allocate_handle( ctxt, objID, loc, &consensus );
   if ( handle == NULL ) {
      free( minfo_list );
      return NULL;
   }

   // perform sanity checks on the values we've gotten
   int retval = 1;
   if ( consensus.N <= 0 ) { retval = 0; }
   if ( consensus.E < 0 )  { retval = 0; }
   if ( consensus.O < 0  ||  consensus.O >= (consensus.N + consensus.E) ) {
      consensus.O = -1; retval = 0;
   }
   // at this point, if we have all valid N/E/O values, we need to rearange our errors based on offset
   int i;
   if ( retval ) {
      for ( i = 0; i < curblock; i++ ) {
         if ( tmp_meta_errs[ (i+consensus.O) % (consensus.N+consensus.E) ] ) {
            handle->thread_states[i].meta_error = 1;
         }
         else if ( cmp_minfo( &(minfo_list[ (i+consensus.O) % (consensus.N+consensus.E) ]), &(consensus) ) ) {
            handle->thread_states[i].meta_error = 1;
         }
         if ( tmp_data_errs[ (i+consensus.O) % (consensus.N+consensus.E) ] ) { handle->thread_states[i].data_error = 1; }
      }
   }
   if ( consensus.partsz <= 0 ) { retval = 0; }
   if ( consensus.versz < 0 )   { retval = 0; }
   if ( consensus.blocksz < 0 ) { retval = 0; }
   if ( consensus.totsz < 0 )   { retval = 0; }
   // if we have successfully identified all meta values, try to set crcs appropriately
   if ( retval ) {
      for ( i = 0; i < curblock; i++ ) {
         if ( handle->thread_states[i].meta_error == 0 ) { 
            handle->thread_states[i].minfo.csum = minfo_list[ (i+consensus.O) % (consensus.N+consensus.E) ].csum;
         }
      }
   }

   // indicate whether the handle appears usable or not
   handle->mode = retval;
   free( minfo_list );

   return handle;
}


/**
 * Converts a generic handle (produced by ne_stat()) into a handle for a specific operation
 * @param ne_handle handle : Reference to a generic handle (produced by ne_stat())
 * @param ne_mode mode : Mode to be set for handle (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Reference to the modified handle, or NULL if an error occured
 */
ne_handle ne_convert_handle( ne_handle handle, ne_mode mode ) {
   // sanity check for NULL value
   if ( handle == NULL ) {
      LOG( LOG_ERR, "Received NULL ne_handle!\n" );
      return NULL;
   }

   // check that mode is appropriate
   if ( handle->mode != 1 ) {
      LOG( LOG_ERR, "Received ne_handle has inappropriate mode value!\n" );
      return NULL;
   }

   // set our mode to the new value
   handle->mode = mode;

   // we need to startup some threads
   TQ_Init_Opts tqopts;
   if ( mode == NE_REBUILD ) {
      tqopts.log_prefix = "ReReadQueue";
   }
   else if ( mode == NE_WRONLY || mode == NE_WRALL ) {
      tqopts.log_prefix = "WriteQueue";
   }
   else {
      tqopts.log_prefix = "ReadQueue";
   }
   tqopts.init_flags = TQ_HALT; // initialize the threads in a HALTED state (essential for reads, doesn't hurt writes)
   tqopts.max_qdepth = QDEPTH;
   tqopts.num_threads = 1;
   tqopts.num_prod_threads = ( mode == NE_WRONLY || mode == NE_WRALL ) ? 0 : 1;
   DAL_mode dmode = DAL_READ;
   if ( mode == NE_WRONLY || mode == NE_WRALL ) {
      dmode = DAL_WRITE;
      tqopts.thread_init_func = write_init;
      tqopts.thread_consumer_func = write_consume;
      tqopts.thread_producer_func = NULL;
      tqopts.thread_pause_func = write_pause;
      tqopts.thread_resume_func = write_resume;
      tqopts.thread_term_func = write_term;
   }
   else {
      tqopts.thread_init_func = read_init;
      tqopts.thread_consumer_func = read_consume;
      tqopts.thread_producer_func = NULL;
      tqopts.thread_pause_func = read_pause;
      tqopts.thread_resume_func = read_resume;
      tqopts.thread_term_func = read_term;
   }
   // finally, startup the threads
   int i;
   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
      handle->thread_states.dmode = dmode;
      tqopts.global_state = &(handle->thread_states[i]);
      handle->thread_queues[i] = tq_init( &tqopts );
      if ( handle->thread_queues[i] == NULL ) {
         LOG( LOG_ERR, "Failed to create thread_queue for block %d!\n", i );
         // if we failed to initialize any thread_queue, attempt to abort everything else
         for ( i -= 1; i >= 0; i-- ) {
            tq_set_flags( handle->thread_queues[i], TQ_ABORT );
            tq_next_thread_status( handle->thread_queues[i], NULL );
            tq_close( handle->thread_queues[i] );
         }
         return NULL;
      }
   }

   // For reading handles, we may need to get meta info consensus and correct any outliers
   if ( mode != NE_WRONLY  &&  mode != NE_WRALL  &&  handle->totsz == 0 ) {
      // only get meta info if it hasn't already been set (totsz is a good example value)

      // create a reference array for all of our meta_info structs
      meta_info** minforefs = calloc( handle->epat.N + handle->epat.E, sizeof( meta_info* ) );
      if ( minforefs == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for meta_info references!\n" );
         // might as well continue to use 'i'
         for ( i -= 1; i >= 0; i-- ) {
            tq_set_flags( handle->thread_queues[i], TQ_ABORT );
            tq_next_thread_status( handle->thread_queues[i], NULL );
            tq_close( handle->thread_queues[i] );
         }
         return NULL;
      }

      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         // set references for every thread struct
         minforefs[i] = &(handle->thread_states[i].minfo);
      }

      // get consensus values across all threads
      meta_info consensus;
      int match_count = check_matches( minfo_list, curblock + 1, ctxt->max_blocks, &consensus );
      free( minforefs ); // now unneeded
      if ( consensus.N != handle->epat.N  ||  consensus.E != handle->epat.E  ||  
            consensus.O != handle->epat.O  ||  consensus.partsz != handle->epat.partsz ) {
         LOG( LOG_ERR, "Read meta values ( N=%d, E=%d, O=%d, partsz=%zd ) disagree with handle values ( N=%d, E=%d, O=%d, partsz=%zd )!\n", 
              consensus.N, consensus.E, consensus.O, consensus.partsz, handle->epat.N, handle->epat.E, handle->epat.O, handle->epat.partsz );
         // might as well continue to use 'i'
         for ( i -= 1; i >= 0; i-- ) {
            tq_set_flags( handle->thread_queues[i], TQ_ABORT );
            tq_next_thread_status( handle->thread_queues[i], NULL );
            tq_close( handle->thread_queues[i] );
         }
         return NULL;
      }

      // confirm and correct meta info values
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         if ( cmp_minfo( &(handle->thread_states[i].minfo), &(consensus) ) ) {
            LOG( LOG_WARN, "Meta values of thread %d do not match consensus!\n" );
            handle->thread_states[i].meta_err = 1;
            cpy_minfo( &(handle->thread_states[i].minfo), &(consensus) );
         } 
      }

   }

   // start with zero erasure threads running
   handle->ethreads_running = 0;
   if ( mode != NE_RDONLY ) {  handle->ethreads_running = handle->epat.E; }

   // unpause threads
   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
      // determine our iosize
      size_t iosz = ctxt->dal->io_size; // need to know our iosz
      if ( handle->state.versz ) { iosz = handle->state.versz; } // if we already have a versz, use that instead

      // initialize ioqueues
      handle->thread_states[i].ioq = create_ioqueue( iosz, handle->epat.partsz, dmode );
      if ( handle->thread_states[i].ioq == NULL ) {
         LOG( LOG_ERR, "Failed to create ioqueue for thread %d!\n", i );
         break;
      }
      // remove the PAUSE flag, allowing thread to begin processing
      if ( i < handle->epat.N + handle->ethreads_running ) {
         if( tq_unset_flags( handle->thread_queues[i], TQ_PAUSE ) ) {
            LOG( LOG_ERR, "Failed to unset PAUSE flag for block %d\n", i );
            break;
         }
      }
   }
   // abort if any errors occurred
   if ( i != handle->epat.N + handle->epat.E ) { // any 'break' condition should trigger this
      for( ; i >= 0; i-- ) {
         if ( handle->thread_states[i].ioq ) {
            destroy_ioqueue( handle->thread_states[i].ioq );
         }
      }
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         tq_set_flags( handle->thread_queues[i], TQ_ABORT );
         tq_next_thread_status( handle->thread_queues[i], NULL );
         tq_close( handle->thread_queues[i] );
      }
      return NULL;
   }

   return handle;
}


/**
 * Create a new handle for reading, writing, or rebuilding a specific object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @param ne_erasure epat : Erasure pattern of the object to be rebuilt
 * @param ne_mode mode : Handle mode (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_open( ne_ctxt ctxt, const char* objID, ne_location loc, ne_erasure epat, ne_mode mode ) {
   // verify our mode arg and context
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_ctxt argument!\n" );
      return NULL;
   }

   // verify that our mode argument makes sense
   if ( mode != NE_RDONLY  &&  mode != NE_RDALL  &&  mode != NE_WRONLY  &&  mode != NE_WRALL  &&  mode != NE_REBUILD ) {
      LOG( LOG_ERR, "Recieved an inappropriate mode argument!\n" );
      return NULL;
   }

   // create a meta_info struct to pass for handle creation
   meta_info minfo;
   minfo.N = epat.N;
   minfo.E = epat.E;
   minfo.O = epat.O;
   minfo.partsz = epat.partsz;
   minfo.versz = ( mode == NE_WRONLY || mode == NE_WRALL ) ? ctxt->dal->io_size : 0;
   minfo.blocksz = 0;
   minfo.crcsum = 0;
   minfo.totsz = 0;

   // allocate our handle structure
   ne_handle handle = allocate_handle( ctxt, objID, loc, &minfo );
   if ( handle == NULL ) {
      LOG( LOG_ERR, "Failed to create an ne_handle!\n" );
      return NULL;
   }

   // convert our handle to the approprate mode and start threads
   ne_handle converted_handle = ne_convert_handle( handle, mode );
   if ( converted_handle == NULL ) {
      LOG( LOG_ERR, "Failed to convert handle to appropriate mode!\n" );
      free_handle( handle );
      return NULL;
   }

   return handle;  // same reference as converted handle
}


/**
 * Close an open ne_handle
 * @param ne_handle handle : The ne_handle reference to close
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero on success, and -1 on a failure
 */
int ne_close( ne_handle handle, ne_erasure* epat, ne_state* sref ) {
   // check error conditions
   if ( !(handle) ) {
      LOG( LOG_ERR, "Received a NULL handle!\n" );
      errno = EINVAL;
      return -1;
   }

   int N = handle->epat.N;
   int E = handle->epat.E;
   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * N;

   int i;
   if ( handle->mode == NE_WRONLY  ||  handle->mode == NE_WRALL ) {
      // propagate our current totsz value to all threads
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         handle->thread_states[i].minfo.totsz = handle->totsz;
      }
      // output zero-fill to complete our current stripe
      size_t partstripe = handle->totsz % stripesz; 
      if ( partstripe ) {
         void* zerobuff = calloc( 1, partstripe );
         if ( zerobuff == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for a zero buffer!\n" );
            return -1;
         }
         if ( ne_write( handle, zerobuff, stripesz - partstripe ) != (stripesz - partstripe) ) {
            LOG( LOG_ERR, "Failed to write zero-fill to handle!\n" );
            free( zerobuff );
            return -1;
         }
         free( zerobuff );
      }
   }

   // set a FINISHED state for all threads
   int ret_val = 0;
   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
      // make sure to release any remaining ioblocks
      if ( handle->iob[i] ) {
         if ( handle->iob[i]->datasz  &&  ( handle->mode == NE_WRONLY  ||  handle->mode == NE_WRALL ) ) {
            // if data remains, push it now
            if ( tq_enqueue( handle->thread_queues[i], TQ_NONE, (void*)(handle->iob[i]) ) ) {
               LOG( LOG_ERR, "Failed to enqueue final ioblock to thread_queue %d!\n", i );
               ret_val = -1;
            }
         }
         else {
            release_ioblock( handle->thread_states[i].ioq );
         }
         handle->iob[i] = NULL;
      }
      // signal the thread to finish
      if ( tq_set_flags( handle->thread_queues[i], TQ_FINISHED ) ) {
         LOG( LOG_ERR, "Failed to set a FINISHED state for thread %d!\n", i );
         ret_val = -1;
         // attempt to abort, ignoring errors
         tq_set_flags( handle->thread_queues[i], TQ_ABORT );
      }
   }

   // verify thread termination and close all queues
   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
      tq_next_thread_status( handle->thread_queues[i], NULL );
      tq_close( handle->thread_queues[i] );
      destroy_ioqueue( handle->thread_states[i].ioq );
   }

   // populate in info structs
   if ( ne_get_info( handle, epat, sref ) < 0 ) { ret_val = -1; }

   if ( handle->mode == NE_WRITE  ||  handle->mode == NE_REBUILD ) {
      // check the status of all blocks
      int numerrs = 0; // for checking write safety
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         if ( handle->thread_states[i].meta_err  ||  handle->thread_states[i].data_err ) { 
            numerrs++;
         }
      }

      // verify that our data meets safetly thresholds
      if ( numerrs > 0  &&  numerrs > ( handle->epat.E - MIN_PROTECTION ) ) { ret_val = -1; }
   }

   free_handle( handle );

   // modify our return value to reflect any errors encountered
   if ( ret_val == 0 ) { ret_val = numerrs; }

   return ret_val;
}



// ---------------------- RETRIEVAL/SEEDING OF HANDLE INFO ----------------------


/**
 * Retrieve erasure and status info for a given object handle
 * @param ne_handle handle : Handle to retrieve info for
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero on success, and -1 on a failure
 */
int ne_get_info( ne_handle handle, ne_erasure* epat, ne_state* sref ) {
   // sanity checks
   if ( handle == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_handle!\n" );
      return -1;
   }
   if ( handle->mode == 0 ) {
      LOG( LOG_ERR, "Received an improperly initilized handle!\n" );
      return -1;
   }

   // populate the epat struct
   if ( epat ) {
      epat->N = handle->epat.N;
      epat->E = handle->epat.E;
      epat->O = handle->epat.O;
      epat->partsz = handle->epat.partsz;
   }
   // populate the state struct
   if ( sref ) {
      sref->versz = handle->versz;
      sref->blocksz = handle->blocksz;
      sref->totsz = handle->totsz;
      if ( sref->meta_status ) {
         int i;
         for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
            if ( handle->thread_states[i].meta_err ) { sref->meta_status[i] = 1; }
            else { sref->meta_status[i] = 0; }
         }
      }
      if ( sref->data_status ) {
         int i;
         for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
            if ( handle->thread_states[i].data_err ) { sref->data_status[i] = 1; }
            else { sref->data_status[i] = 0; }
         }
      }
      if ( sref->csum ) {
         int i;
         for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
            sref->csum[i] = handle->thread_state[i].minfo.crcsum;
         }
      }
   }

   return 0;
}


/**
 * Seed error patterns into a given handle (may useful for speeding up ne_rebuild())
 * @param ne_handle handle : Handle for which to set an error pattern
 * @param ne_state* sref : Reference to an ne_state struct containing the error pattern
 * @return int : Zero on success, and -1 on a failure
 */
int ne_seed_status( ne_handle handle, ne_state* sref ) {
   // sanity checks
   if ( handle == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_handle!\n" );
      return -1;
   }
   if ( handle->mode == 0 ) {
      LOG( LOG_ERR, "Received an improperly initilized handle!\n" );
      return -1;
   }
   if ( sref == NULL ) {
      LOG( LOG_ERR, "Received a null ne_state reference!\n" );
      return -1;
   }

   // set handle values
   handle->versz = sref->versz;
   handle->blocksz = sref->blocksz;
   handle->totsz = sref->totsz;
   if ( sref->meta_status ) {
      int i;
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         if ( sref->meta_status[i] ) { handle->thread_states[i].meta_err = 1; }
         else { handle->thread_states[i].meta_err = 0; }
      }
   }
   if ( sref->data_status ) {
      int i;
      for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
         if ( sref->data_status[i] ) { handle->thread_states[i].data_err = 1; }
         else { handle->thread_states[i].data_err = 0; }
      }
   }
   // ignore csum values!

   return 0;
}



// ---------------------- READ/WRITE/REBUILD FUNCTIONS ----------------------


/**
 * Verify a given erasure striped object and reconstruct any damaged data, if possible
 * @param ne_handle handle : Handle on which to perform a rebuild
 * @param ne_erasure* epat : Erasure pattern of the object to be rebuilt
 * @param ne_state* sref : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero if no stripe errors were found, a positive integer bitmask of any repaired 
 *               errors, or a negative value if an unrecoverable failure occurred
 */
int ne_rebuild( ne_handle handle, ne_erasure* epat, ne_status* sref ) {
   // TODO
   return -1;
}


/**
 * Seek to a new offset on a read ne_handle
 * @param ne_handle handle : Handle on which to seek (must be open for read)
 * @param off_t offset : Offset to seek to ( -1 == EOF )
 * @return off_t : New offset of handle ( negative value, if an error occurred )
 */
off_t ne_seek( ne_handle handle, off_t offset ) {
   // check error conditions
   if ( !(handle) ) {
      LOG( LOG_ERR, "Received a NULL handle!\n" );
      errno = EINVAL;
      return -1;
   }

   if ( handle->mode != NE_RDONLY  &&  handle->mode != NE_RDALL ) {
      LOG( LOG_ERR, "Handle is in improper mode for reading!\n" );
      errno = EPERM;
      return -1;
   }

   if ( offset > handle->totsz ) {
      offset = handle->totsz;
      LOG( LOG_WARNING, "Seek offset extends beyond EOF, resizing read request to %zu\n", offset);
   }

   int N = handle->epat.N;
   int E = handle->epat.E;
   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * N;

   // skip to appropriate stripe

   unsigned int cur_stripe = (unsigned int)( handle->iob_offset / partsz ); // I think int truncation actually works out in our favor here
   unsigned int tgt_stripe = (unsigned int)( offset / stripesz );
   ssize_t max_data = ioqueue_maxdata( handle->thread_states[0]->ioq );
   if ( max_data < 0 ) { max_data = 0; } // if we hit an error, just assume no read-ahead

   // if the offset is behind us or so far in front that there is no chance of the work having already been done...
   if ( (cur_stripe > tgt_stripe)  ||  
         ( (handle->iob_offset + max_data) < ( tgt_stripe * partsz )) ) {
      LOG( LOG_INFO, "New offset of %zd will require threads to reseek\n", offset );
      off_t new_iob_off = -1;
      int i;
      for ( i = 0; i < (N + handle->ethreads_running); i++ ) {
         // first, pause this thread
         if ( tq_set_flags( handle->thread_queues[i], TQ_HALT ) ) {
            LOG( LOG_ERR, "Failed to set HALT state for block %d!\n", i );
            break;
         }
         // next, release any unneeded ioblock reference
         if ( handle->iob[i] != NULL ) {
            if ( release_ioblock( handle->thread_states[i].ioq ) ) {
               LOG( LOG_ERR, "Failed to release ioblock ref for block %d!\n", i );
               break;
            }
            handle->iob[i] = NULL;
         }
         // wait for the thread to pause
         if ( tq_wait_for_pause( handle->thread_queues[i] ) ) {
            LOG( LOG_ERR, "Failed to detect thread pause for block %d!\n", i );
            break;
         }
         // empty all remaining queue elements
         int depth = tq_depth( handle->thread_queues[i] );
         while ( depth > 0 ) {
            depth = tq_dequeue( handle->thread_queue[i], TQ_HALT, &(handle->iob[i]) );
            if ( depth < 0 ) {
               LOG( LOG_ERR, "Failed to dequeue from HALTED thread_queue %d!\n", i );
               break;
            }
            if ( handle->iob[i] != NULL ) {
               release_ioblock( handle->thread_states[i].ioq );
               handle->iob[i] = NULL;
            }
            depth--; // decrement, as dequeue depth includes the returned element
         }
         if ( depth != 0 ) { break; } // catch any previous error
         // set the thread to our target offset
         handle->thread_states[i].offset = (tgt_stripe * partsz);
         // unpause the thread
         if ( tq_unset_flags( handle->thread_queues[i], TQ_HALT ) ) {
            LOG( LOG_ERR, "Failed to unset HALT state for block %d!\n", i );
            break;
         }
         // retrieve a new ioblock
         if ( tq_dequeue( handle->thread_queues[i], TQ_NONE, &(handle->iob[i]) ) < 0 ) {
            LOG( LOG_ERR, "Failed to retrieve ioblock for block %d after seek!\n", i );
            break;
         }
         // by now, the thread must have updated our offset
         off_t real_iob_off = handle->thread_states[i].offset;
         // sanity check this value
         if ( real_iob_off < 0  ||  real_iob_off > (tgt_stripe * partsz) ) {
            LOG( LOG_ERR, "Real offset of block %d (%zd) is not in expected range!\n", i, real_iob_off );
            break;
         }
         // if this is our first block, remember this value and set ioblock data size
         if( new_iob_off < 0 ) {
            new_iob_off = real_iob_off;
            handle->iob_datasz = handle->iob[i]->datasz;
         }
         else if ( new_iob_off != real_iob_off ) { //otherwise, further sanity check
            LOG( LOG_ERR, "Real offset of block %d (%zd) does not match previous value of %zd!\n", i, real_iob_off, new_iob_off );
            break;
         }
         else if ( handle->iob[i]->datasz != handle->iob_datasz ) {
            LOG( LOG_ERR, "Data size of ioblock for position %d (%zd) does not match that of previous ioblocks (%zd)!\n", 
                           handle->iob[i]->datasz, handle->iob_datasz );
            break;
         }
      }
      // catch any error conditions by checking our index
      if ( i != (N + handle->ethreads_running) ) {
         handle->mode = NE_ERR; // make sure that no one tries to reuse this broken handle!
         errno = EBADF;
         return -1;
      }

      handle->iob_offset = new_iob_off;
      int iob_stripe = (int)( new_iob_off / partsz );
      handle->sub_offset = offset - ( iob_stripe * stripesz );
   }
   else {
      // we may still be many stripes behind the given offset.  Calculate how many.
      unsigned int munch_stripes = tgt_stripe - cur_stripe;
      // if we're at all behind...
      if (munch_stripes) {
         LOG( LOG_INFO, "Attempting to 'munch' %d stripes to reach offset of %zd\n", munch_stripes, offset );

         // first, skip our sub_offset ahead to the next stripe boundary
         handle->sub_offset = ( (cur_stripe+1) * stripesz ) - ( handle->iob_offset * N );

         // just chuck buffers off of each queue until we hit the right stripe
         unsigned int thread_munched = 1;
         for ( ; thread_munched < munch_stripes; thread_munched++ ) {

            if ( handle->sub_off >= handle->iob_datasz ) {
               if ( read_stripes( handle ) ) {
                  LOG( LOG_ERR, "Failed to read additional stripes!\n" );
                  return -1;
               }
            }

            // skip to the start of the next stripe
            handle->sub_off += stripesz;

         }
         LOG( LOG_INFO, "Finished buffer 'cunching'\n" );
      }
      handle->sub_off += offset % stripesz;
   }
   LOG( LOG_INFO, "Post seek: IOBlock_Offset=%zd, Sub_Offset=%zd\n", handle->iob_offset, handle->sub_offset );
   return ( handle->iob_offset * N ) + handle->sub_offset; // should equal our target offset
}




/**
 * Read from a given NE_RDONLY or NE_RDALL handle
 * @param ne_handle handle : The ne_handle reference to read from
 * @param off_t offset : Offset at which to read
 * @param void* buffer : Reference to a buffer to be filled with read data
 * @param size_t bytes : Number of bytes to be read
 * @return ssize_t : The number of bytes successfully read, or -1 on a failure
 */
ssize_t ne_read( ne_handle handle, void* buffer, size_t bytes ) {
   // check boundary and invalid call conditions
   if ( !(handle) ) {
      LOG( LOG_ERR, "Received a NULL handle!\n" );
      errno = EINVAL;
      return -1;
   }
   if (bytes > UINT_MAX) {
     LOG( LOG_ERR, "Not yet validated for write-sizes above %lu\n", UINT_MAX);
     errno = EFBIG;             /* sort of */
     return -1;
   }
   if ( handle->mode != NE_RDONLY  &&  handle->mode != NE_RDALL ) {
      LOG( LOG_ERR, "Handle is in improper mode for reading!\n" );
      errno = EPERM;
      return -1;
   }
   size_t offset = ( handle->iob_offset * handle->epat.N ) + handle->sub_offset;
   LOG( LOG_INFO, "Called to retrieve %zu bytes at offset %zu\n", bytes, offset );
   if ( (offset + bytes) > handle->totsz ) {
      if ( offset >= handle->totsz )
         return 0; //EOF
      bytes = handle->totsz - offset;
      LOG( LOG_WARNING, "Read would extend beyond EOF, resizing read request to %zu\n", bytes);
   }

   // get some useful reference values
   int     N = handle->epat.N;
   int     E = handle->epat.E;
   ssize_t partsz = handle->epat.partsz;
   size_t  stripesz = partsz * N;
   int     orig_stripe = (int)( offset / stripesz ); // getting help from integer truncation
   int     iob_stripe = (int)( handle->iob_offset / stripesz );

// ---------------------- BEGIN MAIN READ LOOP ----------------------

   // time to start actually filling this read request
   size_t bytes_read = 0;
   while( bytes_read < bytes ) { // while data still remains to be read, loop over each stripe
      // first, check if we need to populate additional data
      if ( handle->sub_offset >= handle->iob_datasz ) {
         if ( read_stripes( handle ) < 0 ) {
            LOG( LOG_ERR, "Failed to populate stripes beyond offset %zu!", offset );
            return -1;
         }
      }

      int    cur_stripe = (int)(handle->sub_offset / stripesz);
      off_t  off_in_stripe = handle->sub_offset % stripesz; 
      size_t to_read_in_stripe = (bytes - bytes_read) % stripesz;

      // copy buffers from each block
      int cur_block = off_in_stripe / partsz;
      for( ; cur_block < N; cur_block++ ) {
         ioblock* cur_iob = handle->iob[ cur_block ];
         // make sure the ioblock has sufficient data
         if ( ( cur_iob->datasz - handle->iob_offset ) < partsz ) {
            LOG( LOG_ERR, "Ioblock at position %d of stripe %d is subsized!\n", cur_block, cur_stripe + iob_stripe );
            return -1;
         }
         // make sure the ioblock has no errors in this stripe
         if ( cur_iob->error_end > ( cur_stripe * partsz ) ) {
            LOG( LOG_ERR, "Ioblock at position %d of stripe %d has an error beyond requested stripe (err_end = %zu)!\n", 
                          cur_block, cur_stripe + iob_stripe, cur_iob->err_end );
            return -1;
         }
         // otherwise, copy this data off to our caller's buffer
         off_t  block_off  = ( off_in_stripe % partsz );
         size_t block_read = ( to_read_in_stripe > (partsz - block_off) ) ? (partsz - block_off) : to_read_in_stripe;
         if ( block_read == 0 ) { break; } // if we've completed our reads, stop here
         memcpy( buffer + bytes_read, cur_iob->buff + ( cur_stripe * partsz ), block_read );
         // update all values
         bytes_read += block_read;
         handle->sub_offset += block_read;
         off_in_stripe += block_read;
         to_read_in_stripe -= block_read;
      }
   }

   return bytes_read;
}



/**
 * Write to a given NE_WRONLY or NE_WRALL handle
 * @param ne_handle handle : The ne_handle reference to write to
 * @param const void* buffer : Buffer to be written to the handle
 * @param size_t bytes : Number of bytes to be written from the buffer
 * @return ssize_t : The number of bytes successfully written, or -1 on a failure
 */
ssize_t ne_write( ne_handle handle, const void* buffer, size_t bytes ) {

   // necessary?
   if (bytes > UINT_MAX) {
     LOG( LOG_ERR, "Not yet validated for write-sizes above %lu!\n", UINT_MAX);
     errno = EFBIG;             /* sort of */
     return -1;
   }

   if ( !(handle) ) {
     LOG( LOG_ERR, "Received a NULL handle!\n" );
     errno = EINVAL;
     return -1;
   }

   if ( handle->mode != NE_WRONLY  &&  handle->mode != NE_WRALL ) {
     LOG( LOG_ERR, "Handle is in improper mode for writing! %d\n", handle->mode );
     errno = EINVAL;
     return -1;
   }

   int N = handle->epat.N;
   int E = handle->epat.E;
   size_t partsz = handle->epat.partsz;
   size_t stripesz = ( N * partsz );
   off_t offset = handle->iob_offset + handle->sub_offset;
   unsigned int stripenum = offset / stripesz;


   // initialize erasure structs (these never change for writes, so we can just check here)
   if ( handle->e_ready == 0 ) {
      PRINTdbg( "ne_write: initializing erasure matricies...\n");
      // Generate an encoding matrix
      // NOTE: The matrix generated by gf_gen_rs_matrix is not always invertable for N>=6 and E>=5!
      gf_gen_crs_matrix(handle->encode_matrix, N+E, N);
      // Generate g_tbls from encode matrix
      ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);

      handle->e_ready = 1;
   }
   // allocate space for our buffer references
   void** tgt_refs = calloc( N+E, sizeof( char* ) );
   if ( tgt_refs == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a target buffer array!\n" );
      return -1;
   }


   int outblock = ( offset % stripesz ) / partsz; //determine what block we're filling
   size_t to_write = partsz - ( offset % partsz ); //determine if we need to finish writing a data part

   LOG( LOG_INFO, "Write of %zu bytes at offset %zu\n   Init write block = %d\n   Init write size = %zu\n",
                  bytes, ( handle->iob_offset * N ) + handle->sub_offset, outblock, to_write );

   // write out data from the buffer until we have all of it
   // NOTE - the (outblock >= N) check is meant to ensure we don't quit before outputing erasure parts
   ssize_t written = 0;
   while ( written < bytes  ||  outblock >= N ) {
      ioblock* push_block = NULL;
      int reserved;
      // check that the current ioblock has room for our data
      if ( (reserved = reserve_ioblock( &(handle->iob[outblock]), &(push_block), handle->thread_states[i].ioq )) == 0 ) {
         // if this is a data part, we need to fill it now
         if ( outblock < N ) {
            // make sure we don't try to store more data than we were given
            char complete_part = 1;
            if ( to_write > ( bytes - written ) ) {
               to_write = (bytes - written);
               complete_part = 0;
            }
            void* tgt = ioblock_write_tgt( handle->iob[outblock] );
            // copy caller data into our ioblock
            memcpy( tgt, buffer + written, to_write ); // no error check, SEGFAULT or nothing
            // update any data tracking values
            if ( ioblock_update_fill( handle->iob[outblock], to_write, 0 ) ) {
               LOG( LOG_ERR, "Failed to update data size of ioblock reference %d!\n", outblock );
               errno = EBADF;
               free( tgt_refs );
               return -1;
            }
            written += to_write;
            handle->sub_offset += to_write;
            handle->totsz += to_write;
            // check if we have completed a part (always the case for erasure parts)
            if ( complete_part ) { outblock++; }
         }
         else {
            // assume we will successfully generate erasure
            if ( ioblock_update_fill( handle->iob[outblock], partsz, 0 ) ) {
               LOG( LOG_ERR, "Failed to update data size of ioblock reference %d!\n", outblock );
               errno = EBADF;
               free( tgt_refs );
               return -1;
            }
            outblock++;
         }

         // check if we have completed a stripe
         if ( outblock == (N + E) ) {
            LOG( LOG_INFO, "Generating erasure parts for stripe %u\n", stripenum );
            // build an array of data/erasure references while reseting outblock to zero
            for ( outblock -= 1; outblock >= 0; outblock-- ) {
               // previously written data will be one partsz behind
               tgt_refs[outblock] = ioblock_write_target( handle->iob[outblock] ) - partsz;
            }
            // generate erasure parts
            ec_encode_data(bsz, N, E, handle->g_tbls,
                           (unsigned char **)tgt_refs,
                           (unsigned char **)&(tgt_refs[N]));
         }
      }
      else if ( reserved > 0 ) {
         // the block is full and must be pushed to our iothread
         if ( tq_enqueue( handle->thread_queues[outblock], TQ_NONE, (void*) push_block ) ) {
            LOG( LOG_ERR, "Failed to push ioblock to thread_queue %d\n", outblock );
            errno = EBADF;
            free( tgt_refs );
            return -1;
         }
         outblock++;
      }
      else {
         LOG( LOG_ERR, "Failed to reserve ioblock for position %d!\n", outblock );
         errno = EBADF;
         free( tgt_refs );
         return -1;
      }

      if ( outblock == (N + E) ) {
         // reset to the beginning of the stripe
         outblock = 0;
      }

   }

   // we have output all data
   free( tgt_refs );
   return written;
}









//     --------------          OLD STUFF







static int set_block_xattr(ne_handle handle, int block);



// #defines, macros, external functions, etc, that we don't want exported
// for users of the library some are also used in libneTest.c
//
// #include "erasure_internals.h"


/* The following are defined here, so as to hide them from users of the library */
// erasure functions
#ifdef HAVE_LIBISAL
extern uint32_t      crc32_ieee(uint32_t seed, uint8_t * buf, uint64_t len);
extern void          ec_encode_data(int len, int srcs, int dests, unsigned char *v,unsigned char **src, unsigned char **dest);

#else
extern uint32_t      crc32_ieee_base(uint32_t seed, uint8_t * buf, uint64_t len);
extern void          ec_encode_data_base(int len, int srcs, int dests, unsigned char *v,unsigned char **src, unsigned char **dest);
#endif


// internal structures

typedef enum {
  BQ_FINISHED = 0x01 << 0, // signals to threads that all work has been issued and/or completed
  BQ_ABORT    = 0x01 << 1, // signals to threads that an unrecoverable errror requires early termination
  BQ_HALT     = 0x01 << 2  // signals to threads that work should be paused
} BQ_Control_Flags;

typedef enum {
  BQ_OPEN     = 0x01 << 0, // indicates that this thread has successfully opened its data file
  BQ_HALTED   = 0x01 << 1  // indicates that this thread is 'paused'
} BQ_State_Flags;


typedef struct buffer_queue {
  pthread_mutex_t    qlock;
  void*              buffers[MAX_QDEPTH];    /* array of buffers for passing data on the queue */
  //char               buf_error[MAX_QDEPTH];  /* indicates  errors associated with specific data buffers */
  pthread_cond_t     thread_resume;          /* cv signals there is a full slot */
  pthread_cond_t     master_resume;         /* cv signals there is an empty slot */
  int                qdepth;             /* number of elements in the queue */
  int                head;               /* next full position */  
  int                tail;               /* next empty position */
  BQ_Control_Flags   con_flags;          /* meant for sending thread commands */
  BQ_State_Flags     state_flags;        /* meant for signaling thread status */
  size_t             buffer_size;        /* size of an individual data buffer */
  struct GenericFD   file;               /* file descriptor */
  char               path[2048];         /* path to the file */
  int                block_number;       /* block num (within stripe) of the file for this queue */
  int                block_number_abs;   /* absolute block corresponding to block_number */
  ne_handle          handle;             /* pointer back up to the ne_handle */
  off_t              offset;             /* for write - amount of partial block 
                                             that has been stored in the buffer[tail]
                                            for read - current offset within the 
                                             block file */
} BufferQueue;


// This struct is intended to allow read threads to pass
// meta-file/xattr info back to the ne_open() function
typedef struct read_meta_buffer_struct {
   int N;
   int E;
   int O;
   unsigned int bsz;
   unsigned long nsz;
   u64 totsz;
}* read_meta_buffer;



struct handle {
   /* Erasure Info */
   e_state erasure_state;
   char  alloc_state;    // indicates whether we allocated the e_state struct or not
   //char* path_fmt;

   /* Read/Write Info and Structures */
   ne_mode mode;
   unsigned long buff_rem;
   off_t buff_offset;

   /* Threading fields */
   void *buffer_list[MAX_QDEPTH];
   void *block_buffs[MAX_QDEPTH][MAXPARTS];
   pthread_t threads[MAXPARTS];
   BufferQueue blocks[MAXPARTS];
   unsigned int ethreads_running;

   /* Erasure Manipulation Structures */
   unsigned char e_ready;
   unsigned char prev_in_err[ MAXPARTS ];
   unsigned int  prev_err_cnt;
   unsigned char *encode_matrix;
   unsigned char *decode_matrix;
   unsigned char *invert_matrix;
   unsigned char *g_tbls;
   unsigned char  decode_index[ MAXPARTS ];

   /* Used for rebuilds to restore the original ownership to the rebuilt file. */
   uid_t owner;
   gid_t group;

   /* path-printing technique provided by caller */
   SnprintfFunc   snprintf;
   void*          printf_state;        // caller-data to be provided to <snprintf>

   /* pass-through to RDMA/sockets impl */
   SktAuth        auth;

   /* run-time dispatch of sockets versus file implementation */
   const uDAL*    impl;

   /* optional timing/benchmarking */
   TimingData     timing_data;
   TimingData*    timing_data_ptr;  /* caller of ne_open() can provide their own TimingData
                                       (e.g. so data can survive ne_close()) */
};


void bq_destroy(BufferQueue *bq) {
  // XXX: Should technically check these for errors (ie. still locked)
  pthread_mutex_destroy(&bq->qlock);
  pthread_cond_destroy(&bq->thread_resume);
  pthread_cond_destroy(&bq->master_resume);
}

int bq_init(BufferQueue *bq, int block_number, ne_handle handle) {
//  int i;
//  for(i = 0; i < MAX_QDEPTH; i++) {
//    bq->buffers[i] = buffers[i];
//  }

  bq->block_number     = block_number;
  bq->qdepth           = 0;
  bq->head             = 0;
  bq->tail             = 0;
  bq->con_flags        = 0;
  bq->state_flags      = 0;
  bq->buffer_size      = handle->erasure_state->bsz;
  bq->handle           = handle;
  bq->offset           = 0;

  FD_INIT(bq->file, handle);

  if( handle->mode == NE_RDONLY || handle->mode == NE_RDALL || handle->mode == NE_STAT ) {
    // initialize all read threads in a halted state
    bq->con_flags |= BQ_HALT;
    // allocate space for the manifest info
    bq->buffers[0] = malloc( sizeof( struct read_meta_buffer_struct ) );
    if ( bq->buffers[0] == NULL ) {
      return -1;
    }
  }

  if(pthread_mutex_init(&bq->qlock, NULL)) {
    PRINTerr("failed to initialize mutex for qlock\n");
    return -1;
  }
  if(pthread_cond_init(&bq->thread_resume, NULL)) {
    PRINTerr("failed to initialize cv for thread_resume\n");
    // should also destroy the mutex
    pthread_mutex_destroy(&bq->qlock);
    return -1;
  }
  if(pthread_cond_init(&bq->master_resume, NULL)) {
    PRINTerr("failed to initialize cv for master_resume\n");
    pthread_mutex_destroy(&bq->qlock);
    pthread_cond_destroy(&bq->thread_resume);
    return -1;
  }

  return 0;
}

void bq_signal(BufferQueue* bq, BQ_Control_Flags sig) {
  pthread_mutex_lock(&bq->qlock);
  PRINTdbg("signalling 0x%x to block %d\n", (uint32_t)sig, bq->block_number);
  bq->con_flags |= sig;
  pthread_cond_signal(&bq->thread_resume);
  pthread_mutex_unlock(&bq->qlock);  
}

void bq_close(BufferQueue *bq) {
  bq_signal(bq, BQ_FINISHED);
}

void bq_abort(BufferQueue *bq) {
  bq_signal(bq, BQ_ABORT);
}


void bq_finish(void* arg) {
  BufferQueue *bq = (BufferQueue *)arg;
  PRINTdbg("exiting thread for block %d, in %s\n", bq->block_number, bq->path);
}


void *bq_writer(void *arg) {
  BufferQueue *bq      = (BufferQueue *)arg;
  ne_handle    handle  = bq->handle;

  TimingData*  timing = handle->timing_data_ptr;
  int          server = bq->block_number_abs; // absolute "block number" of the server

  size_t       written = 0;
  int          error;
  char         aborted = 0; // set to 1 on abort and 2 on pthread lock error

  char* meta_status = &(handle->erasure_state->meta_status[bq->block_number]);
  char* data_status = &(handle->erasure_state->data_status[bq->block_number]);

#ifdef INT_CRC
  const int write_size = bq->buffer_size + sizeof(u32);
#else
  const int write_size = bq->buffer_size;
#endif

  if (timing->flags & TF_THREAD)
     fast_timer_start(&timing->stats[server].thread);
  
  // debugging, assure we see thread entry/exit, even via cancellation
  PRINTdbg("entering write thread for block %d, in %s\n", bq->block_number, bq->path);
  pthread_cleanup_push(bq_finish, bq);

// ---------------------- OPEN DATA FILE ----------------------

  if (timing->flags & TF_OPEN)
     fast_timer_start(&timing->stats[server].open);

  // open the file.
  OPEN(bq->file, handle->auth, handle->impl, bq->path, O_WRONLY|O_CREAT, 0666);

  if (timing->flags & TF_OPEN)
  {
     fast_timer_stop(&timing->stats[server].open);
     log_histo_add_interval(&timing->stats[server].open_h,
                            &timing->stats[server].open);
  }

  PRINTdbg("open issued for file %d\n", bq->block_number);

// ---------------------- INITIALIZE MAIN PROCESS LOOP ----------------------

  // use 'read' to time how long we spend waiting to pull work off our queue
  // I've moved this outside the critical section, partially to spend less time
  // holding the queue lock, and partially because time spent waiting for our 
  // queue lock to be released is indicative of ne_write() copying data around
  if (timing->flags & TF_RW)
     fast_timer_start(&timing->stats[server].read);

  if(pthread_mutex_lock(&bq->qlock) != 0) {
    PRINTerr("failed to lock queue lock: %s\n", strerror(error));
    // outside of critical section, but should be fine as flags aren't shared
    *data_status = 1;
    aborted = 2;
  }
  // only set BQ_OPEN after aquiring the queue lock
  // this will allow initialize_queues() to complete and ne_open to reset umask
  bq->state_flags |= BQ_OPEN;
  // this is intended to avoid any oddities from instruction re-ordering
  if(FD_ERR(bq->file)) {
    PRINTerr("failed to open data file for block %d\n", bq->block_number);
    *data_status = 1;
  }
  pthread_cond_signal(&bq->master_resume);
  // As no work could have been queued yet, we'll give up the lock as soon as we hit the main loop
  
  while( !(aborted) ) { //condition check used only to skip the main loop if we failed to get the lock above


// ---------------------- CHECK FOR SPECIAL CONDITIONS ----------------------

    // the thread should always be holding its queue lock at this point

    // check for any states that require us to wait on the master proc, but allow a FINISHED or ABORT signal to break us out
    while ( ( bq->qdepth == 0  ||  (bq->con_flags & BQ_HALT) )
            &&  !((bq->con_flags & BQ_FINISHED) || (bq->con_flags & BQ_ABORT)) ) {
      // note the halted state if we were asked to pause
      if ( bq->con_flags & BQ_HALT ) {
         bq->state_flags |= BQ_HALTED;
         // the master proc could have been waiting for us to halt, so we must signal
         pthread_cond_signal(&bq->master_resume);
         // a reseek is comming, reset our internal/handle values to account for it
         written = 0;
         handle->erasure_state->csum[bq->block_number] = 0;
      }

      // wait on the thread_resume condition
      PRINTdbg("bq_writer[%d]: waiting for signal from ne_write\n", bq->block_number);
      pthread_cond_wait(&bq->thread_resume, &bq->qlock);

      // if we were halted, make sure we immediately indicate that we aren't any more 
      // and reseek our input file.
      if ( bq->state_flags & BQ_HALTED ) {
         PRINTdbg( "thread %d is resuming\n", bq->block_number );
         // let the master proc know that we are no longer halted
         bq->state_flags &= ~(BQ_HALTED);
         if ( (bq->offset == 0)  &&  !(FD_ERR(bq->file)) ) {
            // reseek our input file, if possible
            // Note: technically, it may be possible to unset any data error here.
            // Currently, I don't think there's a benefit to this though.
            if ( bq->offset != HNDLOP(lseek, bq->file, bq->offset, SEEK_SET) ) {
               PRINTerr( "thread %d is entering an unwritable state (seek error)\n", bq->block_number );
               *data_status = 1;
            }
            else {
               PRINTdbg( "thread %d has successfully reseeked its output file and will reset any data errors\n", bq->block_number );
               *data_status = 0; // a successful reseek to zero and a good file descriptor means we can safely attempt writes again
            }
         }
      }
    }

    // check for flags that might tell us to quit
    if(bq->con_flags & BQ_ABORT) {
      PRINTerr("thread %d is aborting\n", bq->block_number);
      // make sure no one thinks we finished properly
      *data_status = 1;
      pthread_mutex_unlock(&bq->qlock);
      // note that we should unlink the destination
      aborted = 1;
      // let the post-loop code cleanup after us
      break;
    }

    if((bq->qdepth == 0) && (bq->con_flags & BQ_FINISHED)) {       // then we are done.
      PRINTdbg("BQ_writer completed all work for block %d\n", bq->block_number);
      pthread_mutex_unlock(&bq->qlock);
      break;
    }
    pthread_mutex_unlock(&bq->qlock);

    // stop the 'read' timer once we have work to do
    if (timing->flags & TF_RW) {
       fast_timer_stop(&timing->stats[server].read);
       log_histo_add_interval(&timing->stats[server].read_h,
                              &timing->stats[server].read);
    }


    if( !(*data_status) ) {

// ---------------------- WRITE TO THE DATA FILE ----------------------

      if (timing->flags & TF_RW)
         fast_timer_start(&timing->stats[server].write);

// removing this since the newer NFS clients are better behaved
/*
      if(written >= SYNC_SIZE) {
         if ( HNDLOP(fsync, bq->file) )
            *data_status = 1;
         written = 0;
      }

      PRINTdbg("Writing block %d\n", bq->block_number);
*/

      *(u32*)( bq->buffers[bq->head] + bq->buffer_size )   = crc32_ieee(TEST_SEED, bq->buffers[bq->head], bq->buffer_size);
      error     = write_all(&bq->file, bq->buffers[bq->head], write_size);
      handle->erasure_state->csum[bq->block_number] += *(u32*)( bq->buffers[bq->head] + bq->buffer_size );

      PRINTdbg("write done for block %d at offset %zd\n", bq->block_number, written);
      if (timing->flags & TF_RW) {
         fast_timer_stop(&timing->stats[server].write);
         log_histo_add_interval(&timing->stats[server].write_h,
                                &timing->stats[server].write);
      }

    }
    else { // there were previous errors. skipping the write
      error = write_size;
    }

    if(error < write_size) {
      *data_status = 1;
    }
    else {
      // track data written to this block
      written += bq->buffer_size;
    }

// ---------------------- CLEAR ENTRY FROM THE BUFFER QUEUE ----------------------

    // use 'read' to time how long it takes us to receive work
    if (timing->flags & TF_RW)
       fast_timer_start(&timing->stats[server].read);

    // get the queue lock so that we can adjust the head position
    if((error = pthread_mutex_lock(&bq->qlock)) != 0) {
      PRINTerr("failed to lock queue lock: %s\n", strerror(error));
      // note the error
      *data_status = 1;
      // set the aborted flag, for consistency
      aborted = 2;
      // let the post-loop code cleanup after us
      break;
    }

    // even if there was an error, say we wrote the block and move on.
    // the producer thread is responsible for checking the error flag
    // and killing us if needed.

    bq->head = (bq->head + 1) % MAX_QDEPTH;
    bq->qdepth--;
    PRINTdbg( "completed a work buffer, set queue depth to %d and queue head to %d\n", bq->qdepth, bq->head );

    if ( bq->qdepth == MAX_QDEPTH - 1 )
       pthread_cond_signal(&bq->master_resume);
  }

// ---------------------- END OF MAIN PROCESS LOOP ----------------------

  // should have already relinquished the queue lock before breaking out

  // stop the 'read' timer, which will still be running after any loop breakout
  if (timing->flags & TF_RW) {
    fast_timer_stop(&timing->stats[server].read);
    log_histo_add_interval(&timing->stats[server].read_h,
                           &timing->stats[server].read);
  }

  // for whatever reason, this thread's work is done
  // close the file, so long as we didn't fail to open it in the first place
  int close_rc = 1;
  if ( !(FD_ERR(bq->file)) ) {
     if (timing->flags & TF_CLOSE)
        fast_timer_start(&timing->stats[server].close);
     close_rc = HNDLOP(close, bq->file);
     if (timing->flags & TF_CLOSE) {
        fast_timer_stop(&timing->stats[server].close);
        log_histo_add_interval(&timing->stats[server].close_h,
                               &timing->stats[server].close);
     }
  }

  // this should catch any errors from close or within the main loop as well as the case where this thread was 
  // unused by ne_rebuild()
  if ( close_rc  ||  (*data_status)  ||  (bq->con_flags & BQ_HALT) ) {
    if ( close_rc ) {
       PRINTerr("error closing block %d\n", bq->block_number);
       *data_status = 1;   // ensure the error was noted
       *meta_status = 1;   // skipping any attempt to set the meta info
    }
    else if ( (*data_status) ) {
       PRINTerr("early termination due to previous data error for block %d\n", bq->block_number);
       *meta_status = 1;   // skipping any attempt to set the meta info
    }
    else {
       PRINTdbg( "thread %d was never used and will terminate early\n", bq->block_number );
    }

    // check if the write has been aborted
    if ( aborted == 1  ||  (bq->con_flags & BQ_HALT) )
      PATHOP(unlink, handle->impl, handle->auth, bq->path); // try to clean up after ourselves.

    if (timing->flags & TF_THREAD)
       fast_timer_stop(&timing->stats[server].thread);

    return NULL; // don't bother trying to rename
  }

// ---------------------- STORE META INFO ----------------------

  // set our part size based on how much we have written
  handle->erasure_state->ncompsz[ bq->block_number ] = written;

  if(set_block_xattr(bq->handle, bq->block_number) != 0) {
    *meta_status = 1;
    // if we failed to set the xattr, don't bother with the rename.
    PRINTerr("error setting xattr for block %d\n", bq->block_number);
    if (timing->flags & TF_THREAD)
       fast_timer_stop(&timing->stats[server].thread);
    return NULL;
  }

// ---------------------- RENAME OUTPUT FILES TO FINAL LOCATIONS ----------------------

  // rename
  char block_file_path[MAXNAME];
  //  sprintf( block_file_path, handle->path_fmt,
  //           (bq->block_number+handle->erasure_state->O)%(handle->erasure_state->N+handle->erasure_state->E) );
  handle->snprintf( block_file_path, MAXNAME, handle->path_fmt,
                    (bq->block_number+handle->erasure_state->O)%(handle->erasure_state->N+handle->erasure_state->E), handle->printf_state );

  PRINTdbg("bq_writer: renaming old:  %s\n", bq->path );
  PRINTdbg("                    new:  %s\n", block_file_path );

  if (timing->flags & TF_RENAME)
     fast_timer_start(&timing->stats[server].rename);

  if( PATHOP( rename, handle->impl, handle->auth, bq->path, block_file_path ) != 0 ) {
    PRINTerr("bq_writer: rename failed: %s\n", strerror(errno) );
    *data_status = 1;
  }

  if (timing->flags & TF_RENAME)
     fast_timer_stop(&timing->stats[server].rename);

  if ( handle->mode == NE_REBUILD ) {
    PRINTdbg( "performing chown of rebuilt file \"%s\"\n", block_file_path );
    PATHOP(chown, handle->impl, handle->auth, block_file_path, handle->owner, handle->group);
  }

#ifdef META_FILES
  // rename the META file too
  strncat( bq->path, META_SFX, strlen(META_SFX)+1 );
  strncat( block_file_path, META_SFX, strlen(META_SFX)+1 );

  PRINTdbg("bq_writer: renaming meta old:  %s\n", bq->path );
  PRINTdbg("                         new:  %s\n", block_file_path );

  if (timing->flags & TF_RENAME)
     fast_timer_start(&timing->stats[server].rename);

  if ( PATHOP( rename, handle->impl, handle->auth, bq->path, block_file_path ) != 0 ) {
     PRINTerr("bq_writer: rename failed: %s\n", strerror(errno) );
     *meta_status = 1;
  }

  if (timing->flags & TF_RENAME)
     fast_timer_stop(&timing->stats[server].rename);

  if ( handle->mode == NE_REBUILD ) {
    PRINTdbg( "performing chown of rebuilt meta file \"%s\"\n", block_file_path );
    PATHOP(chown, handle->impl, handle->auth, block_file_path, handle->owner, handle->group);
  }

#endif

  if (timing->flags & TF_THREAD)
     fast_timer_stop(&timing->stats[server].thread);

  pthread_cleanup_pop(1);
  return NULL;
}



void* bq_reader(void* arg) {
   BufferQueue* bq      = (BufferQueue *)arg;
   ne_handle    handle  = bq->handle;

   TimingData*  timing  = handle->timing_data_ptr;
   int          server = bq->block_number_abs;

   int          error   = 0;

   if (timing->flags & TF_THREAD)
      fast_timer_start(&timing->stats[server].thread);
  
   // debugging, assure we see thread entry/exit, even via cancellation
   PRINTdbg("entering read thread for block %d, in %s\n", bq->block_number, bq->path);
   pthread_cleanup_push(bq_finish, bq);

// ---------------------- READ META INFO ----------------------

   // we will use the first of our buffers as a container for our manifest values
   read_meta_buffer meta_buf = (read_meta_buffer) bq->buffers[0];
   // initialize all values to indicate errors
   meta_buf->N = 0;
   meta_buf->E = -1;
   meta_buf->O = -1;
   meta_buf->bsz = 0;
   meta_buf->nsz = 0;
   meta_buf->totsz = 0;
   char* meta_status = &(handle->erasure_state->meta_status[bq->block_number]);
   char* data_status = &(handle->erasure_state->data_status[bq->block_number]);

   // pull the meta info for this thread's block
   char metaval[METALEN];
   if ( ne_get_xattr1( handle->impl, handle->auth, bq->path, metaval, METALEN ) < 0 ) {
      PRINTerr( "bq_reader: failed to retrieve meta info for file \"%s\"\n", bq->path );
      *meta_status = 1;
   }
   else {
      PRINTdbg( "retrieved meta string for block %d (file %s): \"%s\"\n", bq->block_number, bq->path, metaval );
      // declared here so that the compiler can hopefully free up this memory outside of the 'else' block
      char xattrN[5];            /* char array to get n parts from xattr */
      char xattrE[5];            /* char array to get erasure parts from xattr */
      char xattrO[5];            /* char array to get erasure_offset from xattr */
      char xattrbsz[20];         /* char array to get chunksize from xattr */
      char xattrnsize[20];       /* char array to get total size from xattr */
      char xattrncompsize[20];   /* char array to get ncompsz from xattr */
      char xattrcsum[50];        /* char array to get check-sum from xattr */
      char xattrtotsize[160];    /* char array to get totsz from xattr */

      // only process the xattr if we successfully retreived it
      int ret = sscanf(metaval,"%4s %4s %4s %19s %19s %19s %49s %159s",
            xattrN,
            xattrE,
            xattrO,
            xattrbsz,
            xattrnsize,
            xattrncompsize,
            xattrcsum,
            xattrtotsize);
      if (ret != 8) {
         PRINTerr( "bq_reader: sscanf parsed only %d values from meta info of block %d: \"%s\"\n", ret, bq->block_number, metaval);
         *meta_status = 1;
      }

      char* endptr;
      // simple macro to save some repeated lines of code
      // this is used to parse all meta values, check for errors, and assign them to their appropriate locations
#define PARSE_VALUE( VAL, STR, GT_VAL, PARSE_FUNC, TYPE ) \
      if ( ret > GT_VAL ) { \
         TYPE tmp_val = (TYPE) PARSE_FUNC ( STR, &(endptr), 10 ); \
         if ( *(endptr) == '\0'  &&  (tmp_val > VAL ) ) { \
            VAL = tmp_val; \
         } \
         else { \
            PRINTerr( "bq_reader: failed to parse meta value at position %d for block %d: \"%s\"\n", GT_VAL, bq->block_number, STR ); \
            *meta_status = 1; \
         } \
      }
      // N, E, O, bsz, and nsz are global values, and thus need to go in the meta_buf
      PARSE_VALUE( meta_buf->N, xattrN, 0, strtol, int )
      PARSE_VALUE( meta_buf->E, xattrE, 1, strtol, int )
      PARSE_VALUE( meta_buf->O, xattrO, 2, strtol, int )
      PARSE_VALUE( meta_buf->bsz, xattrbsz, 3, strtoul, unsigned int )
      PARSE_VALUE( meta_buf->nsz, xattrnsize, 4, strtoul, unsigned int )
      // ncompsz and csum are considered 'per-part' info, and thus can just be stored to the handle struct
      PARSE_VALUE( handle->erasure_state->ncompsz[ bq->block_number ], xattrncompsize, 5, strtoul, unsigned int )
      PARSE_VALUE( handle->erasure_state->csum[ bq->block_number ], xattrcsum, 6, strtoull, u64 )
      // totsz is global, so this needs to go into the meta_buf
      PARSE_VALUE( meta_buf->totsz, xattrtotsize, 7, strtoull, u64 )
   }

// ---------------------- OPEN DATA FILE ----------------------

   if (timing->flags & TF_OPEN)
      fast_timer_start(&timing->stats[server].open);

   OPEN( bq->file, handle->auth, handle->impl, bq->path, O_RDONLY );

   if (timing->flags & TF_OPEN)
   {
      fast_timer_stop(&timing->stats[server].open);
      log_histo_add_interval(&timing->stats[server].open_h,
                            &timing->stats[server].open);
   }

   PRINTdbg("opened file %d\n", bq->block_number);

// ---------------------- INITIALIZE MAIN PROCESS LOOP ----------------------

   char   aborted = 0;         // set to 1 on pthread_lock error
   char   resetable_error = 0; // set on read error, reset on successful re-seek
   char   permanent_error = 0; // set on failure to open file or global crc mismatch, never cleared

   // use 'write' to time how long we spend waiting to push buffers to our queue
   // I've moved this outside the critical section, partially to spend less time
   // holding the queue lock, and partially because time spent waiting for our 
   // queue lock to be released is indicative of ne_read() copying data around
   if (timing->flags & TF_RW)
      fast_timer_start(&timing->stats[server].write);

   // attempt to lock the queue before beginning the main loop
   if((error = pthread_mutex_lock(&bq->qlock)) != 0) {
      PRINTerr("failed to lock queue lock: %s\n", strerror(error));
      // note the error
      *data_status = 1;
      aborted = 1;
   }
   // only set BQ_OPEN after aquiring the queue lock
   // this will allow initialize_queues() to complete and ne_open to reset umask
   bq->state_flags |= BQ_OPEN;
   // this is intended to avoid any oddities from instruction re-ordering
   if(FD_ERR(bq->file)) {
      PRINTerr( "failed to open data file for block %d: \"%s\"\n", bq->block_number, bq->path );
      *data_status = 1;
      permanent_error = 1;
      resetable_error = 1; // while not actually resetable, we need to set this to avoid any read attempts on the bad FD
   }
   // As we should have initialized in a halted state, we'll signal and give up the lock as soon as we hit the main loop

   // local value for the files crcsum
   u64 local_crcsum = 0;
   // flag to indicate if we can trust our local crcsum for the file
   char good_crc = 1;
   // used to indicate read return values
   size_t error = 0;
   // used to calculate the end of the file.  However, we are not yet sure that values needed for determining this are set
   // in the handle.  Therefore, initialize this to zero, then reset once we have been 'resumed' by the master proc below.
   int num_stripes = 0;

   while ( !(aborted) ) { // condition check used just to skip the main loop if we failed to get the queue lock

// ---------------------- CHECK FOR SPECIAL CONDITIONS ----------------------

      // the thread should always be holding its queue lock at this point

      // check for any states that require us to wait on the master proc, but allow a FINISHED or ABORT signal to break us out.
      // Note, it is tempting to wait on 'resetable_error' here; however, we depend upon this thread to set error states for 
      // all buffers and to advance the queue.  Otherwise, ne_read() would be forced to assume that this thread is just really 
      // darn slow.
      while ( ( bq->qdepth == MAX_QDEPTH  ||  (bq->con_flags & BQ_HALT) )
              &&  !((bq->con_flags & BQ_FINISHED) || (bq->con_flags & BQ_ABORT)) ) {
         // note the halted state if we were asked to pause
         if ( bq->con_flags & BQ_HALT ) {
            bq->state_flags |= BQ_HALTED;
            // the master proc could have been waiting for us to halt, so we must signal
            pthread_cond_signal(&bq->master_resume);
            good_crc = 0; // a reseek is comming, so assume we can't use our crcsum
         }

         // wait on the thread_resume condition
         PRINTdbg("bq_reader[%d]: waiting for signal from ne_read\n", bq->block_number);
         pthread_cond_wait(&bq->thread_resume, &bq->qlock);

         // if we were halted, we have some housekeeping to take care of
         if ( bq->state_flags & BQ_HALTED ) {
            PRINTdbg( "thread %d is resuming\n", bq->block_number );
            // let the master proc know that we are no longer halted
            bq->state_flags &= ~(BQ_HALTED);
            // reseek our input file, if possible
            if ( !(permanent_error) ) {
               resetable_error = 0; // reseeking, so clear our temporary error state
               if ( bq->offset != HNDLOP(lseek, bq->file, bq->offset, SEEK_SET) ) {
                  PRINTerr( "thread %d is entering an unreadable state (seek error)\n", bq->block_number );
                  *data_status = 1;
                  resetable_error = 1; // do not attempt any reads until we successfully reseek
               }
               else if ( bq->offset == 0 ) {
                  good_crc = 1; // if we're starting from offset zero again, our crcsum is valid
                  local_crcsum = 0;
               }
            }
            // as the handle structs should now be initialized, calculate how large our file is expected to be
            if ( !( bq->con_flags & (BQ_FINISHED | BQ_ABORT) ) )
               num_stripes = (int)( ( handle->erasure_state->totsz - 1 ) / ( bq->buffer_size * (size_t)handle->erasure_state->N ) );
            // technically, this will be number of stripes minus 1, due to int truncation
         }
      }

      // check for flags that might tell us to quit
      if(bq->con_flags & BQ_ABORT) {
         PRINTerr("thread %d is aborting\n", bq->block_number);
         // make sure no one thinks we finished properly
         *data_status = 1;
         pthread_mutex_unlock(&bq->qlock);
         aborted = 1; //probably unnecessary
         // let the post-loop code cleanup after us
         break;
      }

      // if the finished flag is set, only terminate if we've hit an error, are still halted, or the queue is full.
      // This is mean to ensure that we have the chance to reach EOF and verify the global crc
      if( (bq->con_flags & BQ_FINISHED)
          && ( (bq->con_flags & BQ_HALT)  ||  !(good_crc)  ||  (bq->qdepth == MAX_QDEPTH) ) ) {
         PRINTdbg("BQ_reader %d finished\n", bq->block_number);
         pthread_mutex_unlock(&bq->qlock);
         break;
      }
      pthread_mutex_unlock(&bq->qlock);

      // stop the 'write' timer once we have work to do
      if (timing->flags & TF_RW) {
          fast_timer_stop(&timing->stats[server].write);
          log_histo_add_interval(&timing->stats[server].write_h,
                                 &timing->stats[server].write);
      }


// ---------------------- READ FROM DATA FILE ----------------------

      error = 0;

      // only read if we are at a good offset within the file
      if ( !(resetable_error) ) {

         u32 crc_val = 0;

         if (timing->flags & TF_RW)
            fast_timer_start(&timing->stats[server].read);

         error     = read_all(&bq->file, bq->buffers[bq->tail], bq->buffer_size + sizeof(u32));

         if (timing->flags & TF_RW) {
            fast_timer_stop(&timing->stats[server].read);
            log_histo_add_interval(&timing->stats[server].read_h,
                                   &timing->stats[server].read);
         }

         if ( error != bq->buffer_size + sizeof(u32) ) {
            PRINTerr( "read error for block %d at offset %zd\n", bq->block_number, bq->offset );
            *data_status = 1;
            resetable_error = 1;
            error = 0;
         }
         else {
            crc_val   = crc32_ieee(TEST_SEED, bq->buffers[bq->tail], bq->buffer_size);
            if ( memcmp( bq->buffers[bq->tail] + bq->buffer_size, &crc_val, sizeof(u32) ) ) {
               PRINTerr( "crc mismatch detected by block %d at offset %zd\n", bq->block_number, bq->offset );
               *data_status = 1;
               // this is why we need this 'error' value to persist outside the loop, to catch transient data errors.
               // No need to set the 'resetable_error' flag, as one bad block won't necessarily affect others.
               // Note that I've also elected not to invalidate our local crcsum here, as it doesn't really matter once 
               // we've already noted the error.
               bq->offset += error; // still increment offset
               error = 0;
            }
            else {
               // leave the crc value sitting in the buffer.  This will be a signal to ne_read() that the buffer is
               // usable.
               local_crcsum += crc_val;
               PRINTdbg("read done for block %d at offset %zd\n", bq->block_number, bq->offset);
            }
         }
      }

      if( error == 0 ) { //paradoxically, meaning there was an error...
         // zero out the crc position to indicate a bad buffer to ne_read()
         *(u32*)(bq->buffers[bq->tail] + bq->buffer_size) = 0;
         // any error means we can't trust our local crcsum any more
         good_crc = 0;
      }
      else {
         bq->offset += error;
      }

      // check if we are at the end of our file
      if ( !(resetable_error)  &&  ( ( bq->offset / ( bq->buffer_size + sizeof(u32) ) ) > num_stripes ) ) {
         PRINTdbg( "thread %d has reached the end of its data file ( offset = %zd )\n", bq->block_number, bq->offset );
         resetable_error = 1; // this should allow us to refuse any further buffers while avoiding a reported error

         // if we're at the end of the file, and both our local crcsum and the global are 'trustworthy', verify them
         if ( good_crc  &&  !(*meta_status)  &&  
               ( local_crcsum != handle->erasure_state->csum[bq->block_number] ) ) {
            *data_status = 1;
            // if the global doesn't match, something very odd is going on with this block.  Best to avoid reading it 
            // from now on.
            permanent_error = 1; // note: resetable flag already set above
            PRINTerr( "thread %d detected global crc mismatch ( data = %llu, meta = %llu )\n", 
                        bq->block_number, local_crcsum, handle->erasure_state->csum[bq->block_number] );
         }
      }


// ---------------------- ADD ENTRY TO BUFFER QUEUE ----------------------

      // use 'write' to time how long it takes us to receive work
      if (timing->flags & TF_RW)
         fast_timer_start(&timing->stats[server].write);

      // get the queue lock so that we can adjust the tail position
      if((error = pthread_mutex_lock(&bq->qlock)) != 0) {
         PRINTerr("failed to lock queue lock: %s\n", strerror(error));
         // note the error
         *data_status = 1;
         // set the aborted flag, for consistency
         aborted = 2;
         // let the post-loop code cleanup after us
         break;
      }

      // even if there was an error, just zero the crc and move on.
      // the master proc is responsible for checking the error flag
      // and killing us if needed.

      bq->tail = (bq->tail + 1) % MAX_QDEPTH;
      bq->qdepth++;
      PRINTdbg( "completed a work buffer, set queue depth to %d and queue tail to %d\n", bq->qdepth, bq->tail );

      // only signal if it is likely that the master has been waiting for a new buffer
      if ( bq->qdepth == 1 )
         pthread_cond_signal(&bq->master_resume);

   }

// ---------------------- END OF MAIN PROCESS LOOP ----------------------

   // should have already relinquished the queue lock before breaking out

   // stop the 'write' timer, which will still be running after any loop breakout
   if (timing->flags & TF_RW) {
      fast_timer_stop(&timing->stats[server].write);
      log_histo_add_interval(&timing->stats[server].write_h,
                           &timing->stats[server].write);
   }

   // for whatever reason, this thread's work is done.  close the file
   if (timing->flags & TF_CLOSE)
      fast_timer_start(&timing->stats[server].close);

   int close_rc = 1;

   // don't bother attempting to close a file we failed to open
   if ( ! FD_ERR( bq->file ) )
      close_rc = HNDLOP(close, bq->file);

   if (timing->flags & TF_CLOSE)
   {
      fast_timer_stop(&timing->stats[server].close);
      log_histo_add_interval(&timing->stats[server].close_h,
                            &timing->stats[server].close);
   }
   // at least note any close error, even though this shouldn't matter for reads
   if ( close_rc ) {
      PRINTerr("error closing block %d\n", bq->block_number);
      *data_status = 1;
   }

   // all done!
   if (timing->flags & TF_THREAD)
      fast_timer_stop(&timing->stats[server].thread);

   pthread_cleanup_pop(1);
   return NULL;
}


void terminate_threads( BQ_Control_Flags flag, ne_handle handle, int thread_cnt, int thread_offset ) {
   int i;
   /* wait for the threads */
   for(i = thread_offset; i < (thread_offset + thread_cnt); i++) {
      bq_signal( &handle->blocks[i], flag );
      pthread_join( handle->threads[i], NULL );
      bq_destroy( &handle->blocks[i] );
      PRINTdbg( "thread %d has terminated\n", i );
   }
}


void signal_threads( BQ_Control_Flags flag, ne_handle handle, int thread_cnt, int thread_offset ) {
   int i;
   /* wait for the threads */
   for(i = thread_offset; i < (thread_offset + thread_cnt); i++) {
      bq_signal( &handle->blocks[i], flag );
   }
}


// Used to resume a specific thread.
// Pulled out into a seperate func to allow ne_rebuild1_vl() to call it directly
void bq_resume( off_t offset, BufferQueue* bq, int queue_position ) {
   // TODO: should probably give these funcs a return value and actually do 
   // some error checking for this lock call
   pthread_mutex_lock( &bq->qlock );
   while( !(bq->state_flags & BQ_HALTED) ) {
      PRINTdbg( "master proc waiting on thread %d to signal\n", bq->block_number );
      pthread_cond_wait( &bq->master_resume, &bq->qlock );
   }

   // now that the thread is suspended, clear out the buffer queue
   bq->qdepth = 0;
   // Note: we set queue positions to a specific value in order to avoid any oddness with queues potentially
   // being misaligned from one another, which will break erasure.
   // as these buffers are consistently overwritten, we really just need to pretend we read them all
   bq->head = queue_position;
   bq->tail = queue_position;

   // use offset of zero in the special case of offset < 0 (unused rebuild thread)
   bq->offset = 0;

   if ( offset >= 0 ) {
      bq->offset = offset;
      // clear the HALT signal only if we got a valid offset (rebuild will give -1 offset to avoid this)
      PRINTdbg("clearing 0x%x signal for block %d\n", (uint32_t)BQ_HALT, bq->block_number);
      bq->con_flags &= ~(BQ_HALT);
      pthread_cond_signal( &bq->thread_resume );
   }
   pthread_mutex_unlock( &bq->qlock );
}


void resume_threads( off_t offset, ne_handle handle, int thread_cnt, int thread_offset, int queue_position ) {
   int i;
   /* wait for the threads */
   for(i = thread_offset; i < (thread_offset + thread_cnt); i++) {
      BufferQueue* bq = &handle->blocks[i];
      bq_resume( offset, bq, queue_position );
   }
}



/**
 * Initialize the buffer queues for the handle and start the threads.
 *
 * @return -1 on failure, 0 on success.
 */
static int initialize_queues(ne_handle handle) {
   int i;
   int num_blocks = handle->erasure_state->N + handle->erasure_state->E;

   struct read_meta_buffer_struct read_meta_state; //needed to determine metadata consensus for reads

   /* open files and initialize BufferQueues */
   for(i = 0; i < num_blocks; i++) {
      int error, file_descriptor;
      char path[MAXNAME];
      BufferQueue *bq = &handle->blocks[i];
      // handle all offset adjustments before calling bq_init()
      bq->block_number_abs = (i + handle->erasure_state->O) % num_blocks;
      // generate the path
      handle->snprintf(bq->path, MAXNAME, handle->path_fmt, bq->block_number_abs, handle->printf_state);

      if ( handle->mode == NE_REBUILD ) {
         strcat(bq->path, REBUILD_SFX);
         PRINTdbg( "starting up rebuild/write thread for block %d\n", i );
      }
      else if ( handle->mode == NE_WRONLY ) {
         strcat(bq->path, WRITE_SFX);
         PRINTdbg( "starting up write thread for block %d\n", i );
      }
      else {
         PRINTdbg( "starting up read thread for block %d\n", i );
      }

      if ( (i == 1)  &&  (strncmp(bq->path, handle->blocks[0].path, MAXNAME) == 0) ) {
         // sanity check, blocks should not have matching paths
         // I only bother to check for the first and second blocks, just to avoid simple mistakes (no %d in path).
         // More complex sprintf functions may still have collisions later on, but that is the responsibility of 
         // the caller, who gave us those functions.
         PRINTerr( "detected a name collision between blocks 0 and 1\n" );
         terminate_threads( BQ_ABORT, handle, i, 0 );
         errno = EINVAL;
         return -1;
      }
    
      if(bq_init(bq, i, handle) < 0) {
         PRINTerr("bq_init failed for block %d\n", i);
         terminate_threads( BQ_ABORT, handle, i, 0 );
         return -1;
      }
      // note that read threads will be initialized in a halted state

      // start the threads
      if ( handle->mode == NE_WRONLY  ||  handle->mode == NE_REBUILD )
         error = pthread_create(&handle->threads[i], NULL, bq_writer, (void *)bq);
      else
         error = pthread_create(&handle->threads[i], NULL, bq_reader, (void *)bq);
      if(error != 0) {
         PRINTerr("failed to start thread %d\n", i);
         terminate_threads( BQ_ABORT, handle, i, 0 );
         return -1;
      }
   }

   // We finish checking thread status down here in order to give them a bit more time to spin up.
   for(i = 0; i < num_blocks; i++) {

      BufferQueue *bq = &handle->blocks[i];

      PRINTdbg("Checking for error opening block %d\n", i);

      if ( pthread_mutex_lock( &bq->qlock ) ) {
         PRINTerr( "failed to aquire queue lock for thread %d\n", i );
         terminate_threads( BQ_ABORT, handle, num_blocks, 0 );
         return -1;
      }

      // wait for the queue to be ready.
      while( !( bq->state_flags & BQ_OPEN ) ) // wait for the thread to open its file
         pthread_cond_wait( &bq->master_resume, &bq->qlock );
      pthread_mutex_unlock( &bq->qlock ); // just waiting for open to complete, don't need to hold this longer

   }

   // For reads, find the most common values from amongst all meta information, now that all threads have started.
   // We have to do this here, in case we don't have a bsz value
   if ( handle->mode == NE_RDONLY  ||  handle->mode == NE_RDALL ) {
      int matches = check_matches( handle->blocks, i, &(read_meta_state) );
      // sanity check: if N/E values differ at this point, the meta info is in a very odd state
      if ( handle->erasure_state->N != read_meta_state.N  ||  handle->erasure_state->E != read_meta_state.E  ||  handle->erasure_state->O != read_meta_state.O ) {
         PRINTerr( "detected mismatch between provided N/E/O (%d/%d/%d) and the most common meta values for this stripe (%d/%d/%d)\n", 
                     handle->erasure_state->N, handle->erasure_state->E, handle->erasure_state->O, read_meta_state.N, read_meta_state.E, read_meta_state.O );
         terminate_threads( BQ_ABORT, handle, num_blocks, 0 );
         errno = EBADFD; // hopefully gives a bit more insight than just ENODATA
         if ( matches == 0 ) // no valid meta info, assume the file doesn't exist
            errno = ENOENT;
         return -1;
      }
      if ( !(handle->erasure_state->bsz) ) // only set bsz if not already specified via NE_SETBSZ mode
         handle->erasure_state->bsz = read_meta_state.bsz;
      handle->erasure_state->nsz = read_meta_state.nsz;
      handle->erasure_state->totsz = read_meta_state.totsz;
      // Note: ncompsz and crcsum are set by the thread itself
      for ( i = 0; i < num_blocks; i++ ) {
         // take this opportunity to mark all mismatched meta values as incorrect
         read_meta_buffer read_buf = (read_meta_buffer)handle->blocks[i].buffers[0];
         if ( read_buf->N != handle->erasure_state->N  ||  read_buf->E != handle->erasure_state->E  ||  read_buf->O != handle->erasure_state->O  ||  
              read_buf->bsz != handle->erasure_state->bsz  ||  read_buf->nsz != handle->erasure_state->nsz  ||  
              read_buf->totsz != handle->erasure_state->totsz ) {
            handle->erasure_state->meta_status[i] = 1;
            PRINTerr( "detected mismatch between accepted meta values and those read by block %d\n", i );
         }
         // free our read_meta_buff structs
         free( read_buf ); 
         // update each thread's buffer size, just in case
         handle->blocks[i].buffer_size = handle->erasure_state->bsz;
      }
   }


   /* allocate buffers */
   for(i = 0; i < MAX_QDEPTH; i++) {

      PRINTdbg( "creating buffer list for queue position %d\n", i );

      int j;
      // note, we always make space for a crc.  For writes, this is only needed if we are including 
      // intermediate-crcs.  For reads, we will use this extra space to indicate any data 
      // integrity errors for each buffer..
      int error = posix_memalign(&handle->buffer_list[i], 64,
                               num_blocks * ( handle->erasure_state->bsz + sizeof( u32 ) ) );
      if(error == -1) {
         // clean up previously allocated buffers and fail.
         // we can't recover from this error.
         for(j = i-1; j >= 0; j--) {
            free(handle->buffer_list[j]);
         }
         PRINTerr("posix_memalign failed for queue %d\n", i);
         errno = ENOMEM;
         return -1;
      }

      //void *buffers[MAX_QDEPTH];
      for(j = 0; j < num_blocks; j++) {
         handle->block_buffs[i][j] = handle->buffer_list[i] + ( j * ( handle->erasure_state->bsz + sizeof( u32 ) ) );
         // assign pointers into the memaligned buffers.
         handle->blocks[j].buffers[i] = handle->block_buffs[i][j];
      }

   }

   // finally, we have to give all necessary read threads the go-ahead to begin
   if( handle->mode == NE_RDONLY ) {
      resume_threads( 0, handle, handle->erasure_state->N, 0, 0 );
   }
   else if ( handle->mode == NE_RDALL ) {
      // for NE_RDALL, we immediately start both data and erasure threads
      resume_threads( 0, handle, handle->erasure_state->N + handle->erasure_state->E, 0, 0 );
      handle->ethreads_running  = handle->erasure_state->E;
   }

   return 0;
}


int bq_enqueue(BufferQueue *bq, const void *buf, size_t size) {
  int ret = 0;

  if((ret = pthread_mutex_lock(&bq->qlock)) != 0) {
    PRINTerr("Failed to lock queue for write\n");
    errno = ret;
    return -1;
  }

  while(bq->qdepth == MAX_QDEPTH)
    pthread_cond_wait(&bq->master_resume, &bq->qlock);

  // NOTE: _Might_ be able to get away with not locking here, since
  // access is controled by the qdepth var, which will not allow a
  // read until we say there is stuff here to be read.
  // 
  // bq->buffers[bq->tail] is a pointer to the beginning of the
  // buffer. bq->buffers[bq->tail] + bq->offset should be a pointer to
  // the inside of the buffer.
  //
  // Even if this is an unused rebuild thread, we still need to perform 
  // this memcpy so that the data is available for erasure generation.
  memcpy(bq->buffers[bq->tail]+bq->offset, buf, size);

  if(size+bq->offset < bq->buffer_size) {
    // then this is not a complete block.
    PRINTdbg("saved incomplete buffer for block %d at queue position %d\n", bq->block_number, bq->tail);
    bq->offset += size;
  }
  else {
    bq->offset = 0;
    bq->tail = (bq->tail + 1) % MAX_QDEPTH;
    PRINTdbg("queued complete buffer for block %d at queue position %d\n", bq->block_number, bq->tail);
    bq->qdepth++;
    pthread_cond_signal(&bq->thread_resume);
  }
  pthread_mutex_unlock(&bq->qlock);

  return 0;
}


ne_handle create_handle( SnprintfFunc fn, void* state,
                           uDALType itype, SktAuth auth,
                           TimingFlagsValue timing_flags, TimingData* timing_data,
                           char *path, ne_mode mode, e_state erasure_state ) {
   // this makes a convenient place to handle a NE_NOINFO case.
   // Just make sure not to infinite loop by calling ne_stat1() after it calls us!
   if ( ( mode & ~(NE_ESTATE) ) != NE_STAT  &&  !(erasure_state->N) ) {
      PRINTdbg( "using ne_stat1() to determine N/E/O values for handle\n" );
      // need to stash our bsz value, in case NE_SETBSZ was specified
      u32 bsz = erasure_state->bsz; 
      // if we don't have any N/E/O values, let ne_stat() do the work of determining those
      if ( ne_stat1( fn, state, itype, auth, timing_flags, timing_data, path, erasure_state ) < 0 )
         return NULL;
      // we now need to stash the values we care about, and clear the rest of this struct; 
      // otherwise, our meta-info/data errors may not take offset into account.
      int N = erasure_state->N;
      int E = erasure_state->E;
      int O = erasure_state->O;
      // completely clear the e_state struct to ensure no extra state is carried over
      memset( erasure_state, 0, sizeof( struct ne_state_struct ) );
      // then reassign the values we wish to preserve
      erasure_state->N = N;
      erasure_state->E = E;
      erasure_state->O = O;
      // if bsz was not set, this value will be overwritten in initialize_queues()
      erasure_state->bsz = bsz;
   }

   ne_handle handle = malloc( sizeof( struct handle ) );
   if ( handle == NULL )
      return handle;
   memset(handle, 0, sizeof(struct handle));

   /* initialize any non-zero handle members */
   handle->erasure_state = erasure_state;
   handle->buff_offset   = 0;
   handle->prev_err_cnt  = 0;
   handle->owner         = 0;
   handle->group         = 0;
   handle->alloc_state   = 1;
   // make sure to note the NE_ESTATE flag, if set
   if ( mode & NE_ESTATE )
      handle->alloc_state = 0;
   handle->mode = ( mode & ~(NE_ESTATE) ); // don't bother recoring ESTATE in the mode

   handle->snprintf     = fn;
   handle->printf_state = state;
   handle->auth         = auth;
   handle->impl         = get_impl(itype);

   if (! handle->impl) {
      PRINTerr( "create_handle: couldn't find implementation for itype %d\n", itype );
      free( handle );
      errno = EINVAL;
      return NULL;
   }

   handle->timing_data_ptr = (timing_data ? timing_data : &handle->timing_data);
   TimingData* timing      = handle->timing_data_ptr; // shorthand
   timing->flags           = timing_flags;

   if (timing->flags) {
      fast_timer_inits();

      // // redundant with memset() on handle
      // init_bench_stats(&handle->agg_stats);
   }
   if (timing->flags & TF_HANDLE)
      fast_timer_start(&timing->handle_timer); /* start overall timer for handle */

   char* nfile = malloc( strlen(path) + 1 );
   strncpy( nfile, path, strlen(path) + 1 );
   handle->path_fmt = nfile;

   return handle;
}


/**
 * Internal helper function, intended to parse a list of variadic ne_open() args
 * into the provided erasure_state struct and to strip parsing flags from the 
 * provided ne_mode value.
 * @param va_list ap : Variadic argument list for parsing
 * @param ne_mode* mode : Mode value to assist in parsing
 * @return e_state : e_state struct pointer with N/E/O/bsz values populated
 */
e_state parse_va_open_args( va_list ap, ne_mode* mode ) {

   e_state erasure_state = NULL;
   // if the NE_ESTATE flag was supplied, use the provided ne_state_struct
   if ( *mode & NE_ESTATE ) {
      erasure_state = va_arg( ap, e_state );
      // leave the NE_ESTATE flag intact, as we will need to remeber whether we allocated this ourselves later
   }
   else { // otherwise, allocate our own struct
      erasure_state = malloc( sizeof( struct ne_state_struct ) );
   }
   // check both for malloc failure and a NULL argument
   if ( erasure_state == NULL ) {
      errno = ENOMEM;
      return NULL;
   }

   // whether we allocated it or not, ensure there is no garbage data in our e_state struct
   memset( erasure_state, 0, sizeof( struct ne_state_struct ) );

   char noinfo = 0; // note if NE_NOINFO was provided

   // if the NE_NOINFO flag wasn't supplied, we expect O/N/E args
   if ( !(*mode & NE_NOINFO) ) {
      erasure_state->O = va_arg( ap, int );
      erasure_state->N = va_arg( ap, int );
      erasure_state->E = va_arg( ap, int );
   }
   else { // clear the NE_NOINFO flag
      *mode &= ~(NE_NOINFO);
      noinfo = 1;
   }
   // if the NE_SETBSZ was supplied, we expect a bsz arg
   if ( *mode & NE_SETBSZ ) {
      erasure_state->bsz = va_arg( ap, int );
      *mode &= ~(NE_SETBSZ); // clear the NE_SETBSZ flag
   }
   else { // otherwise, use the default value from our header file
      erasure_state->bsz = BLKSZ;
   }

   if ( ( (*mode & ~(NE_ESTATE)) == NE_WRONLY )  &&  (noinfo) ) {
      PRINTerr( "ne_open: recieved an invalid \"NE_NOINFO\" flag for \"NE_WRONLY\" operation\n");
      if (*mode & NE_ESTATE)
         free( erasure_state );
      errno = EINVAL;
      return NULL;
   }

#ifdef INT_CRC
   //shrink data size to fit crc within block
   erasure_state->bsz -= sizeof( u32 );
#endif

   if ( !(noinfo) ) {
      if ( erasure_state->N < 1  ||  erasure_state->N > MAXN ) {
         PRINTerr( "ne_open: improper N arguement received - %d\n", erasure_state->N);
         errno = EINVAL;
         return 0;
      }
      if ( erasure_state->E < 0  ||  erasure_state->E > MAXE ) {
         PRINTerr( "ne_open: improper E arguement received - %d\n", erasure_state->E);
         errno = EINVAL;
         return 0;
      }
      if ( erasure_state->O < 0  ||  erasure_state->O >= (erasure_state->N+erasure_state->E) ) {
         PRINTerr( "ne_open: improper erasure_offset arguement received - %d\n", erasure_state->O);
         errno = EINVAL;
         return 0;
      }
   }
   if ( erasure_state->bsz < 0  ||  erasure_state->bsz > MAXBLKSZ ) {
      PRINTerr( "ne_open: improper bsz argument received - %d\n", erasure_state->bsz );
      errno = EINVAL;
      return 0;
   }

   // return the new ne_state_struct
   return erasure_state;
}


/**
 * Opens a new handle for a specific erasure striping
 *
 * ne_open(path, mode, ...)  calls this with fn=ne_default_snprintf, and printf_state=NULL
 *
 * @param SnprintfFunc : function takes block-number and <state> and produces per-block path from template.
 * @param state : optional state to be used by SnprintfFunc (e.g. configuration details)
 * @param itype : uDAL implementation for low-level storage access
 * @param auth : optional credentials (actually AWSContext*) to authenticate socket connections (e.g. RDMA)
 * @param timing_flags : control collection of timing-data across various operations
 * @param timing_data_ptr : optional TimingData not in ne_handle (i.e. to survive ne_close())
 * @param char* path : sprintf format-template for individual files of in each stripe.
 * @param ne_mode mode : Mode in which the file is to be opened.  Either NE_RDONLY, NE_WRONLY, or NE_REBUILD.
 * @param int erasure_offset : Offset of the erasure stripe, defining the name of the first N file
 * @param int N : Data width of the striping
 * @param int E : Erasure width of the striping
 *
 * @return ne_handle : The new handle for the opened erasure striping
 */
int initialize_handle( ne_handle handle )
{
   // assign short names for easy reference (should never be altered)
   const int N = handle->erasure_state->N;
   const int E = handle->erasure_state->E;
   const int erasure_offset = handle->erasure_state->O;
   const u32 bsz = handle->erasure_state->bsz;
   PRINTdbg( "ne_open: using stripe values (N=%d,E=%d,bsz=%d,offset=%d)\n", N,E,bsz,erasure_offset);

   // umask is process-wide, so we have to manipulate it outside of the threads
   mode_t mask = umask(0000);
   if(initialize_queues(handle) < 0) {
     // all destruction/cleanup should be handled in initialize_queues()
     return -1;
   }
   umask(mask);
   int nerr = 0;
   int i;
   // quick loop to count up errors
   for ( i = 0; i < (handle->erasure_state->N + handle->erasure_state->E); i++ ) {
     if ( handle->erasure_state->data_status[i]  ||  handle->erasure_state->meta_status[i] )
       nerr++;
   }
   if( (handle->mode == NE_WRONLY || handle->mode == NE_REBUILD)  &&  UNSAFE(handle,nerr) ) {
     PRINTerr( "errors have rendered the handle unsafe to continue\n" );
     terminate_threads( BQ_ABORT, handle, handle->erasure_state->N + handle->erasure_state->E, 0 );
     errno = EIO;
     return -1; //don't hand out a dead handle!
   }

   /* allocate matrices */
   handle->encode_matrix = malloc(sizeof(char) * (N+E) * N);
   handle->decode_matrix = malloc(sizeof(char) * (N+E) * N);
   handle->invert_matrix = malloc(sizeof(char) * (N+E) * N);
   handle->g_tbls = malloc(sizeof(char) * N * E * 32);

   return 0;
}


// caller (e.g. MC-sockets DAL) specifies SprintfFunc, stat, and SktAuth
// New: caller also provides flags that control whether stats are collected
//
// <timing_data> can be NULL if caller doesn't want to provide their own
//    TimingData.  (The reason for doing so would be to survive
//    ne_close().)
ne_handle ne_open1( SnprintfFunc fn, void* state,
                    uDALType itype, SktAuth auth,
                    TimingFlagsValue timing_flags, TimingData* timing_data,
                    char *path, ne_mode mode, ... ) {
   va_list vl;
   va_start(vl, mode);
   // this function will parse args and zero out our e_state struct
   e_state erasure_state_tmp = parse_va_open_args( vl, &mode );
   va_end(vl);
   if ( erasure_state_tmp == NULL )
      return NULL; // check for failure condition, parse_va_open_args will set errno

   // this will handle allocating a handle and setting values
   ne_handle handle = create_handle( fn, state, itype, auth,
                                     timing_flags, timing_data,
                                     path, mode, erasure_state_tmp );
   if ( handle == NULL ) {
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      return handle;
   }

   // Note: handle->mode == ( mode & ~(NE_ESTATE) )
   if ( handle->mode != NE_WRONLY  &&  handle->mode != NE_RDONLY  &&  handle->mode != NE_RDALL ) { //reject improper mode arguments
      PRINTerr( "improper mode argument received - %d\n", handle->mode );
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      errno = EINVAL;
      return NULL;
   }

   if( initialize_handle(handle) ) {
      free( handle->path_fmt );
      free( handle );
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      return NULL;
   }
   return handle;
}


// provide defaults for SprintfFunc, printf_state, and SktAuth
// so naive callers can continue to work (in some cases).
ne_handle ne_open( char *path, ne_mode mode, ... ) {
   va_list vl;
   va_start(vl, mode);
   // this function will parse args and zero out our e_state struct
   e_state erasure_state_tmp = parse_va_open_args( vl, &mode );
   va_end(vl);
   if ( erasure_state_tmp == NULL )
      return NULL; // check for failure condition, parse_va_open_args will set errno

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return NULL;
   }

   // this will handle allocating a handle and setting values
   ne_handle handle = create_handle( ne_default_snprintf, NULL, UDAL_POSIX, auth,
                                     0, NULL,
                                     path, mode, erasure_state_tmp );
   if ( handle == NULL ) {
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      return NULL;
   }

   // Note: handle->mode == ( mode & ~(NE_ESTATE) )
   if ( handle->mode != NE_WRONLY  &&  handle->mode != NE_RDONLY  &&  handle->mode != NE_RDALL ) { //reject improper mode arguments
      PRINTerr( "improper mode argument received - %d\n", handle->mode );
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      errno = EINVAL;
      return NULL;
   }

   if( initialize_handle(handle) ) {
      free( handle->path_fmt );
      free( handle );
      if ( !(mode & NE_ESTATE) )
         free( erasure_state_tmp );
      return NULL;
   }
   return handle;
}



/**
 * Threaded read of nbytes of data at offset from the erasure striping referenced by the given handle
 * @param ne_handle handle : Open handle referencing the desired erasure striping
 * @param void* buffer : Memory location in which to store the retrieved data
 * @param int nbytes : Integer number of bytes to be read
 * @param off_t offset : Offset within the data at which to begin the read
 * @return int : The number of bytes read or -1 on a failure
 */
ssize_t ne_read( ne_handle handle, void *buffer, size_t nbytes, off_t offset ) {

   PRINTdbg( "called to retrieve %zu bytes at offset %zd\n", nbytes, offset );

// ---------------------- CHECK BOUNDARY AND INVALID CALL CONDITIONS ----------------------

   if ( !(handle) ) {
      PRINTerr( "ne_read: received a NULL handle!\n" );
      errno = EINVAL;
      return -1;
   }

   int N = handle->erasure_state->N;
   int E = handle->erasure_state->E;
   size_t bsz = handle->erasure_state->bsz;
   size_t stripesz = bsz * N;

   if (nbytes > UINT_MAX) {
     PRINTerr( "ne_read: not yet validated for write-sizes above %lu\n", UINT_MAX);
     errno = EFBIG;             /* sort of */
     return -1;
   }

   if ( handle->mode != NE_RDONLY  &&  handle->mode != NE_RDALL  &&  handle->mode != NE_REBUILD ) {
      PRINTerr( "ne_read: handle is in improper mode for reading!\n" );
      errno = EPERM;
      return -1;
   }

   if ( (offset + nbytes) > handle->erasure_state->totsz ) {
      if ( offset >= handle->erasure_state->totsz )
         return 0; //EOF
      nbytes = handle->erasure_state->totsz - offset;
      PRINTdbg("ne_read: read would extend beyond EOF, resizing read request to %zu\n", nbytes);
   }

// ---------------------- SKIP TO APPROPRIATE FILE STRIPE ----------------------

   int cur_stripe = (int)( offset / stripesz ); // I think int truncation actually works out in our favor here

   // if the offset is behind us or so far in front that there is no chance of the work having already been done...
   if ( (handle->buff_offset > ( cur_stripe * stripesz ))  ||  (( handle->buff_offset + ( MAX_QDEPTH * stripesz ) ) < ( cur_stripe * stripesz )) ) {
      PRINTdbg( "new offset of %zd will require threads to reseek\n", offset );
      // we need to halt all threads and trigger a reseek
      signal_threads( BQ_HALT, handle, N+E, 0 );
      resume_threads( (off_t)cur_stripe * ( bsz + sizeof(u32) ), handle, N + handle->ethreads_running, 0, 0 );
   }
   else {
      // we may still be at least a few stripes behind the given offset.  Calculate how many.
      int munch_stripes = (int)(( offset - handle->buff_offset ) / stripesz);
      // if we're at all behind...
      if (munch_stripes) {
         PRINTdbg( "attempting to 'munch' %d buffers off of all queues (%d) to reach offset of %zd\n", munch_stripes, N + handle->ethreads_running, offset );
         // just chuck buffers off of each queue until we hit the right stripe
         int i;
         for( i = 0; i < (N + handle->ethreads_running); i++ ) {
            int thread_munched = 0;

            // since the entire loop is a critical section, just lock around it
            if ( pthread_mutex_lock( &handle->blocks[i].qlock ) ) {
               PRINTerr( "failed to acquire queue lock for thread %d\n", i );
               return -1;
            }
            while ( thread_munched < munch_stripes ) {
               
               while ( handle->blocks[i].qdepth == 0 ) {
                  PRINTdbg( "waiting on thread %d to produce a buffer\n", i );
                  // releasing the lock here will let the thread get some work done
                  pthread_cond_wait( &handle->blocks[i].master_resume, &handle->blocks[i].qlock );
               }

               // remove as many buffers as we can without going beyond the appropriate stripe
               int orig_depth = handle->blocks[i].qdepth;
               handle->blocks[i].qdepth = ( (orig_depth - ( munch_stripes - thread_munched )) < 0 ) ? 0 : (orig_depth - ( munch_stripes - thread_munched ));
               handle->blocks[i].head = ( handle->blocks[i].head + ( orig_depth - handle->blocks[i].qdepth ) ) % MAX_QDEPTH;
               thread_munched += ( orig_depth - handle->blocks[i].qdepth );

               // make sure to signal the thread if we just made room for it to resume work
               if ( orig_depth == MAX_QDEPTH )
                  pthread_cond_signal( &handle->blocks[i].thread_resume );
            }
            pthread_mutex_unlock( &handle->blocks[i].qlock );
         }
         PRINTdbg( "finished pre-read buffer 'munching'\n" );
      }
   }

   // we are now on the proper stripe for all running queues, so update our offset to reflect that
   off_t orig_handle_offset = handle->buff_offset;
   handle->buff_offset = cur_stripe * stripesz;
   offset = offset - handle->buff_offset;

   PRINTdbg( "after reaching the appropriate stripe, read is at offset %zd\n", offset );

// ---------------------- BEGIN MAIN READ LOOP ----------------------

   // time to start actually filling this read request
   size_t bytes_read = 0;
   while( nbytes ) { // while data still remains to be read, loop over each stripe
      unsigned char stripe_in_err[MAXPARTS] = { 0 };
      unsigned char stripe_err_list[MAXPARTS] = { 0 };
      int nstripe_errors = 0;
      int nsrcerr = 0;
      size_t to_read_in_stripe = ( nbytes % stripesz ) - offset;
      // Note: It is ESSENTIAL for rebuilds that this 'read_full_stripe' remains zero if the read request exactly aligns to 
      //  the end of the stripe.
      char read_full_stripe = ( (offset + nbytes) > stripesz ) ? 1 : 0;
      int cur_block = 0;
      int skip_blocks = (int)( offset / bsz ); // determine how many blocks we need to skip over to hit the first requested data
      offset = offset % bsz; // adjust offset to be that within the first requested block

      PRINTdbg( "preparing to read from stripe %d beginning at offset %zd in block %d\n", cur_stripe, offset, skip_blocks );

      // sanity check, to ensure rebuilds are reading at stripe alignments only
      if ( handle->mode == NE_REBUILD  &&  read_full_stripe ) {
         PRINTerr( "detected an attempt to read beyond a single stripe during a rebuild operation, which is not permitted\n" );
         return -1;
      }

// ---------------------- VERIFY INTEGRITY OF ALL BLOCKS IN STRIPE ----------------------

      // First, loop through all data buffers in the stripe, looking for errors.
      // Then, starup erasure threads as necessary to deal with errors.
      // Technically, it would theoretically be more efficient to limit this to 
      // only the blocks we expect to need.  However, if we hit any error in those 
      // blocks, we'll suddenly need the whole stripe.  I am hoping the reduced 
      // complexity of just checking them all will be worth what will probably 
      // only be the slightest of performance hits.  Besides, in the expected 
      // use case of reading a file start to finish, we'll eventually need this 
      // entire stripe regardless.
      int ethreads_to_check = 0;
      while ( nstripe_errors > ethreads_to_check  ||  cur_block < N ) {
         // check if we can even handle however many errors we've hit so far
         if ( nstripe_errors > E ) {
            PRINTdbg( "stripe %d has too many data errors (%d) to be recovered\n", cur_stripe, nstripe_errors );
            errno=ENODATA;
            return -1;
         }
         int threads_to_start = (nstripe_errors - handle->ethreads_running); // previous check should insure we don't start more than E
         if ( threads_to_start > 0 ) { // ignore possible negative value
            PRINTdbg( "starting up %d erasure threads at offset (%zd) to cope with data errors\n", threads_to_start, (off_t)cur_stripe * bsz );
            // when starting up erasure threads, use block-zero's head position to keep us aligned to the rest of the stripe
            // as the thread itself should never adjust head position, we should be safe to reference it without a lock
            resume_threads( (off_t)cur_stripe * ( bsz + sizeof(u32) ), handle, threads_to_start, N + handle->ethreads_running, handle->blocks[0].head );
            handle->ethreads_running += threads_to_start;
         }

         // limith the number of erasure blocks we verify to the number we need for data regeneration... 
         // ...unless we are reading for a rebuild op, then we need the entire stripe.
         ethreads_to_check = ( handle->mode == NE_REBUILD ) ? handle->erasure_state->E : nstripe_errors;

         // we now need to check each needed block for errors.
         // Note that neglecting to reassign cur_block in this loop allows us to avoid 
         // rechecking threads when the outer while-loop repeats.
         for ( ; cur_block < (N + ethreads_to_check); cur_block++ ) {
            if ( pthread_mutex_lock( &handle->blocks[cur_block].qlock ) ) {
               PRINTerr( "failed to acquire queue lock for erasure thread %d\n", cur_block );
               return -1;
            }
            // if necessary, wait for the buffer to be ready
            while ( handle->blocks[cur_block].qdepth == 0 ) {
               PRINTdbg( "waiting on thread %d to produce a buffer\n", cur_block );
               // releasing the lock here will let the thread get some work done
               pthread_cond_wait( &handle->blocks[cur_block].master_resume, &handle->blocks[cur_block].qlock );
            }
            pthread_mutex_unlock( &handle->blocks[cur_block].qlock ); // just wanted the buffer to be ready, don't need to hold this

            // check for errors based on whether the crc position of this buffer was cleaned out
            if ( !(*(u32*)( handle->blocks[cur_block].buffers[ handle->blocks[cur_block].head ] + bsz )) ) {
               // a zero value in the crc position means the buffer is bad
               PRINTdbg( "detected bad buffer for block %d at stripe %d\n", cur_block, cur_stripe );
               stripe_err_list[nstripe_errors] = cur_block;
               nstripe_errors++;
               stripe_in_err[cur_block] = 1;
               // we just need to note the error, nothing to be done about it until we have all buffers ready
            }

            // make sure to note the number of data errors for later erasure use
            if ( cur_block == (N - 1) )
               nsrcerr = nstripe_errors;
         }
         PRINTdbg( "have checked %d blocks in stripe %d for errors\n", cur_block, cur_stripe );
      }

// ---------------------- HALT UNNECESSARY ERASURE THREADS ----------------------

      // if we'er trying to avoid unnecessary reads AND are not re-hitting the same stripe as a previous call...
      if ( handle->mode == NE_RDONLY  &&  handle->buff_offset != orig_handle_offset ) {
         // keep the greater of how many erasure threads we've needed in the last couple of stripes...
         if ( nstripe_errors > handle->prev_err_cnt )
            handle->prev_err_cnt = nstripe_errors;
         // ...and halt all others
         for ( cur_block = (N + nstripe_errors); cur_block < (N + handle->prev_err_cnt); cur_block++ ) {
            bq_signal( &handle->blocks[cur_block], BQ_HALT );
            handle->ethreads_running--;
         }
         // need to reassign, in case the number of errors is decreasing
         handle->prev_err_cnt = nstripe_errors;
      }

// ---------------------- REGENERATE FAULTY BLOCKS FROM ERASURE ----------------------

      // if necessary, engage erasure code to regenerate the missing buffers
      if ( nsrcerr  ||  ( handle->mode == NE_REBUILD  &&  nstripe_errors ) ) {

         // check if our erasure_state has changed, and invalidate our erasure_structs if so
         for ( cur_block = 0; cur_block < (N + E); cur_block++ ) {
            if ( handle->prev_in_err[ cur_block ] != stripe_in_err[ cur_block ] ) {
               handle->e_ready = 0;
               handle->prev_in_err[ cur_block ] = stripe_in_err[ cur_block ];
            }
         }

         if ( handle->timing_data_ptr->flags & TF_ERASURE )
            fast_timer_start(&handle->timing_data_ptr->erasure);

         if ( !(handle->e_ready) ) {

            // Generate an encoding matrix
            // NOTE: The matrix generated by gf_gen_rs_matrix is not always invertable for N>=6 and E>=5!
            PRINTdbg("initializing erasure structs...\n");
            gf_gen_rs_matrix(handle->encode_matrix, N+E, N);

            // Generate g_tbls from encode matrix
            ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);
            unsigned char tmpmatrix[ MAXPARTS * MAXPARTS ] = {0};
            int ret_code = gf_gen_decode_matrix_simple( handle->encode_matrix, handle->decode_matrix,
                  handle->invert_matrix, tmpmatrix, handle->decode_index, stripe_err_list, 
                  nstripe_errors, N, N+E);

            if (ret_code != 0) {
               PRINTerr("failure to generate decode matrix, errors may exceed erasure limits\n");
               errno=ENODATA;

               if ( handle->timing_data_ptr->flags & TF_ERASURE ) {
                  fast_timer_stop(&handle->timing_data_ptr->erasure);
                  log_histo_add_interval(&handle->timing_data_ptr->erasure_h,
                                         &handle->timing_data_ptr->erasure);
               }
               errno = ENODATA;
               return -1;
            }


            PRINTdbg( "init erasure tables nsrcerr = %d e_ready = %d...\n", nsrcerr, handle->e_ready );
            ec_init_tables(N, nstripe_errors, handle->decode_matrix, handle->g_tbls);

            handle->e_ready = 1; //indicate that rebuild structures are initialized
         }

         // as this struct will change depending on the head position of our queues, we must generate here
         unsigned char *recov[ MAXPARTS ];
         for (cur_block = 0; cur_block < N; cur_block++) {
            BufferQueue* bq = &handle->blocks[handle->decode_index[cur_block]];
            recov[cur_block] = bq->buffers[ bq->head ];
         }

         unsigned char* temp_buffs[ nstripe_errors ];
         for ( cur_block = 0; cur_block < nstripe_errors; cur_block++ ) {
            // assign storage locations for the repaired buffers to be on top of the faulty buffers
            BufferQueue* bq = &handle->blocks[stripe_err_list[ cur_block ]];
            temp_buffs[ cur_block ] = bq->buffers[ bq->head ];
            // as we are regenerating over the bad buffer, mark it as usable for future iterations
            *(u32*)( temp_buffs[ cur_block ] + bsz ) = 1;
         }

         PRINTdbg( "performing regeneration from erasure...\n" );

         ec_encode_data(bsz, N, nstripe_errors, handle->g_tbls, recov, &temp_buffs[0]);

         if ( handle->timing_data_ptr->flags & TF_ERASURE ) {
            fast_timer_stop(&handle->timing_data_ptr->erasure);
            log_histo_add_interval(&handle->timing_data_ptr->erasure_h,
                                   &handle->timing_data_ptr->erasure);
         }


      }

// ---------------------- COPY OUT REQUESTED PORTIONS OF THE STRIPE ----------------------

      // finally, copy all requested data into the buffer and clear unneeded queue entries
      for( cur_block = 0; cur_block <  N + handle->ethreads_running; cur_block++ ) {
         BufferQueue* bq = &handle->blocks[cur_block];

         // does this buffer contain requested data?
         if ( cur_block >= skip_blocks  &&  cur_block < N ) {
            // if so, copy it to our output buffer
            size_t to_copy = ( (bsz - offset) > nbytes ) ? nbytes : (bsz - offset);
            if ( buffer ) { // only actually copy out the data if we have a valid buffer
               // as no one but ne_read should be adjusting the head position, and we have already verified that this buffer is ready, 
               // we can safely copy from it without holding the queue lock
               PRINTdbg( "copying %zd bytes from thread %d's buffer at position %d to the output buff\n", to_copy, cur_block, bq->head );
               memcpy( buffer + bytes_read, bq->buffers[bq->head] + offset, to_copy );
            }
            nbytes -= to_copy;
            bytes_read += to_copy;
            offset = 0; // as we have copied from the first applicable value, this offset is no longer relevant
         }

         // if this write request extends beyond the current stripe, we need to clear out these queue entries
         if ( read_full_stripe ) {
            PRINTdbg( "clearing a buffer from thread %d's queue as our read offset is beyond it\n", cur_block );
            if ( pthread_mutex_lock( &handle->blocks[cur_block].qlock ) ) {
               PRINTerr( "failed to acquire queue lock for thread %d\n", cur_block );
               return -1;
            }
            // wait for a buffer to be produced, if necessary
            while ( handle->blocks[cur_block].qdepth == 0 ) {
               PRINTdbg( "waiting on thread %d to produce a buffer\n", cur_block );
               // releasing the lock here will let the thread get some work done
               pthread_cond_wait( &handle->blocks[cur_block].master_resume, &handle->blocks[cur_block].qlock );
            }

            // just throw away this buffer
            handle->blocks[cur_block].qdepth--;
            handle->blocks[cur_block].head = ( handle->blocks[cur_block].head + 1 ) % MAX_QDEPTH;

            // make sure to signal the thread if we just made room for it to resume work
            if ( handle->blocks[cur_block].qdepth == MAX_QDEPTH - 1 )
               pthread_cond_signal( &handle->blocks[cur_block].thread_resume );
            pthread_mutex_unlock( &handle->blocks[cur_block].qlock );
         }
         else if ( !(nbytes) ) { // early breakout if we are done.  No point looping over all queues
            break;
         }
      }

// ---------------------- END OF MAIN READ LOOP ----------------------

      // set all values to align with the next stripe
      offset = 0;
      cur_stripe++;
      if ( read_full_stripe )
         handle->buff_offset += stripesz;
   }

   return bytes_read;
}



/**
 * Writes nbytes from buffer into the erasure striping specified by the provided handle
 * @param ne_handle handle : Handle for the erasure striping to be written to
 * @param void* buffer : Buffer containing the data to be written
 * @param int nbytes : Number of data bytes to be written from buffer
 * @return int : Number of bytes written or -1 on error
 */
ssize_t ne_write( ne_handle handle, const void *buffer, size_t nbytes )
{
 
   int N;                       /* number of raid parts not including E */ 
   int E;                       /* num erasure stripes */
   unsigned int bsz;                     /* chunksize in k */ 
   int counter;                 /* general counter */
   int ecounter;                /* general counter */
   ssize_t ret_out;             /* Number of bytes returned by read() and write() */
   unsigned long long totsize;  /* used to sum total size of the input file/stream */
   int mtot;                    /* N + numerasure stripes */
   u32 readsize;
   u32 writesize;
   u32 crc;                     /* crc 32 */
   TimingData* timing = handle->timing_data_ptr;

   if (nbytes > UINT_MAX) {
     PRINTerr( "ne_write: not yet validated for write-sizes above %lu\n", UINT_MAX);
     errno = EFBIG;             /* sort of */
     return -1;
   }

   if ( !(handle) ) {
     PRINTerr( "ne_write: received a NULL handle!\n" );
     errno = EINVAL;
     return -1;
   }

   if ( handle->mode != NE_WRONLY  &&  handle->mode != NE_REBUILD ) {
     PRINTerr( "ne_write: handle is in improper mode for writing! %d\n", handle->mode );
     errno = EINVAL;
     return -1;
   }

   N = handle->erasure_state->N;
   E = handle->erasure_state->E;
   bsz = handle->erasure_state->bsz;

   mtot=N+E;


   /* loop until the file input or stream input ends */
   totsize = 0;
   while (1) { 

      counter = handle->buff_rem / bsz;
      /* loop over the parts and write the parts, sum and count bytes per part etc. */
      // NOTE: regarding benchmark timers, this routine just hands off work asynchronously to
      // bq_writer threads.  We let each individual thread maintain its own stats.
      while (counter < N) {

         writesize = ( handle->buff_rem % bsz ); // ? amount being written to block (block size - already written).
         readsize = bsz - writesize; // amount being read for block[block_index] from source buffer

         //avoid reading beyond end of buffer
         if ( totsize + readsize > nbytes ) { readsize = nbytes-totsize; }

         if ( readsize < 1 ) {
            PRINTdbg("ne_write: reading of input is now complete\n");
            break;
         }

         // I think we can understand this as follows: the "read offset" is an
         // offset in the generated erasure data, not including the user's data,
         // and the "write offset" is the logical position in the total output,
         // not including the 4-bytes-per-block of CRC data.
         PRINTdbg( "ne_write: reading input for %lu bytes with offset of %llu "
                   "and writing to offset of %lu in handle buffer\n",
                   (unsigned long)readsize, totsize, handle->buff_rem );
         //memcpy ( handle->buffer + handle->buff_rem, buffer+totsize, readsize);
         int queue_result = bq_enqueue(&handle->blocks[counter], buffer+totsize, readsize);
         if(queue_result == -1) {
           // bq_enqueue will set errno.
           return -1;
         }
         
         PRINTdbg( "ne_write:   ...copy complete.\n");

         totsize += readsize;
         writesize = readsize + ( handle->buff_rem % bsz );
         handle->buff_rem += readsize;

         if ( writesize < bsz ) {  //if there is not enough data to write a full block, stash it in the handle buffer
            PRINTdbg("ne_write: reading of input is complete, stashed %lu bytes in handle buffer\n", (unsigned long)readsize);
            break;
         }

         counter++;
      } //end of writes for N

      // If we haven't written a whole stripe, terminate. This happens
      // if there is not enough data to form a complete stripe.
      if ( counter != N ) {
         break;
      }


      /* calculate and write erasure */
      if (timing->flags & TF_ERASURE)
         fast_timer_start(&timing->erasure);

      if ( handle->e_ready == 0 ) {
         PRINTdbg( "ne_write: initializing erasure matricies...\n");
         // Generate an encoding matrix
         // NOTE: The matrix generated by gf_gen_rs_matrix is not always invertable for N>=6 and E>=5!
         gf_gen_rs_matrix(handle->encode_matrix, mtot, N);
         // Generate g_tbls from encode matrix
         ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);

         handle->e_ready = 1;
      }

      // Perform matrix dot_prod for EC encoding
      // using g_tbls from encode matrix encode_matrix
      // Need to lock the two buffers here.
      int i;
      int buffer_index;
      for(i = N; i < handle->erasure_state->N + handle->erasure_state->E; i++) {
        BufferQueue *bq = &handle->blocks[i];
        if(pthread_mutex_lock(&bq->qlock) != 0) {
          PRINTerr("Failed to acquire lock for erasure blocks\n");
          return -1;
        }
        while(bq->qdepth == MAX_QDEPTH) {
          PRINTdbg("waiting on erasure thread %d\n", i);
          pthread_cond_wait(&bq->master_resume, &bq->qlock);
        }
        if(i == N) {
          buffer_index = bq->tail;
        }
        else {
          assert(buffer_index == bq->tail);
        }
      }
      PRINTdbg( "ne_write: calculating %d erasure from %d data blocks at queue position %d\n", E, N, buffer_index );

      // this makes a good place to increment our global 'part-size'
      handle->erasure_state->nsz += bsz;

      ec_encode_data(bsz, N, E, handle->g_tbls,
                     (unsigned char **)handle->block_buffs[buffer_index],
                     (unsigned char **)&(handle->block_buffs[buffer_index][N]));

      if (timing->flags & TF_ERASURE) {
         fast_timer_stop(&timing->erasure);
         log_histo_add_interval(&timing->erasure_h,
                                &timing->erasure);
      }

      for(i = N; i < handle->erasure_state->N + handle->erasure_state->E; i++) {
         BufferQueue *bq = &handle->blocks[i];
         bq->tail = (bq->tail + 1) % MAX_QDEPTH;
         bq->qdepth++;
         pthread_cond_signal(&bq->thread_resume);
         pthread_mutex_unlock(&bq->qlock);
      }

      //now that we have written out all data, reset buffer
      handle->buff_rem = 0; 
   }
   handle->erasure_state->totsz += totsize; //as it is impossible to write at an offset, the sum of writes will be the total size

   int nerr = 0; //used for reporting excessive errors at the end of the function
   for ( counter = 0; counter < N + E; counter++) {
      if ( handle->erasure_state->meta_status[counter] || handle->erasure_state->data_status[counter] )
           nerr++;
   }

   // If the errors exceed the minimum protection threshold number of
   // errrors then fail the write.
   if( UNSAFE(handle,nerr) ) {
     PRINTerr("ne_write: errors exceed minimum protection level (%d)\n",
              MIN_PROTECTION);
     errno = EIO;
     return -1;
   }
   else {
     return totsize;
   }
}



/**
 * Closes the erasure striping indicated by the provided handle and flushes
 * the handle buffer, if necessary.
 *
 * @param ne_handle handle : Handle for the striping to be closed
 *
 * @return int : Status code.  Success is indicated by 0, and failure by -1.
 *               A positive value indicates that the operation was
 *               successful, but that errors were encountered in the
 *               stripe.  The Least-Significant Bit of the return code
 *               corresponds to the first of the N data stripe files, while
 *               each subsequent bit corresponds to the next N files and
 *               then the E files.  A 1 in these positions indicates that
 *               an error was encountered while acessing that specific
 *               file.  Note, this code does not account for the offset of
 *               the stripe.  The code will be relative to the file names
 *               only.  (i.e. an error in "<output_path>1<output_path>"
 *               would be encoded in the second bit of the output, a
 *               decimal value of 2)
 */
int ne_close( ne_handle handle ) 
{
   int            counter;
   int            ret = 0;
   
   PRINTdbg( "entering ne_close()\n" );

   if ( handle == NULL ) {
      PRINTerr( "ne_close: received a NULL handle\n" );
      errno = EINVAL;
      return -1;
   }

   int N = handle->erasure_state->N;
   int E = handle->erasure_state->E;
   u32 bsz = handle->erasure_state->bsz;

   TimingData* timing = handle->timing_data_ptr; /* shorthand */


   /* flush the handle buffer if necessary */
   if ( handle->mode == NE_WRONLY  &&  handle->buff_rem != 0 ) {
      // Note: this should be skipped for rebuilds
      int tmp;
      unsigned char* zero_buff;
      PRINTdbg( "ne_close: flushing handle buffer...\n" );
      //zero the buffer to the end of the stripe
      tmp = (N*bsz) - handle->buff_rem;
      zero_buff = malloc(sizeof(char) * tmp);
      bzero(zero_buff, tmp );

      if ( tmp != ne_write( handle, zero_buff, tmp ) ) { //make ne_write do all the work
         PRINTerr( "ne_close: failed to flush handle buffer\n" );
         ret = -1;
      }

      // make sure to decrement totsz, so the zero-fill is not included in the total data size
      handle->erasure_state->totsz -= tmp;
      free( zero_buff );
   }


   // set the umask here to catch all meta files and reset before returning
   mode_t mask = umask(0000);
   /* Close file descriptors and free bufs and set xattrs for written files */
   counter = 0;
   for(counter = 0; counter < N+E; counter++) {
      bq_close(&handle->blocks[counter]);
   }

   int nerr = 0;
   int i;
   /* wait for the threads */
   for(i = 0; i < N+E; i++) {
      pthread_join(handle->threads[i], NULL);
      bq_destroy(&handle->blocks[i]);
      if ( handle->erasure_state->meta_status[i] || handle->erasure_state->data_status[i] )
         nerr++; //use this opportunity to count how many errors we have
   }

   // all potential meta-file manipulation should be done now (threads have exited)
   // should be safe to reset umask
   umask(mask);

   if ( handle->mode != NE_STAT ) { // ne_stat() won't have allocated buffs
      /* free the buffers */
      for(i = 0; i < MAX_QDEPTH; i++) {
         free(handle->buffer_list[i]);
      }
   }

   if( UNSAFE(handle,nerr)  &&  ( handle->mode == NE_WRONLY  ||  handle->mode == NE_REBUILD ) ) {
      PRINTdbg( "ne_close: detected unsafe error levels following write operation\n" );
      errno = EIO;
      ret = -1;
   }
   // NOTE: I think we only really care about returning a -1 for writes.  Otherwise, ne_read() should handle
   // setting errors in the case of excess errors.  ne_close() should just report what errrors there were
   //else if ( ( handle->mode != NE_STAT )  &&  ( nerr > handle->erasure_state->E ) ) { /* for non-writes */
   //   PRINTdbg( "ne_close: detected excessive errors following a read operation\n" );
   //   errno = ENODATA;
   //   ret = -1;
   //}
   if ( ret == 0 ) {
      PRINTdbg( "ne_close: encoding error pattern in return value...\n" );
      /* Encode any file errors into the return status */
      for( counter = 0; counter < N+E; counter++ ) {
         if ( handle->erasure_state->data_status[counter] | handle->erasure_state->meta_status[counter] ) {
            if ( handle->mode != NE_STAT )
               ret += ( 1 << ((counter + handle->erasure_state->O) % (N+E)) );
            else  // for NE_STAT, data/meta status structs are always arranged as if erasure_state->O is zero
               ret += ( 1 << counter );
         }
      }
   }

   if ( handle->path_fmt != NULL )
      free(handle->path_fmt);

   free(handle->encode_matrix);
   free(handle->decode_matrix);
   free(handle->invert_matrix);
   free(handle->g_tbls);
   
   if (timing->flags & TF_HANDLE)
      fast_timer_stop(&timing->handle_timer); /* overall cost of this op */

   // if we are only serving libne (marfs/pftool would've provided and
   // alternative TimingData, so timing data could survive ne_close()), and
   // we have been requested to collect timing data (e.g. on the libneTest
   // command-line), then dump it to stdout now.
   if (timing->flags
       && (handle->timing_data_ptr == &handle->timing_data)) {
      show_handle_stats(handle);
   }

   // only free erasure_state if we didn't receive NE_ESTATE at open time
   if ( handle->alloc_state )
      free( handle->erasure_state );
   free(handle);
   return ret;
}


/**
 * Determines whether the parent directory of the given file exists
 * @param char* path : Character string to be searched
 * @param int max_length : Maximum length of the character string to be scanned
 * @return int : 0 if the parent directory does exist and -1 if not
 */
int parent_dir_missing(uDALType itype, SktAuth auth, char* path, int max_length ) {
   char*       tmp   = path;
   int         len   = 0;
   int         index = -1;
   const uDAL* impl  = get_impl(itype);

   struct stat status;
   int         res;

   while ( (len < max_length) &&  (*tmp != '\0') ) {
      if( *tmp == '/' )
         index = len;
      len++;
      tmp++;
   }
   
   tmp = path;
   *(tmp + index) = '\0';
   res = PATHOP(stat, impl, auth, tmp, &status );
   PRINTdbg( "parent_dir_missing: stat of \"%s\" returned %d\n", path, res );
   *(tmp + index) = '/';

   return res;
}


/**
 * Deletes the erasure striping of the specified width with the specified path format
 *
 * ne_delete(path, width)  calls this with fn=ne_default_snprintf, and printf_state=NULL
 *
 * @param char* path : Name structure for the files of the desired striping.  This should contain a single "%d" field.
 * @param int width : Total width of the erasure striping (i.e. N+E)
 * @return int : 0 on success and -1 on failure
 */
int ne_delete1( SnprintfFunc snprintf_fn, void* state,
                uDALType itype, SktAuth auth,
                TimingFlagsValue timing_flags, TimingData* timing_data,
                char* path, int width ) {

   char  file[MAXNAME];       /* array name of files */
   char  partial[MAXNAME];
   int   counter;
   int   ret = 0;
   int   parent_missing;

   const uDAL* impl = get_impl(itype);

   // flags control collection of timing stats
   FastTimer  timer;            // we don't have an ne_handle
   if (timing_flags & TF_HANDLE) {
      fast_timer_inits();
      fast_timer_reset(&timer); /* prepare timer for use */
      fast_timer_start(&timer); /* start overall timer */
   }

   for( counter=0; counter<width; counter++ ) {
      parent_missing = -2;
      bzero( file, sizeof(file) );

      snprintf_fn( file,    MAXNAME, path, counter, snprintf_fn );

      snprintf_fn( partial, MAXNAME, path, counter, snprintf_fn );
      strncat( partial, WRITE_SFX, MAXNAME - strlen(partial) );

      // unlink the file or the unfinished file.  If both fail, check
      // whether the parent directory exists.  If not, indicate an error.
      if ( ne_delete_block1(impl, auth, file)
           &&  PATHOP(unlink, impl, auth, partial )
           &&  (parent_missing = parent_dir_missing(itype, auth, file, MAXNAME)) ) {

         ret = -1;
      }
   }

   if (timing_flags & TF_HANDLE) {
      fast_timer_stop(&timer);
      if (timing_data)
         log_histo_add_interval(&timing_data->misc_h, &timer);
      else
         fast_timer_show(&timer, (timing_flags & TF_SIMPLE),  "delete: ", 0);
   }

   return ret;
}

int ne_delete(char* path, int width ) {

   // This is safe for builds with/without sockets and/or socket-authentication enabled.
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_delete1(ne_default_snprintf, NULL, UDAL_POSIX, auth, 0, NULL, path, width);
}



int ne_rebuild1_vl( SnprintfFunc fn, void* state,
                    uDALType itype, SktAuth auth,
                    TimingFlagsValue timing_flags, TimingData* timing_data,
                    char *path, ne_mode mode, va_list ap ) {

// ---------------------- OPEN THE ORIGINAL STRIPE FOR READ ----------------------

   PRINTdbg( "opening handle for read\n" );

   // open the stripe for RDALL to verify all data/erasure blocks and to regenerate as we read
   // this function will parse args and zero out our e_state struct
   ne_mode read_mode = NE_RDALL | ( mode & ( NE_NOINFO | NE_SETBSZ | NE_ESTATE ) );
   e_state erasure_state_read = parse_va_open_args( ap, &read_mode );
   if ( erasure_state_read == NULL )
      return -1; // check for failure condition, parse_va_open_args will set errno

   // this will handle allocating a handle and setting values
   ne_handle read_handle = create_handle( fn, state, itype, auth,
                                          timing_flags, timing_data,
                                          path, read_mode, erasure_state_read );
   if ( read_handle == NULL ) {
      if ( !(read_mode & NE_ESTATE) )
         free( erasure_state_read );
      return -1;
   }

   // we know our mode arg is good, no need to check

   // prep the handle for reading
   if( initialize_handle(read_handle) ) {
      free( read_handle->path_fmt );
      free( read_handle );
      if ( !(read_mode & NE_ESTATE) )
         free( erasure_state_read );
      return -1;
   }


   // as we didn't parse our own arguments, we now need to pull their values out of the read handle structs.
   // Even if opened with NE_NOINFO, these should be populated by the time ne_open() returns.
   int N = erasure_state_read->N;
   int E = erasure_state_read->E;
   int O = erasure_state_read->O;
   u32 bsz = erasure_state_read->bsz;
   u64 totsz = erasure_state_read->totsz; // this will be used for determining when to stop, rather than for populating our write structs

   // now that handle initialization is complete, we can explicitly indicate to the read handle that this is a rebuild operation, 
   //  requiring the regeneration of all erasure data
   read_handle->mode = NE_REBUILD;

// ---------------------- OPEN THE SAME STRIPE FOR OUTPUT ----------------------

   PRINTdbg( "opening handle for write\n" );

   e_state erasure_state_write = malloc( sizeof( struct ne_state_struct ) );
   if ( erasure_state_write == NULL ) {
      errno = ENOMEM;
      ne_close( read_handle );
      return -1;
   }
   // zeroing out this struct is mandatory, as we won't be calling parse_va_open_args() with it
   // who knows what malloc gave us
   memset( erasure_state_write, 0, sizeof( struct ne_state_struct ) );

   // populate the write handle with values set in the read handle
   erasure_state_write->N = N;
   erasure_state_write->E = E;
   erasure_state_write->O = O;
   erasure_state_write->bsz = bsz;

   // this will handle allocating a handle and setting values (we can pass in NE_REBUILD here to use the proper suffix for writes)
   ne_handle write_handle = create_handle( fn, state, itype, auth,
                                           timing_flags, timing_data,
                                           path, NE_REBUILD, erasure_state_write );
   if ( write_handle == NULL ) {
      free( erasure_state_write );
      ne_close( read_handle );
      return -1;
   }

   if( initialize_handle(write_handle) ) {
      free( write_handle->path_fmt );
      free( write_handle );
      free( erasure_state_write );
      ne_close( read_handle );
      return -1;
   }

   // determine owner uid and gid by stating the first intact data file
   int i;
   for ( i = 0; i < (N + E); i++ ) {
      BufferQueue* bq = &read_handle->blocks[i];
      if ( !(erasure_state_read->data_status[i]) ) {
         struct stat st_info;

         if (read_handle->timing_data_ptr->flags & TF_STAT)
            fast_timer_start(&read_handle->timing_data_ptr->stats[i].stat);

         int ret = PATHOP(stat, read_handle->impl, read_handle->auth, bq->path, &st_info);

         if (read_handle->timing_data_ptr->flags & TF_STAT)
            fast_timer_stop(&read_handle->timing_data_ptr->stats[i].stat);

         if ( ret == 0 ) {
            write_handle->owner = st_info.st_uid;
            write_handle->group = st_info.st_gid;
            break;
         }
         else {
            PRINTerr( "failure of stat for file \"%s\"\n", bq->path );
         }
      }
   }
   // check for a failure to get uid/gid, but don't bother to stop the rebuild
   if ( i == (N + E) )
      PRINTerr( "failed to determine proper uid/gid for rebuilt parts\n" );

// ---------------------- BEGIN MAIN REBUILD LOOP ----------------------

   // let's create a struct to indicate which blocks we are rebuilding
   char being_rebuilt[ N + E ];
   // keep track of how many times we have re-read the original file
   char iterations = 0;
   // use this as a flag for final completion
   char all_rebuilt = 0;

   // Now, we need to loop over all data, until the entire file has been successfully read and re-written as necessary
   while ( !(all_rebuilt) ) {

      char err_out = 0; // use this flag to indicate a hard failure condition

      PRINTdbg( "performing iteration %d of the rebuild process\n", iterations + 1 );
      // We should only have to read the original a maximum of two times.  Once to detect all data errors, and 
      // a second time to output all regenerated blocks.  Any more than that is suspicious and should be reported.
      if ( iterations > 1 ) {
         err_out = 1;
         break;
      }

// ---------------------- DEACTIVATE UNNEEDED OUTPUT THREADS ----------------------

      // At this point, we are either just beginning the rebuild process or are restarting it after hitting an additional 
      // error.  Either way, we need to reach into the write handle and explicitly error-out all blocks that aren't being 
      // rebuilt.  This will prevent any data from being written to them until needed.  Setting their error state like 
      // this should also allow us to bypass having the blocks considered 'bad' at close time of write_handle.
      signal_threads( BQ_HALT, write_handle, N+E, 0 ); // halt all write threads
      // explicitly clear fields set during the writing process
      write_handle->erasure_state->nsz = 0;
      write_handle->erasure_state->totsz = 0;
      for ( i = 0; i < (N + E); i++ ) {
         BufferQueue* bq = &write_handle->blocks[i];
         if ( erasure_state_read->meta_status[i] || erasure_state_read->data_status[i] ) {
            // if the read handle has this block listed as being in error, we need to actually write out a new copy
            PRINTdbg( "block %d is needed for output, and will be resumed\n", i );
            bq_resume( 0, bq, 0 );
            being_rebuilt[i] = 1;
         }
         else {
            PRINTdbg( "block %d is unneeded, and will be skipped\n", i );
            bq_resume( -1, bq, 0 ); // otherwise, use a special offset value of -1 to tell the thread to error itself out
            being_rebuilt[i] = 0;
         }
         // Note: For write threads, adjusting this offset is misleading, as it refers to a queue buffer offset rather than 
         // one of a file.  However, as resume_threads() should be clearing out the write queue before removing the HALT flag,
         // this offset needs to be reset to zero regardless.  As we know this should always be zero, we can use a negative 
         // value to overload the meaning.
      }

// ---------------------- REPAIR DAMAGED BLOCKS ----------------------

      off_t bytes_repaired = 0; // keep track of how much of the file has successfully been repaired
      // Note: it is ESSENTIAL, that the ne_read calls performed during rebuilds perfectly align to the stripe width.  Any larger, and 
      //  buffers may be removed from queues before we can use them for reconstructing damaged files.
      size_t buff_size = bsz * N;

      // this loop will actually perform the data movement
      while ( bytes_repaired < totsz ) {
         // read the bytes from the original file
         PRINTdbg( "reading %zd bytes from original file\n", buff_size );
         off_t bytes_read = ne_read( read_handle, NULL, buff_size, bytes_repaired );
         // check for an under-read
         if ( bytes_read < (off_t)buff_size ) { // the off_t cast is needed to ensure bytes_read is not autocast to an unsigned value
            if( bytes_read < 0 ) {
               PRINTerr( "ne_rebuild: failed to read %zd bytes at offset %zd from the original stripe\n", buff_size, bytes_repaired );
               err_out = 1;
               break;
            }
            // an under-read is expected at the end of the file.  Anywhere else is a problem.
            if ( (bytes_repaired + bytes_read) < totsz ) {
               PRINTerr( "ne_rebuild: read at offset %zd returned fewer bytes than expected (%zd instead of %zd)\n", bytes_repaired, bytes_read, (totsz - bytes_repaired) );
               err_out = 1;
               break;
            }
            PRINTdbg( "received %zd bytes instead of a full stripe (%zd bytes), indicating end of file\n", bytes_read, buff_size );
         }
         // sanity check, to ensure we are reading at stripe alignments
         if ( read_handle->buff_offset != bytes_repaired ) {
            PRINTerr( "ne_rebuild: expected a read handle offset of %zd following a stripe rebuild, but instead found %zd\n", bytes_repaired, read_handle->buff_offset );
            err_out = 1;
            break;
         }
         // then write reconstructed buffers out to the repairing blocks
         for ( i = 0; i < (N + E); i++ ) {
            if ( being_rebuilt[i] ) {
               PRINTdbg( "writing %zd bytes to rebulding block %d\n", bsz, i );
               BufferQueue* bq_read =  &read_handle->blocks[i];
               BufferQueue* bq_write = &write_handle->blocks[i];
               bq_enqueue( bq_write, bq_read->buffers[ bq_read->head ], bsz );
            }
         }
         // increment write handle values
         write_handle->erasure_state->nsz += bsz;
         write_handle->erasure_state->totsz += bytes_read;
         bytes_repaired += bytes_read;
      }
      iterations++; // note that we have read the original again

// ---------------------- CHECK FAILURE/SUCCESS CONDITIONS ----------------------

      // Any read/write errors should cause us to fail.
      if ( err_out )
         break;

      // loop through all blocks in the read handle, checking for new errors
      all_rebuilt = 1;
      for ( i = 0; i < (N + E); i++ ) {
         if ( ( erasure_state_read->meta_status[i] || erasure_state_read->data_status[i] )  &&  !(being_rebuilt[i]) ) {
            all_rebuilt = 0; // any error which we have not yet rebuilt will require a re-run
            PRINTdbg( "new error in block %d will require a re-read\n", i );
         }
      }

// ---------------------- END MAIN LOOP AND CLEANUP ----------------------

   }

   PRINTdbg( "exited rebuild loop after %d iterations\n", iterations );

   ne_close( read_handle );
   if ( !(all_rebuilt) ) { // error conditions should have left this at zero
      PRINTdbg( "aborting all write threads due to rebuild error\n" );
      // we need to make sure that our 'repaired' blocks are never used
      signal_threads( BQ_ABORT, write_handle, N+E, 0 );
      ne_close( write_handle ); // don't care about the return here
      return -1;
   }

   // finalize our rebuilt blocks
   // this close should skip writing out any zero-fill.  We have already handled those blocks above.
   return ne_close(write_handle); // the error pattern returned by ne_close() should now indicate the state of the stripe
}


int ne_rebuild1( SnprintfFunc fn, void* state,
                 uDALType itype, SktAuth auth,
                 TimingFlagsValue timing_flags, TimingData* timing_data,
                 char* path, ne_mode mode, ... ) {

   int ret; 
   va_list vl;
   va_start(vl, mode);
   ret = ne_rebuild1_vl(fn, state, itype, auth, timing_flags, timing_data, path, mode, vl); 
   va_end(vl);
   return ret; 
}


int ne_rebuild( char *path, ne_mode mode, ... ) {
   int ret;

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   va_list   vl;
   va_start(vl, mode);
   ret = ne_rebuild1_vl(ne_default_snprintf, NULL, UDAL_POSIX, auth, 0, NULL, path, mode, vl);
   va_end(vl);

   return ret;
}



#ifndef HAVE_LIBISAL
// This replicates the function defined in libisal.  If we define it here,
// and do static linking with libisal, the linker will complain.

void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
        int i, j;

        for (i = 0; i < rows; i++) {
                for (j = 0; j < k; j++) {
                        gf_vect_mul_init(*a++, g_tbls);
                        g_tbls += 32;
                }
        }
}
#endif




/**
 * Performs a rebuild operation on the erasure striping indicated by the given handle, but ignores faulty xattr values.
 * @param ne_handle handle : The handle for the erasure striping to be repaired
 * @return int : Status code.  Success is indicated by 0 and failure by -1
 */
//int ne_noxattr_rebuild(ne_handle handle) {
//   while ( handle->erasure_state->nerr > 0 ) {
//      handle->erasure_state->nerr--;
//      handle->erasure_state->src_in_err[handle->src_err_list[handle->erasure_state->nerr]] = 0;
//      handle->src_err_list[handle->erasure_state->nerr] = 0;
//   }
//   return ne_rebuild( handle ); 
//}


int ne_stat1( SnprintfFunc fn, void* state,
                    uDALType itype, SktAuth auth,
                    TimingFlagsValue timing_flags, TimingData* timing_data,
                    char *path, e_state e_state_struct ) {
   // zero out the suspicious e_state_struct
   memset( e_state_struct, 0, sizeof( struct ne_state_struct ) );
   ne_handle handle = create_handle( fn, state, itype, auth,
                                     timing_flags, timing_data,
                                     path, NE_STAT | NE_ESTATE, e_state_struct );
   if ( handle == NULL ) {
      errno = EINVAL;
      return -1;
   }

   int i;
   int num_blocks = MIN_MD_CONSENSUS;
   char error = 0; // for indicating an error condition

   struct read_meta_buffer_struct read_meta_state; //needed to determine metadata consensus of read threadss
   int threads_checked = 0; // count of how many threads have already been verified

   // We need to dynamically determine the N, E, O, and bsz values.
   // This requires spinning up threads, one at a time, and checking their meta info for consistency.
   // Thus, we initialize num_blocks to a 'consensus' value and then reset to something more reasonable once 
   // we have a good idea of N/E.

   /* open files and initialize BufferQueues */
   for(i = 0; i < num_blocks; i++) {
      BufferQueue *bq = &handle->blocks[i];
      // handle all offset adjustments before calling bq_init()
      bq->block_number_abs = i;
      // generate the path
      handle->snprintf(bq->path, MAXNAME, handle->path_fmt, i, handle->printf_state);

      if ( (i == 1)  &&  (strncmp(bq->path, handle->blocks[0].path, MAXNAME) == 0) ) {
         // sanity check, blocks should not have matching paths
         // I only bother to check for the first and second blocks, just to avoid simple mistakes (no %d in path).
         // More complex sprintf functions may still have collisions later on, but that is the responsibility of 
         // the caller, who gave us those functions.
         PRINTerr( "detected a name collision between blocks 0 and 1\n" );
         terminate_threads( BQ_ABORT, handle, i, 0 );
         errno = EINVAL;
         return -1;
      }

      PRINTdbg( "starting up thread for block %d\n", i );
    
      if(bq_init(bq, i, handle) < 0) {
         PRINTerr("bq_init failed for block %d\n", i);
         error = 1; // to trigger cleanup below
         break;
      }
      // note that read threads will be initialized in a halted state

      if ( pthread_create(&handle->threads[i], NULL, bq_reader, (void *)bq) ) {
         PRINTerr("failed to start thread %d\n", i);
         error = 1; // to trigger cleanup below
         break;
      }

      if ( handle->erasure_state->N == 0 ) {
         PRINTdbg("Checking for error opening block %d\n", i);

         if ( pthread_mutex_lock( &bq->qlock ) ) {
            PRINTerr( "failed to aquire queue lock for thread %d\n", i );
            error = 1; // to trigger cleanup below
            break;
         }

         // wait for the queue to be ready.
         while( !( bq->state_flags & BQ_OPEN ) ) // wait for the thread to open its file
            pthread_cond_wait( &bq->master_resume, &bq->qlock );
         pthread_mutex_unlock( &bq->qlock ); // just waiting for open to complete, don't need to hold this longer

         threads_checked++;

         // if we have all the threads needed for determining N/E
         if ( (i+1) >= MIN_MD_CONSENSUS ) {

            // find the most common values from amongst all meta information
            int matches = check_matches( handle->blocks, i+1, &(read_meta_state) );

            PRINTdbg( "attempting to determine N/E values after starting %d threads resulted in %d matches\n", i+1, matches );

            // check if our values seem sensible enough to believe.
            // This check can be thought of as two distinct possiblities--
            //    Stripe width appears to be greater than MIN_MD_CONSENSUS, it must also be true that:
            //       - we have at least MIN_MD_CONSENSUS matching values
            //    Stripe width appears to be less than or equal to MIN_MD_CONSENSUS, it must also be true that:
            //       - we have checked exactly MIN_MD_CONSENSUS threads, with more, it doesn't make sense to see this value
            //       - the values are at least potentially valid (N > 0  and  E >= 0)
            //       - we have at least N matches, providing at least some assurance
            if ( 
                  (
                    (read_meta_state.N + read_meta_state.E) > MIN_MD_CONSENSUS  &&
                    matches >= MIN_MD_CONSENSUS //we want at least MIN_MD_CONSENSUS matches before using values
                  )  
                     ||  
                  ( 
                    ((read_meta_state.N + read_meta_state.E) <= MIN_MD_CONSENSUS)  &&  
                    (i < MIN_MD_CONSENSUS)  &&  //only applies when we have checked exactly MIN_MD_CONSENSUS threads (already know (i+1) >= MIN)
                    (read_meta_state.N > 0  &&  read_meta_state.E >= 0)  &&  //ensure these values are sensible
                    (matches >= read_meta_state.N)  //ensure we have at least N matches before believing a low value
                  ) 
               ) {
               PRINTdbg( "setting N/E to %d/%d after locating %d matching values\n", read_meta_state.N, read_meta_state.E, matches );
               handle->erasure_state->N = read_meta_state.N;
               handle->erasure_state->E = read_meta_state.E;
               num_blocks = read_meta_state.N + read_meta_state.E;
            }
            else {
               // if we are still within our bounds, just keep extending the stripe, trying to find *something*
               if ( num_blocks < MAXPARTS ) {
                  num_blocks++;
               }
               else {
                  // otherwise, ENOENT is probably the most applicable failure condition
                  error = 1; // to trigger cleanup below
                  errno = ENOENT;
               }
            }
         } // END OF : if ( (i+1) >= MIN_MD_CONSENSUS )
      } // END OF : if ( handle->erasure_state->N == 0 )
   } // END OF : for(i = 0; i < num_blocks; i++)

   // in some rare cases (N=1 and E=0 for example), we may have started too many threads
   // terminate those now
   terminate_threads( BQ_FINISHED, handle, i - (handle->erasure_state->N + handle->erasure_state->E), handle->erasure_state->N + handle->erasure_state->E );

   // check for errors on open...
   // We finish checking thread status down here in order to give them a bit more time to spin up.
   for(i = threads_checked; i < num_blocks; i++) {

      BufferQueue *bq = &handle->blocks[i];

      PRINTdbg("Checking for error opening block %d\n", i);

      if ( pthread_mutex_lock( &bq->qlock ) ) {
         PRINTerr( "failed to aquire queue lock for thread %d\n", i );
         error = 1; // to trigger cleanup below
         break;
      }

      // wait for the queue to be ready.
      while( !( bq->state_flags & BQ_OPEN ) ) // wait for the thread to open its file
         pthread_cond_wait( &bq->master_resume, &bq->qlock );
      pthread_mutex_unlock( &bq->qlock ); // just waiting for open to complete, don't need to hold this longer

   }


   // find the most common values from amongst all meta information, now that all threads have started
   // we have to do this here, in case we don't have a bsz value
   int matches = check_matches( handle->blocks, num_blocks, &(read_meta_state) );
   if ( read_meta_state.N != handle->erasure_state->N  ||  read_meta_state.E != handle->erasure_state->E ) {
      PRINTerr( "detected a mismatch between consensus N/E values (%d/%d) and those most common to the stripe (%d/%d).\n",
                  handle->erasure_state->N, handle->erasure_state->E, read_meta_state.N, read_meta_state.E );
      errno = EBADFD;
      error = 1;
   }
   handle->erasure_state->O = read_meta_state.O;
   if ( read_meta_state.O < 0  ||  read_meta_state.O >= (read_meta_state.N + read_meta_state.E) ) {
      PRINTerr( "consensus O value (%d) is invalid or incosistent with N/E values (%d/%d).\n", read_meta_state.O, read_meta_state.N, read_meta_state.E );
      errno = EBADFD;
      error = 1;
   }
   handle->erasure_state->bsz = read_meta_state.bsz;
   if ( read_meta_state.bsz <= 0 ) {
      PRINTerr( "consensus bsz value (%lu) is invalid.\n", read_meta_state.bsz );
      errno = EBADFD;
      error = 1;
   }
   handle->erasure_state->nsz = read_meta_state.nsz;
   handle->erasure_state->totsz = read_meta_state.totsz;
   // Note: ncompsz and crcsum are set by the thread itself
   for ( i = 0; i < num_blocks; i++ ) {
      // take this opportunity to mark all mismatched meta values as incorrect
      read_meta_buffer read_buf = (read_meta_buffer)handle->blocks[i].buffers[0];
      if ( read_buf->N != handle->erasure_state->N  ||  read_buf->E != handle->erasure_state->E  ||  read_buf->O != handle->erasure_state->O  ||  
           read_buf->bsz != handle->erasure_state->bsz  ||  read_buf->nsz != handle->erasure_state->nsz  ||  
           read_buf->totsz != handle->erasure_state->totsz )
         handle->erasure_state->meta_status[i] = 1;
      // free our read_meta_buff structs
      free( read_buf ); 
   }

   matches = ne_close( handle ); // let ne_close cleanup, and set error pattern for ret code
   if ( error )
      return -1;
   return matches;

}



int ne_stat( char* path, e_state erasure_state_struct ) {
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_stat1( ne_default_snprintf, NULL, UDAL_POSIX, auth, 0, NULL, path, erasure_state_struct );
}



// ---------------------------------------------------------------------------
// per-block functions
//
// These functions operate on a single block-file.  They are called both
// from (a) administrative applications which run on the server-side, and
// (b) from client-side applications (like the MarFS run-time).  With the
// advent of the uDAL, the second case requires wrapping in
// uDAL-implementation-sensitive code.  See comments above
// ne_delete_block() for details.
// ---------------------------------------------------------------------------




int ne_set_xattr1(const uDAL* impl, SktAuth auth,
                  const char *path, const char *metaval, size_t len) {
   int ret = -1;

   // see comments above OPEN() defn
   GenericFD      fd   = {0};
   struct handle  hndl = {0};
   ne_handle      handle = &hndl;

   handle->impl = impl;
   handle->auth = auth;
   if (! handle->impl) {
      PRINTerr( "ne_set_xattr1: implementation is NULL\n");
      errno = EINVAL;
      return -1;
   }


#ifdef META_FILES
   char meta_file[2048];
   strcpy( meta_file, path );
   strncat( meta_file, META_SFX, strlen(META_SFX) + 1 );

   // cannot set umask here as this is called within threads
   OPEN( fd, handle->auth, handle->impl, meta_file, O_WRONLY | O_CREAT, 0666 );

   if (FD_ERR(fd)) {
      PRINTerr( "ne_close: failed to open file %s\n", meta_file);
      ret = -1;
   }
   else {
      // int val = HNDLOP( write, fd, metaval, strlen(metaval) + 1 );
      int val = write_all(&fd, metaval, len );
      if ( val != len ) {
         PRINTerr( "ne_close: failed to write to file %s\n", meta_file);
         ret = -1;
         HNDLOP(close, fd);
      }
      else {
         ret = HNDLOP(close, fd);
      }
   }

   // PATHOP(chown, handle->impl, handle->auth, meta_file, handle->owner, handle->group);

#else
   // looks like the stuff below might conceivably work with threads.
   // The problem is that fgetxattr/fsetxattr are not yet implemented.
#   error "xattr metadata is not functional with new thread model"

   OPEN( fd, handle->auth, handle->impl, path, O_RDONLY );
   if (FD_ERR(fd)) { 
      PRINTerr("ne_set_xattr: failed to open file %s\n", path);
      ret = -1;
   }
   else {

#   if (AXATTR_SET_FUNC == 5) // XXX: not functional with threads!!!
      ret = HNDLOP(fsetxattr, fd, XATTRKEY, metaval, len, 0);
#   else
      ret = HNDLOP(fsetxattr, fd, XATTRKEY, metaval, len, 0, 0);
#   endif
   }

   if (HNDLOP(close, fd) < 0) {
      PRINTerr("ne_set_xattr: failed to close file %s\n", path);
      ret = -1;
   }
#endif //META_FILES

   return ret;
}

int ne_set_xattr( const char *path, const char *metaval, size_t len) {

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_set_xattr1(get_impl(UDAL_POSIX), auth, path, metaval, len);
}



int ne_get_xattr1( const uDAL* impl, SktAuth auth,
                   const char *path, char *metaval, size_t len) {
   int ret = 0;

   // see comments above OPEN() defn
   GenericFD      fd   = {0};
   struct handle  hndl = {0};
   ne_handle      handle = &hndl;

   handle->impl = impl;
   handle->auth = auth;
   if (! handle->impl) {
      PRINTerr( "ne_get_xattr1: implementation is NULL\n" );
      errno = EINVAL;
      return -1;
   }


#ifdef META_FILES
   char meta_file_path[2048];
   strncpy(meta_file_path, path, 2048);
    strncat(meta_file_path, META_SFX, strlen(META_SFX)+1);

   OPEN( fd, handle->auth, handle->impl, meta_file_path, O_RDONLY );
   if (FD_ERR(fd)) {
      ret = -1;
      PRINTerr("ne_get_xattr: failed to open file %s\n", meta_file_path);
   }
   else {
      // ssize_t size = HNDLOP( read, fd, metaval, len );
      ssize_t size = read_all(&fd, metaval, len);
      if ( size < 0 ) {
         PRINTerr("ne_get_xattr: failed to read from file %s\n", meta_file_path);
         ret = -1;
      }
      else if(size == 0) {
         PRINTerr( "ne_get_xattr: read 0 bytes from metadata file %s\n", meta_file_path);
         ret = -1;
      }
      else if (size == len) {
         // This might mean that the read truncated results to fit into our buffer.
         // Caller should give us a buffer that has more-than-enough room.
         PRINTerr( "ne_get_xattr: read %d bytes from metadata file %s\n", size, meta_file_path);
         ret = -1;
      }

      if (HNDLOP(close, fd) < 0) {
         PRINTerr("ne_get_xattr: failed to close file %s\n", meta_file_path);
         ret = -1;
      }
      if (! ret)
         ret = size;
   }

#else
   // looks like the stuff below might conceivably work with threads.
#   error "xattr metadata is not functional with new thread model"

   OPEN( fd, handle->auth, handle->impl, path, O_RDONLY );
   if (FD_ERR(fd)) { 
      PRINTerr("ne_get_xattr: failed to open file %s\n", path);
      ret = -1;
   }
   else {

#   if (AXATTR_GET_FUNC == 4)
      ret = HNDLOP(fgetxattr, fd, XATTRKEY, &metaval[0], len);
#   else
      ret = HNDLOP(fgetxattr, fd, XATTRKEY, &metaval[0], len, 0, 0);
#   endif
   }

   if (HNDLOP(close, fd) < 0) {
      PRINTerr("ne_get_xattr: failed to close file %s\n", meta_file_path);
      ret = -1;
   }
#endif

   // make sure that our xattr values are null terminated strings
   if ( ret >= 0 )
      *(metaval + ret) = '\0';

   return ret;
}

int ne_get_xattr( const char *path, char *metaval, size_t len) {

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_get_xattr1(get_impl(UDAL_POSIX), auth, path, metaval, len);
}

static int set_block_xattr(ne_handle handle, int block) {
  int tmp = 0;
  TimingData* timing = handle->timing_data_ptr;

  char metaval[1024];
  int xvalen = snprintf(metaval,1024,"%d %d %d %d %lu %lu %llu %llu\n",
                        handle->erasure_state->N, handle->erasure_state->E, handle->erasure_state->O,
                        handle->erasure_state->bsz, handle->erasure_state->nsz,
                        handle->erasure_state->ncompsz[block], (unsigned long long)handle->erasure_state->csum[block],
                        (unsigned long long)handle->erasure_state->totsz);

  PRINTdbg( "ne_close: setting file %d xattr = \"%s\"\n",
            block, metaval );

  char block_file_path[2048];
  handle->snprintf(block_file_path, MAXNAME, handle->path_fmt,
                   (block+handle->erasure_state->O)%(handle->erasure_state->N + handle->erasure_state->E),
                   handle->printf_state);

   if ( handle->mode == NE_REBUILD )
      strncat( block_file_path, REBUILD_SFX, strlen(REBUILD_SFX)+1 );
   else if ( handle->mode == NE_WRONLY )
      strncat( block_file_path, WRITE_SFX, strlen(WRITE_SFX)+1 );
   

   if (timing->flags & TF_XATTR)
      fast_timer_start(&timing->stats[block].xattr);

   int rc = ne_set_xattr1(handle->impl, handle->auth, block_file_path, metaval, xvalen);

   if (timing->flags & TF_XATTR)
      fast_timer_stop(&timing->stats[block].xattr);

   return rc;
}


// unlink a single block (including the manifest file, if
// META_FILES is defined).  This is called from:
//
// (a) a commented-out function in the mc_ring.c MarFS utility ('ch'
//     branch), where I think it would represent a fully-specified
//     block-file on the server-side.  From the server-side, UDAL_POSIX
//     will always work with such paths.
//
// (b) ne_delete1(), where it refers to a fully-specified block-file, but
//     from the client-side.  Therefore, it may potentially need to go
//     through a MarFS RDMA server, so it must acquire uDAL dressing, to
//     allow selection of the appropriate uDAL implementation.


int ne_delete_block1(const uDAL* impl, SktAuth auth, const char *path) {

   int ret = PATHOP(unlink, impl, auth, path);

#ifdef META_FILES
   if(ret == 0) {
      char meta_path[2048];
      strncpy(meta_path, path, 2048);
      strcat(meta_path, META_SFX);

      ret = PATHOP(unlink, impl, auth, meta_path);
   }
#endif

   return ret;
}

int ne_delete_block(const char *path) {

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_delete_block1(get_impl(UDAL_POSIX), auth, path);
}




/**
 * Make a symlink to an existing block.
 */
int ne_link_block1(const uDAL* impl, SktAuth auth,
                   const char *link_path, const char *target) {

   struct stat target_stat;
   int         ret;


#ifdef META_FILES
   char meta_path[2048];
   char meta_path_target[2048];

   strcpy(meta_path, link_path);
   strcat(meta_path, META_SFX);

   strcpy(meta_path_target, target);
   strcat(meta_path_target, META_SFX);
#endif // META_FILES

   // stat the target.
   if (PATHOP(stat, impl, auth, target, &target_stat) == -1) {
      return -1;
   }

   // if it is a symlink, then move it,
   if(S_ISLNK(target_stat.st_mode)) {
      // check that the meta file has a symlink here too, If not then
      // abort without doing anything. If it does, then proceed with
      // making symlinks.
      if(PATHOP(stat, impl, auth, meta_path_target, &target_stat) == -1) {
         return -1;
      }
      if(!S_ISLNK(target_stat.st_mode)) {
         return -1;
      }
      char   tp[2048];
      char   tp_meta[2048];
      size_t link_size;
      if((link_size = PATHOP(readlink, impl, auth, target, tp, 2048)) != -1) {
         tp[link_size] = '\0';
      }
      else {
         return -1;
      }
#ifdef META_FILES
      if((link_size = PATHOP(readlink, impl, auth, meta_path_target, tp_meta, 2048)) != -1) {
         tp_meta[link_size] = '\0';
      }
      else {
         return -1;
      }
#endif

      // make the new links.
      ret = PATHOP(symlink, impl, auth, tp, link_path);
#ifdef META_FILES
      if(ret == 0)
         ret = PATHOP(symlink, impl, auth, tp_meta, meta_path);
#endif

      // remove the old links.
      ret = PATHOP(unlink, impl, auth, target);
#ifdef META_FILES
      if(ret == 0)
         PATHOP(unlink, impl, auth, meta_path_target);
#endif
      return ret;
   }

   // if not, then create the link.
   ret = PATHOP(symlink, impl, auth, target, link_path);
#ifdef META_FILES
   if(ret == 0)
      ret = PATHOP(symlink, impl, auth, meta_path_target, meta_path);
#endif
   return ret;
}


int ne_link_block(const char *link_path, const char *target) {

   // this is safe for builds with/without sockets enabled
   // and with/without socket-authentication enabled
   // However, if you do build with socket-authentication, this will require a read
   // from a file (~/.awsAuth) that should probably only be accessible if ~ is /root.
   SktAuth  auth;
   if (DEFAULT_AUTH_INIT(auth)) {
      PRINTerr("failed to initialize default socket-authentication credentials\n");
      return -1;
   }

   return ne_link_block1(get_impl(UDAL_POSIX), auth, link_path, target);
}



/* The following function was copied from Intel's ISA-L (https://github.com/intel/isa-l/blob/master/examples/ec/ec_simple_example.c).
   The associated Copyright info has been reproduced below */


/**********************************************************************
  Copyright(c) 2011-2018 Intel Corporation All rights reserved.
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
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
**********************************************************************/

/*
 * Generate decode matrix from encode matrix and erasure list
 *
 */

static int gf_gen_decode_matrix_simple(unsigned char * encode_matrix,
                                       unsigned char * decode_matrix,
                                       unsigned char * invert_matrix,
                                       unsigned char * temp_matrix,
                                       unsigned char * decode_index, unsigned char * frag_err_list, int nerrs, int k,
                                       int m)
{
        int i, j, p, r;
        int nsrcerrs = 0;
        unsigned char s, *b = temp_matrix;
        unsigned char frag_in_err[MAXPARTS];

        memset(frag_in_err, 0, sizeof(frag_in_err));

        // Order the fragments in erasure for easier sorting
        for (i = 0; i < nerrs; i++) {
                if (frag_err_list[i] < k)
                        nsrcerrs++;
                frag_in_err[frag_err_list[i]] = 1;
        }

        // Construct b (matrix that encoded remaining frags) by removing erased rows
        for (i = 0, r = 0; i < k; i++, r++) {
                while (frag_in_err[r])
                        r++;
                for (j = 0; j < k; j++)
                        b[k * i + j] = encode_matrix[k * r + j];
                decode_index[i] = r;
        }

        // Invert matrix to get recovery matrix
        if (gf_invert_matrix(b, invert_matrix, k) < 0)
                return -1;

        // Get decode matrix with only wanted recovery rows
        for (i = 0; i < nerrs; i++) {
                if (frag_err_list[i] < k)       // A src err
                        for (j = 0; j < k; j++)
                                decode_matrix[k * i + j] =
                                    invert_matrix[k * frag_err_list[i] + j];
        }

        // For non-src (parity) erasures need to multiply encode matrix * invert
        for (p = 0; p < nerrs; p++) {
                if (frag_err_list[p] >= k) {    // A parity err
                        for (i = 0; i < k; i++) {
                                s = 0;
                                for (j = 0; j < k; j++)
                                        s ^= gf_mul(invert_matrix[j * k + i],
                                                    encode_matrix[k * frag_err_list[p] + j]);
                                decode_matrix[k * p + i] = s;
                        }
                }
        }
        return 0;
}


