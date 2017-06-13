#ifndef __MARFS_FAST_TIMER_H__
#define __MARFS_FAST_TIMER_H__

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
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/



// NOTE: The TSC ticks at the "nominal" processor frequency (i.e. the
// default freq, not counting changes in the actual processor freq
// resulting from idling or turbo, etc.)  As wikipedia puts it: "TSC ticks
// are counting the passage of time, not the number of CPU clock cycles",
// which is perfect for us.


#include <string.h>             // memset()



// indices for TimerValue.v32[] elements
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define LO  0
#  define HI  1
#else
#  define LO  1
#  define HI  0
#endif


typedef union {
   uint64_t v64;
   uint32_t v32[2];             /* see HI/LO */
} TimerValue;

typedef struct {
   int            chip;         /* CPU chip on which timer was started */
   int            core;
   TimerValue     start;
   uint32_t       migrations;   /* n chip/core migrations during timing */
   uint32_t       MHz;          /* nominal CPU freq (see NOTE above) */
   uint64_t       accum;
} FastTimer;


// debugging
int show_cpuid();


// call this once, before any threads try to call fast_timer_sec().
// Returns 0 for success, negative for failure.
int fast_timer_inits();



__attribute__((always_inline)) int fast_timer_reset(FastTimer* ft);

__attribute__((always_inline)) int fast_timer_start(FastTimer* ft);

__attribute__((always_inline)) int fast_timer_stop(FastTimer* ft);


// convert the TSC ticks in ft->accum to elapsed time
double fast_timer_sec(FastTimer* ft);
double fast_timer_nsec(FastTimer* ft);


// print timer stats
int fast_timer_show(FastTimer* ft, const char* str);
int fast_timer_show_details(FastTimer* ft, const char* str);


// You must do this before using a given timer.  Do it again, anytime the
// timer is not running, to reset the accumulator, and the migrations-count
__attribute__((always_inline)) int fast_timer_reset(FastTimer* ft);

__attribute__((always_inline)) int fast_timer_start(FastTimer* ft);

__attribute__((always_inline)) int fast_timer_stop(FastTimer* ft);



#endif
