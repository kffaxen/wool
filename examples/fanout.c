/*
   This file is part of Wool, a library for fine-grained independent
   task parallelism

   Copyright 2009- Karl-Filip Faxén, kff@sics.se
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
       * Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in the
         documentation and/or other materials provided with the distribution.
       * Neither "Wool" nor the names of its contributors may be used to endorse
         or promote products derived from this software without specific prior
         written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR OTHER CONTRIBUTORS BE LIABLE FOR ANY
   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   This is Wool version @WOOL_VERSION@
*/

#include <stdlib.h>
#include <stdio.h>
#include "wool.h"

int loop(int);

TASK_3( int, fanout, int, leaf_size, int, width, int, depth )
{
  if( depth > 0 ) {
    int n,i;
    for( i = 0; i < width-1; i++ ) {
      SPAWN( fanout, leaf_size, width, depth-1 );
    }
    n = CALL( fanout, leaf_size, width, depth-1 );
    for( i = 0; i < width-1; i++ ) {
      n += SYNC( fanout );
    }
    return n;
  } else {
    return loop( leaf_size ) != 0 ? 1 : 0;
  }
}

int main( int argc, char **argv )
{
  int i, d, w, n, m, s = 0;

  argc = wool_init( argc, argv );

  if( argc < 5 ) {
    fprintf( stderr, "Usage: fanout [<wool opts>] <grain> <width> <depth> <reps>\n" );
    return 1;
  }

  n  = atoi( argv[1] );
  w  = atoi( argv[2] );
  d  = atoi( argv[3] );
  m  = atoi( argv[4] );

  for( i=0; i<m; i++) {
    s += CALL( fanout, n, w, d );
  }

  printf( "Number of leaves = %d\n", s );

  wool_fini();

  return 0;
}
