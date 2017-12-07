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

#include "wool.h"
#include <stdio.h>
#include <stdlib.h>

int loop( int );

VOID_TASK_2( tree, int, d, int, n )
{
  if( d>0 ) {
    SPAWN( tree, d-1, n );
    CALL( tree, d-1, n);
    SYNC( tree );
  } else {
    loop( n );
  }
}

TASK_2( int, main, int, argc, char **, argv )
{
  int i, d, n, m;

  if( argc < 4 ) {
    fprintf( stderr, "Usage: stress [<wool opts>] <grain> <depth> <reps>\n" );
    return 1;
  }

  n  = atoi( argv[1] );
  d  = atoi( argv[2] );
  m  = atoi( argv[3] );

  for( i=0; i<m; i++) {
    CALL( tree, d, n );
  }
  printf( "DONE\n" );

  return 0;
}
