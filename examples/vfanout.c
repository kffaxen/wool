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
#include <malloc.h>
#include "wool.h"

int loop(int);

VOID_TASK_2( fanout, int, leaf_size, long int *, res )
{
  *res = loop( leaf_size ) != 0 ? 1 : 0;
}

static int work( int leaf_size, int width )
{
  long int *arr = (long int *) malloc( sizeof(long int) * width );
  int i;
  long int sum = 0;
  
  for( i = 0; i < width; i++ ) {
    SPAWN( fanout, leaf_size, arr+i );
    loop(1000);
  }
  
  for( i = 0; i < width; i++ ) {
    SYNC( fanout );
  }
  
  for( i = 0; i < width; i++ ) {
    sum += arr[i];
  }
  
  return sum;
}

int main( int argc, char **argv )
{
  int i, w, n, m, s = 0;

  argc = wool_init( argc, argv );

  if( argc < 4 ) {
    fprintf( stderr, "Usage: fanout [<wool opts>] <grain> <width> <reps>\n" );
    return 1;
  }

  n  = atoi( argv[1] );
  w  = atoi( argv[2] );
  m  = atoi( argv[3] );

  for( i=0; i<m; i++) {
    s += work( n, w );
  }

  printf( "Number of leaves = %d\n", s );

  wool_fini();

  return 0;
}
