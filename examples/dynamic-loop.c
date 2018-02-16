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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wool.h>

int loop( int );

void flute( int argc, char** argv, int n, int* delays )
{
  int coeff, power;

  if( argc < 1 ) {
    fprintf( stderr, "Params for flute: <coeff> [<power>]\n" );
    exit(1);
  } else if( argc < 2 ) {
    power = 1;
  } else {
    power = atoi( argv[1] );
  }

  coeff = atoi( argv[0] );

  for( int i = 0; i < n; i++ ) {
    delays[i] = coeff * i^power;
  }
}

LOOP_BODY_1( tloop, LARGE_BODY, int, i, int*, delays )
{
  loop( delays[i] );
}

TASK_2( int, main, int, argc, char **, argv )
{
  int n = 100, r = 1000;
  int* delays;
  char* model;

  if( argc < 4 ) {
    fprintf( stderr, "Usage: dynamic-loop <model> <reps> <trip> <params...>\n" );
    exit(1);
  }

  model = argv[1];
  r = atoi( argv[2] );
  n = atoi( argv[3] );

  delays = (int*) malloc( n * sizeof(int) );

  if( !strcmp( model, "flute" ) ) {
    flute( argc-5, argv+5, n, delays );
  } else {
    fprintf( stderr, "Unknown model: %s\n", model );
  }

  for( int j = 0; j < r; j++ ) {
    FOR( tloop, 0, n, delays );
  }

  return 0;
}
