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
  int n = 100, r = 1000, ch = 1;
  int* delays;
  char* model;

  if( argc < 4 ) {
    fprintf( stderr, "Usage: dynamic-loop <model> <reps> <trip> <chunksize> <params...>\n" );
    exit(1);
  }

  model = argv[1];
  r = atoi( argv[2] );
  n = atoi( argv[3] );
  ch = atoi( argv[4] );

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
