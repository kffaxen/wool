/**********************************************************************************************/
/*  This program is part of the Barcelona OpenMP Tasks Suite                                  */
/*  Copyright (C) 2009 Barcelona Supercomputing Center - Centro Nacional de Supercomputacion  */
/*  Copyright (C) 2009 Universitat Politecnica de Catalunya                                   */
/*                                                                                            */
/*  This program is free software; you can redistribute it and/or modify                      */
/*  it under the terms of the GNU General Public License as published by                      */
/*  the Free Software Foundation; either version 2 of the License, or                         */
/*  (at your option) any later version.                                                       */
/*                                                                                            */
/*  This program is distributed in the hope that it will be useful,                           */
/*  but WITHOUT ANY WARRANTY; without even the implied warranty of                            */
/*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                             */
/*  GNU General Public License for more details.                                              */
/*                                                                                            */
/*  You should have received a copy of the GNU General Public License                         */
/*  along with this program; if not, write to the Free Software                               */
/*  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA            */
/**********************************************************************************************/

/*
 *  Original code from the Cilk project
 *
 * Copyright (c) 2000 Massachusetts Institute of Technology
 * Copyright (c) 2000 Matteo Frigo
 *
 * Adapted to Wool by Karl-Filip Faxen
 *
 */

/*---:::[[[]]]:::---
  Changed to Cilk++ to fit multi-model BOTS
  Artur Podobas
  Royal Institute of Technology
  2010
  ---:::[[[]]]:::---*/

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <alloca.h>
//#include "bots.h"
//#Include <Omp.h>
//#include <cilk.h>
#include "wool.h"

#if 0

/* Checking information */

static int solutions[] = {
        1,
        0,
        0,
        2,
        10, /* 5 */
        4,
        40,
        92,
        352,
        724, /* 10 */
        2680,
        14200,
        73712,
        365596,
};
#define MAX_SOLUTIONS sizeof(solutions)/sizeof(int)

#endif

int mycount=0;
//#pragma omp threadprivate(mycount)

int total_count;


/*
 * <a> contains array of <n> queen positions.  Returns 1
 * if none of the queens conflict, and returns 0 otherwise.
 */
int ok(int n, char *a)
{
     int i, j;
     char p, q;

     for (i = 0; i < n; i++) {
	  p = a[i];

	  for (j = i + 1; j < n; j++) {
	       q = a[j];
	       if (q == p || q == p - (j - i) || q == p + (j - i))
		    return 0;
	  }
     }
     return 1;
}

void nqueens_ser (int n, int j, char *a, int *solutions)
{
	int i,res;

	if (n == j) {
		/* good solution, count it */

		*solutions = 1;
		return;
	}

	*solutions = 0;



     	/* try each possible position for queen <j> */
	for (i = 0; i < n; i++) {
		{
	  		/* allocate a temporary array and copy <a> into it */
	  		a[j] = i;
	  		if (ok(j + 1, a)) {
	       			nqueens_ser(n, j + 1, a,&res);

				*solutions += res;

			}
		}
	}
}



//Pre-declare it
//void nqueens_cilk( int n , int j , char *a, int *csols , int depth , int i);

VOID_TASK_FORW_6 (nqueens_wool, int, int, char*, int*, int, int);


#if defined(MANUAL_CUTOFF)


VOID_TASK_5 (nqueens , int , n , int , j , char* , a , int* , solutions , int , depth)
{
	int i;
	int *csols;
#ifdef USE_SYNC_ALL
        Task *mark = GET_MARK();
#endif

	if (n == j) {
		/* good solution, count it */

		*solutions = 1;
		return;
	}

	*solutions = 0;
	csols = (int * ) alloca(n*sizeof(int));
	memset(csols,0,n*sizeof(int));

     	/* try each possible position for queen <j> */
	for (i = 0; i < n; i++) {
		if ( depth < bots_cutoff_value ) {
                  SPAWN (nqueens_wool , n , j , a , csols , depth , i);
		} else {
  			a[j] = i;
  			if (ok(j + 1, a))
       				nqueens_ser(n, j + 1, a,&csols[i]);
		}
	}

#ifdef USE_SYNC_ALL
        if (depth < bots_cutoff_value) SYNC_ALL(nqueens_wool, mark);
#else
              if (depth < bots_cutoff_value) for (i=0;i<n;i++) SYNC(nqueens_wool);
#endif
              for ( i = 0; i < n; i++) *solutions += csols[i];
}


#else

//void nqueens(int n, int j, char *a, int *solutions, int depth)
  VOID_TASK_5 (nqueens , int , n , int , j , char* , a , int* , solutions , int , depth)
{
	int i;
	int *csols;
#ifdef USE_SYNC_ALL
        Task *mark = GET_MARK();
#endif

	if (n == j) {
		/* good solution, count it */

		*solutions = 1;
		return;
	}

	*solutions = 0;
	csols = (int*) alloca(n*sizeof(int));
	memset(csols,0,n*sizeof(int));

     	/* try each possible position for queen <j> */
	for (i = 0; i < n; i++) {
	  SPAWN(nqueens_wool, n , j ,a , csols , depth , i);
	}

#ifdef USE_SYNC_ALL
        SYNC_ALL(nqueens_wool, mark);
#else
        for (i=0;i<n;i++) SYNC(nqueens_wool);
#endif
	for ( i = 0; i < n; i++) *solutions += csols[i];

}

#endif

//To be used with CILK/WOOL
//void nqueens_cilk( int n , int j , char *a, int *csols , int depth , int i)
VOID_TASK_6 (nqueens_wool, int , n , int , j , char* , a , int* , csols , int , depth , int , i)
{
  char *b = (char *) alloca((j + 1) * sizeof(char));

	  			memcpy(b, a, j * sizeof(char));
	  			b[j] = i;
	  			if (ok(j + 1, b))
				  CALL( nqueens,n, j + 1, b,&csols[i],depth+1);
}


TASK_1( int, find_queens, int, size)
{
        char *a = (char *)  malloc(size * sizeof(char));

	total_count=0;
	CALL(nqueens,size, 0, a, &total_count,0);

        return total_count;
}

TASK_2( int, main, int, argc, char **, argv )
{
   int n,m;

   if( argc < 2 ) {
      fprintf( stderr, "Usage: nqueens <woolopt>... <arg>\n" ),
      exit( 2 );
   }

   n = atoi( argv[ 1 ] );

   m = CALL( find_queens, n );

   printf( "%d\n", m );

   return 0;
}
