/*
 * Sparse Cholesky code with little blocks at the leaves of the Quad tree
 * Keith Randall -- Aske Plaat
 * Adapted to C/Wool by Karl-Filip Faxen
 *
 * This code should run with any square sparse real symmetric matrix
 * from MatrixMarket (http://math.nist.gov/MatrixMarket)
 *
 * run with `cholesky -f george-liu.mtx' for a given matrix, or
 * `cholesky -n 1000 -z 10000' for a 1000x1000 random matrix with 10000
 * nonzeros (caution: random matrices produce lots of fill).
 */

/*
 * Copyright (c) 2000 Massachusetts Institute of Technology
 * Copyright (c) 2000 Matteo Frigo
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#include "wool.h"
#include <math.h>
#include <string.h>
#include <stdio.h>
#include "getoptions.h"
#include <assert.h>
#include <stdlib.h>

// #include "config.h"
#if HAVE_MALLOC_H
#include <malloc.h>
#endif

/*************************************************************\
 * Basic types
\*************************************************************/

typedef double Real;

#define BLOCK_DEPTH 2		/* logarithm base 2 of BLOCK_SIZE */
#define BLOCK_SIZE  (1<<BLOCK_DEPTH)	/* 4 seems to be the optimum */

typedef Real Block[BLOCK_SIZE][BLOCK_SIZE];

#define BLOCK(B,I,J) (B[I][J])

#define _00 0
#define _01 1
#define _10 2
#define _11 3

#define TR_00 _00
#define TR_01 _10
#define TR_10 _01
#define TR_11 _11

typedef struct InternalNode {
     struct InternalNode *child[4];
} InternalNode;

typedef struct {
     Block block;
} LeafNode;

typedef InternalNode *Matrix;

/*************************************************************\
 * Linear algebra on blocks
\*************************************************************/

#if 0
/*
 * elem_daxmy - Compute y' = y - ax where a is a Real and x and y are
 * vectors of Reals.
 */
static void elem_daxmy(Real a, Real * x, Real * y, int n)
{
     for (n--; n >= 0; n--)
	  y[n] -= a * x[n];
}
#endif

/*
 * block_schur - Compute Schur complement B' = B - AC.
 */
static void block_schur_full(Block B, Block A, Block C)
{
     int i, j, k;
     for (i = 0; i < BLOCK_SIZE; i++) {
	  for (j = 0; j < BLOCK_SIZE; j++) {
	       for (k = 0; k < BLOCK_SIZE; k++) {
		    BLOCK(B, i, j) -= BLOCK(A, i, k) * BLOCK(C, j, k);
	       }
	  }
     }
}

/*
 * block_schur - Compute Schur complement B' = B - AC.
 */
static void block_schur_half(Block B, Block A, Block C)
{
     int i, j, k;

     /*
      * printf("schur half\n");
      */
     /* Compute Schur complement. */
     for (i = 0; i < BLOCK_SIZE; i++) {
	  for (j = 0; j <= i /* BLOCK_SIZE */ ; j++) {
	       for (k = 0; k < BLOCK_SIZE; k++) {
		    BLOCK(B, i, j) -= BLOCK(A, i, k) * BLOCK(C, j, k);
	       }
	  }
     }
}

/*
 * block_upper_solve - Perform substitution to solve for B' in
 * B'U = B.
 */
static void block_backsub(Block B, Block U)
{
     int i, j, k;

     /* Perform backward substitution. */
     for (i = 0; i < BLOCK_SIZE; i++) {
	  for (j = 0; j < BLOCK_SIZE; j++) {
	       for (k = 0; k < i; k++) {
		    BLOCK(B, j, i) -= BLOCK(U, i, k) * BLOCK(B, j, k);	/* transpose? */
	       }
	       BLOCK(B, j, i) /= BLOCK(U, i, i);
	  }
     }
}

#if 0
/*
 * block_lower_solve - Perform forward substitution to solve for B' in
 * LB' = B.
 */
static void xblock_backsub(Block B, Block L)
{
     int i, k;

     /* Perform forward substitution. */
     for (i = 0; i < BLOCK_SIZE; i++)
	  for (k = 0; k <= i; k++) {
	       BLOCK(B, i, k) /= BLOCK(L, k, k);
	       elem_daxmy(BLOCK(L, i, k), &BLOCK(B, k, 0),
			  &BLOCK(B, i, 0), BLOCK_SIZE - k);
	  }
}
#endif

/*
 * block_cholesky - Factor block B.
 */
static void block_cholesky(Block B)
{
     int i, j, k;

     for (k = 0; k < BLOCK_SIZE; k++) {
	  Real x;
	  if (BLOCK(B, k, k) < 0.0) {
	       printf("sqrt error: %f\n", BLOCK(B, k, k));
	       printf("matrix is probably not numerically stable\n");
	       exit(9);
	  }
	  x = sqrt(BLOCK(B, k, k));
	  for (i = k; i < BLOCK_SIZE; i++) {
	       BLOCK(B, i, k) /= x;
	  }
	  for (j = k + 1; j < BLOCK_SIZE; j++) {
	       for (i = j; i < BLOCK_SIZE; i++) {
		    BLOCK(B, i, j) -= BLOCK(B, i, k) * BLOCK(B, j, k);
		    if (j > i && BLOCK(B, i, j) != 0.0) {
			 printf("Upper not empty\n");
		    }
	       }
	  }
     }
}

/*
 * block_zero - zero block B.
 */
static void block_zero(Block B)
{
     int i, k;

     for (i = 0; i < BLOCK_SIZE; i++) {
	  for (k = 0; k < BLOCK_SIZE; k++) {
	       BLOCK(B, i, k) = 0.0;
	  }
     }
}

/*************************************************************\
 * Allocation and initialization
\*************************************************************/

/*
 * Create new leaf nodes (BLOCK_SIZE x BLOCK_SIZE submatrices)
 */
inline InternalNode *new_block_leaf(void)
{
     LeafNode *leaf = (LeafNode *) malloc(sizeof(LeafNode));
     if (leaf == NULL) {
	  printf("out of memory!\n");
	  exit(1);
     }
     return (InternalNode *) leaf;
}

/*
 * Create internal node in quadtree representation
 */
inline InternalNode *new_internal(InternalNode * a00, InternalNode * a01,
				  InternalNode * a10, InternalNode * a11)
{
     InternalNode *node = (InternalNode *) malloc(sizeof(InternalNode));
     if (node == NULL) {
	  printf("out of memory!\n");
	  exit(1);
     }
     node->child[_00] = a00;
     node->child[_01] = a01;
     node->child[_10] = a10;
     node->child[_11] = a11;
     return node;
}

/*
 * Duplicate matrix.  Resulting matrix may be laid out in memory
 * better than source matrix.
 */
TASK_2( Matrix, copy_matrix, int, depth, Matrix, a)
{
     Matrix r;

     if (!a)
	  return a;

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  LeafNode *R;
	  r = new_block_leaf();
	  R = (LeafNode *) r;
	  memcpy(R->block, A->block, sizeof(Block));
     } else {
	  Matrix r00, r01, r10, r11;

	  depth--;

	  SPAWN( copy_matrix, depth, a->child[_00] );
	  SPAWN( copy_matrix, depth, a->child[_01] );
	  SPAWN( copy_matrix, depth, a->child[_10] );
	  SPAWN( copy_matrix, depth, a->child[_11] );

	  r11 = SYNC( copy_matrix );
	  r10 = SYNC( copy_matrix );
	  r01 = SYNC( copy_matrix );
	  r00 = SYNC( copy_matrix );

	  r = new_internal(r00, r01, r10, r11);
     }
     return r;
}

/*
 * Deallocate matrix.
 */
void free_matrix(int depth, Matrix a)
{
     if (a == NULL)
	  return;
     if (depth == BLOCK_DEPTH) {
	  free(a);
     } else {
	  depth--;
	  free_matrix(depth, a->child[_00]);
	  free_matrix(depth, a->child[_01]);
	  free_matrix(depth, a->child[_10]);
	  free_matrix(depth, a->child[_11]);
	  free(a);
     }
}

/*************************************************************\
 * Simple matrix operations
\*************************************************************/

/*
 * Get matrix element at row r, column c.
 */
Real get_matrix(int depth, Matrix a, int r, int c)
{
     assert(depth >= BLOCK_DEPTH);
     assert(r < (1 << depth));
     assert(c < (1 << depth));

     if (a == NULL)
	  return 0.0;

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  return BLOCK(A->block, r, c);
     } else {
	  int mid;

	  depth--;
	  mid = 1 << depth;

	  if (r < mid) {
	       if (c < mid)
		    return get_matrix(depth, a->child[_00], r, c);
	       else
		    return get_matrix(depth, a->child[_01], r, c - mid);
	  } else {
	       if (c < mid)
		    return get_matrix(depth, a->child[_10], r - mid, c);
	       else
		    return get_matrix(depth, a->child[_11], r - mid, c - mid);
	  }
     }
}

/*
 * Set matrix element at row r, column c to value.
 */
Matrix set_matrix(int depth, Matrix a, int r, int c, Real value)
{
     assert(depth >= BLOCK_DEPTH);
     assert(r < (1 << depth));
     assert(c < (1 << depth));

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A;
	  if (a == NULL) {
	       a = new_block_leaf();
	       A = (LeafNode *) a;
	       block_zero(A->block);
	  } else {
	       A = (LeafNode *) a;
	  }
	  BLOCK(A->block, r, c) = value;
     } else {
	  int mid;

	  if (a == NULL)
	       a = new_internal(NULL, NULL, NULL, NULL);

	  depth--;
	  mid = 1 << depth;

	  if (r < mid) {
	       if (c < mid)
		    a->child[_00] = set_matrix(depth, a->child[_00],
					       r, c, value);
	       else
		    a->child[_01] = set_matrix(depth, a->child[_01],
					       r, c - mid, value);
	  } else {
	       if (c < mid)
		    a->child[_10] = set_matrix(depth, a->child[_10],
					       r - mid, c, value);
	       else
		    a->child[_11] = set_matrix(depth, a->child[_11],
					       r - mid, c - mid, value);
	  }
     }
     return a;
}

void print_matrix_aux(int depth, Matrix a, int r, int c)
{
     if (a == NULL)
	  return;

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  int i, j;
	  for (i = 0; i < BLOCK_SIZE; i++)
	       for (j = 0; j < BLOCK_SIZE; j++)
		    printf("%6d %6d: %12f\n", r + i, c + j, BLOCK(A->block, i, j));
     } else {
	  int mid;
	  depth--;
	  mid = 1 << depth;
	  print_matrix_aux(depth, a->child[_00], r, c);
	  print_matrix_aux(depth, a->child[_01], r, c + mid);
	  print_matrix_aux(depth, a->child[_10], r + mid, c);
	  print_matrix_aux(depth, a->child[_11], r + mid, c + mid);
     }
}

/*
 * Print matrix
 */
void print_matrix(int depth, Matrix a)
{
     print_matrix_aux(depth, a, 0, 0);
}

/*
 * Count number of blocks (leaves) in matrix representation
 */
int num_blocks(int depth, Matrix a)
{
     int res;
     if (a == NULL)
	  return 0;
     if (depth == BLOCK_DEPTH)
	  return 1;

     depth--;
     res = 0;
     res += num_blocks(depth, a->child[_00]);
     res += num_blocks(depth, a->child[_01]);
     res += num_blocks(depth, a->child[_10]);
     res += num_blocks(depth, a->child[_11]);
     return res;
}

/*
 * Count number of nonzeros in matrix
 */
int num_nonzeros(int depth, Matrix a)
{
     int res;
     if (a == NULL)
	  return 0;
     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  int i, j;
	  res = 0;
	  for (i = 0; i < BLOCK_SIZE; i++) {
	       for (j = 0; j < BLOCK_SIZE; j++) {
		    if (BLOCK(A->block, i, j) != 0.0)
			 res++;
	       }
	  }
	  return res;
     }
     depth--;
     res = 0;
     res += num_nonzeros(depth, a->child[_00]);
     res += num_nonzeros(depth, a->child[_01]);
     res += num_nonzeros(depth, a->child[_10]);
     res += num_nonzeros(depth, a->child[_11]);
     return res;
}

/*
 * Compute sum of squares of elements of matrix
 */
Real mag(int depth, Matrix a)
{
     Real res = 0.0;
     if (!a)
	  return res;

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  int i, j;
	  for (i = 0; i < BLOCK_SIZE; i++)
	       for (j = 0; j < BLOCK_SIZE; j++)
		    res += BLOCK(A->block, i, j) * BLOCK(A->block, i, j);
     } else {
	  depth--;
	  res += mag(depth, a->child[_00]);
	  res += mag(depth, a->child[_01]);
	  res += mag(depth, a->child[_10]);
	  res += mag(depth, a->child[_11]);
     }
     return res;
}

/*************************************************************\
 * Cholesky algorithm
\*************************************************************/

/*
 * Perform R -= A * Transpose(B)
 * if lower==1, update only lower-triangular part of R
 */

// The last argument determines whether it is ok if one of a and b are NULL

TASK_5( Matrix, mul_and_subT, int, depth, int, lower, Matrix, a, Matrix, b, Matrix, r )
{
     assert(a != NULL && b != NULL);

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  LeafNode *B = (LeafNode *) b;
	  LeafNode *R;

	  if (r == NULL) {
	       r = new_block_leaf();
	       R = (LeafNode *) r;
	       block_zero(R->block);
	  } else
	       R = (LeafNode *) r;

	  if (lower)
	       block_schur_half(R->block, A->block, B->block);
	  else
	       block_schur_full(R->block, A->block, B->block);
     } else {
	  Matrix r00, r01, r10, r11;

	  depth--;

	  if (r != NULL) {
	       r00 = r->child[_00];
	       r01 = r->child[_01];
	       r10 = r->child[_10];
	       r11 = r->child[_11];
	  } else {
	       r00 = NULL;
	       r01 = NULL;
	       r10 = NULL;
	       r11 = NULL;
	  }

          if (a->child[_00] && b->child[TR_00])
 	      SPAWN( mul_and_subT, depth, lower,
                               a->child[_00], b->child[TR_00],
			       r00 );

	  if (!lower && a->child[_00] && b->child[TR_01]) {
	      SPAWN( mul_and_subT, depth, 0,
				    a->child[_00], b->child[TR_01],
				    r01 );
          }

          if (a->child[_10] && b->child[TR_00])
              SPAWN( mul_and_subT, depth, 0,
                               a->child[_10], b->child[TR_00],
	                       r10 );

          if (a->child[_10] && b->child[TR_01])
              SPAWN( mul_and_subT, depth, lower,
			       a->child[_10], b->child[TR_01],
			       r11 );

          /* Now SYNC them */

          if (a->child[_10] && b->child[TR_01])
              r11 = SYNC( mul_and_subT );
          if (a->child[_10] && b->child[TR_00])
              r10 = SYNC( mul_and_subT );
          if( !lower && a->child[_00] && b->child[TR_01] ) {
              r01 = SYNC( mul_and_subT );
          }
          if (a->child[_00] && b->child[TR_00])
              r00 = SYNC( mul_and_subT );

          /* SPAWN some more */

          if (a->child[_01] && b->child[TR_10])
               SPAWN( mul_and_subT, depth, lower, a->child[_01], b->child[TR_10], r00 );

	  if (!lower  && a->child[_01] && b->child[TR_11])
	       SPAWN( mul_and_subT, depth, 0,
				    a->child[_01], b->child[TR_11], r01 );

          if (a->child[_11] && b->child[TR_10])
               SPAWN( mul_and_subT, depth, 0, a->child[_11], b->child[TR_10], r10 );

	  if (a->child[_11] && b->child[TR_11])
               SPAWN( mul_and_subT, depth, lower, a->child[_11], b->child[TR_11], r11 );

          /* And SYNC */

          if (a->child[_11] && b->child[TR_11])
               r11 = SYNC( mul_and_subT );
          if (a->child[_11] && b->child[TR_10])
               r10 = SYNC( mul_and_subT );
          if( !lower && a->child[_01] && b->child[TR_11] )
               r01 = SYNC( mul_and_subT );
          if (a->child[_01] && b->child[TR_10])
               r00 = SYNC( mul_and_subT );

	  if (r == NULL) {
	       if (r00 || r01 || r10 || r11)
		    r = new_internal(r00, r01, r10, r11);
	  } else {
	       assert(r->child[_00] == NULL || r->child[_00] == r00);
	       assert(r->child[_01] == NULL || r->child[_01] == r01);
	       assert(r->child[_10] == NULL || r->child[_10] == r10);
	       assert(r->child[_11] == NULL || r->child[_11] == r11);
	       r->child[_00] = r00;
	       r->child[_01] = r01;
	       r->child[_10] = r10;
	       r->child[_11] = r11;
	  }
     }
     return r;
}

/*
 * Perform substitution to solve for B in BL = A
 * Returns B in place of A.
 */
TASK_3( Matrix, backsub, int, depth, Matrix, a, Matrix, l )
{
     assert(a != NULL);
     assert(l != NULL);

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  LeafNode *L = (LeafNode *) l;
	  block_backsub(A->block, L->block);
     } else {
	  Matrix a00, a01, a10, a11;
	  Matrix l00, l10, l11;

	  depth--;

	  a00 = a->child[_00];
	  a01 = a->child[_01];
	  a10 = a->child[_10];
	  a11 = a->child[_11];

	  l00 = l->child[_00];
	  l10 = l->child[_10];
	  l11 = l->child[_11];

	  assert(l00 && l11);

	  if (a00)
               SPAWN( backsub, depth, a00, l00);
	  if (a10)
               a10 = CALL( backsub, depth, a10, l00 );
	  if( a00 )
               a00 = SYNC( backsub );


          if( a00 && l10 )
              SPAWN( mul_and_subT, depth, 0, a00, l10, a01 );
          if( a10 && l10 )
              a11 = CALL( mul_and_subT, depth, 0, a10, l10, a11 );
          if( a00 && l10 )
	      a01 = SYNC( mul_and_subT );

	  if( a01 ) {
               SPAWN( backsub, depth, a01, l11 );
          }
	  if( a11 ) {
               a11 = CALL( backsub, depth, a11, l11 );
          }
	  if( a01 ) {
               a01 = SYNC( backsub );
          }

	  a->child[_00] = a00;
	  a->child[_01] = a01;
	  a->child[_10] = a10;
	  a->child[_11] = a11;
     }

     return a;
}

/*
 * Compute Cholesky factorization of A.
 */

TASK_2( Matrix, cholesky, int, depth, Matrix, a )
{
     assert(a != NULL);

     if (depth == BLOCK_DEPTH) {
	  LeafNode *A = (LeafNode *) a;
	  block_cholesky(A->block);
     } else {
	  Matrix a00, a10, a11;

	  depth--;

	  a00 = a->child[_00];
	  a10 = a->child[_10];
	  a11 = a->child[_11];

	  assert(a00);

	  if (!a10) {
	       assert(a11);
	       SPAWN( cholesky, depth, a00 );
	       a11 = CALL( cholesky, depth, a11 );
	       a00 = SYNC( cholesky );
	  } else {
	       a00 = CALL( cholesky, depth, a00 );
	       assert(a00);

	       a10 = CALL( backsub, depth, a10, a00 );
	       assert(a10);

	       a11 = CALL( mul_and_subT, depth, 1, a10, a10, a11 );
	       assert(a11);

	       a11 = CALL( cholesky, depth, a11 );
	       assert(a11);
	  }
	  a->child[_00] = a00;
	  a->child[_10] = a10;
	  a->child[_11] = a11;
     }
     return a;
}

int logarithm(int size)
{
     int k = 0;

     while ((1 << k) < size)
	  k++;
     return k;
}

int usage(void)
{
     fprintf(stderr,
	     "\nUsage: cholesky [<wool-options>] [-n size] [-z nonzeros]\n"
     "                [-f filename] [-benchmark] [-h]\n\n"
     "Default: cholesky -n 500 -z 1000\n\n"
     "This program performs a divide and conquer Cholesky factorization of a\n"
     "sparse symmetric positive definite matrix (A=LL^T).  Using the fact\n"
     "that the matrix is symmetric, Cholesky does half the number of\n"
     "operations of LU.  The method used is the same as with LU, with work\n"
     "Theta(n^3) and critical path Theta(n lg(n)) for the dense case.  A\n"
     );fprintf(stderr,	                                                               /* break the string into smaller pieces.  ISO requires C89 compilers to support at least 509-characterstrings */
     "quad-tree is used to store the nonzero entries of the sparse\n"
     "matrix. Actual work and critical path are influenced by the sparsity\n"
     "pattern of the matrix.\n\n"
     "The input matrix is either read from the provided file or generated\n"
     "randomly with size and nonzero-elements as specified.\n\n");
     return 1;
}

char *specifiers[] = {"-n", "-z", "-f", "-benchmark", "-h", "-r", 0};
int opt_types[] =
{INTARG, INTARG, STRINGARG, BENCHMARK, BOOLARG, INTARG, 0};

TASK_2( int, main, int, argc, char**, argv )
{
     Matrix A, R;
     int size, depth, nonzeros, i, benchmark, help;
     int input_nonzeros, input_blocks, output_nonzeros, output_blocks;
     Real error;
     char buf[1000], filename[100];
     int sizex, sizey;
     int repetitions=1;
     FILE *f;

     A = NULL;

     /* standard benchmark options */
     filename[0] = 0;
     size = 1000;
     nonzeros = 10000;

     get_options(argc, argv, specifiers, opt_types,
		 &size, &nonzeros, filename, &benchmark, &help, &repetitions);

     if (help)
	  return usage();

     if (benchmark) {
	  switch (benchmark) {
	      case 1:		/* short benchmark options -- a little work */
		   filename[0] = 0;
		   size = 128;
		   nonzeros = 100;
		   break;
	      case 2:		/* standard benchmark options */
		   filename[0] = 0;
		   size = 1000;
		   nonzeros = 10000;
		   break;
	      case 3:		/* long benchmark options -- a lot of work */
		   filename[0] = 0;
		   size = 2000;
		   nonzeros = 10000;
		   break;
	  }
     }
     if (filename[0]) {
	  f = fopen(filename, "r");
	  if (f == NULL) {
	       printf("\nFile not found!\n\n");
	       return 1;
	  }
	  /* throw away comment lines */
	  do {
	      char* rval = fgets(buf, 1000, f);
              if( rval == NULL ) break;
	  } while (buf[0] == '%');

	  sscanf(buf, "%d %d", &sizex, &sizey);
	  assert(sizex == sizey);
	  size = sizex;

	  depth = logarithm(size);

	  srand(61066);
	  nonzeros = 0;

	  while (!feof(f)) {
	       double fr, fc;
	       int r, c;
	       Real val;
	       int res;

	       char *rval = fgets(buf, 1000, f);
               if( rval == NULL ) break;

	       res = sscanf(buf, "%lf %lf %lf", &fr, &fc, &val);
	       r = fr;
	       c = fc;
	       if (res <= 0)
		    break;
	       /*
		* some Matrix Market Matrices have no values, only
		* patterns. Then generate values randomly with a
		* nice big fat diagonal for Cholesky
	        */
	       if (res == 2) {
		    double rnd = ((double)rand()) / (double)RAND_MAX;
		    val = (r == c ? 100000.0 * rnd : rnd);
	       }

	       r--;
	       c--;
	       if (r < c) {
		    int t = r;
		    r = c;
		    c = t;
	       }
	       assert(r >= c);
	       assert(r < size);
	       assert(c < size);
	       A = set_matrix(depth, A, r, c, val);
	       nonzeros++;
	  }
     } else {
	  /* generate random matrix */

	  depth = logarithm(size);

	  /* diagonal elements */
	  for (i = 0; i < size; i++)
	       A = set_matrix(depth, A, i, i, 1.0);

	  /* off-diagonal elements */
	  for (i = 0; i < nonzeros - size; i++) {
	       int r, c;
	     again:
	       r = rand() % size;
	       c = rand() % size;
	       if (r <= c)
		    goto again;
	       if (get_matrix(depth, A, r, c) != 0.0)
		    goto again;
	       A = set_matrix(depth, A, r, c, 0.1);
	  }
     }

     /* extend to power of two size with identity matrix */
     for (i = size; i < (1 << depth); i++) {
	  A = set_matrix(depth, A, i, i, 1.0);
     }

     R = CALL( copy_matrix, depth, A);

     input_blocks = num_blocks(depth, R);
     input_nonzeros = num_nonzeros(depth, R);

     for( i=0; i<repetitions; i++ ) {
       R = CALL( cholesky, depth, R);
       if( i<repetitions-1 ) {
         free_matrix(depth, R);
         R = CALL( copy_matrix, depth, A );
       }
     }

     output_blocks = num_blocks(depth, R);
     output_nonzeros = num_nonzeros(depth, R);

     /* test - make sure R * Transpose(R) == A */
     /* compute || A - R * Transpose(R) ||    */

     A = CALL( mul_and_subT, depth, 1, R, R, A ); // not ok if R==NULL
     error = mag(depth, A);

     printf("\nWool Example: cholesky\n");
     printf("Error: %f\n\n", error);
     printf("Options: original size     = %d\n", size);
     printf("         original nonzeros = %d\n", nonzeros);
     printf("         input nonzeros    = %d\n", input_nonzeros);
     printf("         input blocks      = %d\n", input_blocks);
     printf("         output nonzeros   = %d\n", output_nonzeros);
     printf("         output blocks     = %d\n\n", output_blocks);

     free_matrix(depth, A);
     free_matrix(depth, R);

     return 0;
}
