/*
   This file is part of Wool, a library for fine-grained independent
   task parallelism

   Copyright 2009- Peter Jonsson, pj@sics.se
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
*/


/* We are in the same namespace as the examples so keep names unique. */

int fib2( int );

TASK_1( int, pfib2, int, n )
{
    // Make a regular call to lose track of __self and __dq_top.
    return fib2( n );
}

TASK_1( int, pfib2_int, int, n )
{
   if( n < 2 ) {
      return n;
   } else {
      int m,k;
      SPAWN( pfib2_int, n-1 );
      k = fib2( n-2 );
      m = SYNC( pfib2_int );
      return m+k;
   }
}

int fib2( int n )
{
   if( n < 2 ) {
      return n;
   } else {
      int m,k;
      SPAWN( pfib2_int, n-1 );
      k = fib2( n-2 );
      m = SYNC( pfib2_int );
      return m+k;
   }
}
