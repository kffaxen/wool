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

#define _GNU_SOURCE
#include <assert.h>
#include <sched.h> // For improved multiprocessor performance
#include <time.h>  // d:o
#include "wool-common.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/time.h>
#include <signal.h>
#include <sys/types.h>

#define ST_OLD     1
#define ST_THIEF   2
#define ST_SAMPLED 4

#ifndef AVOID_RANDOM
  #define AVOID_RANDOM 0
#endif

#ifndef WOOL_STEAL_PARSAMP
  #define WOOL_STEAL_PARSAMP 0
#endif

#if WOOL_STEAL_PARSAMP
  #define WOOL_STEAL_NEW_SET 1
  #define WOOL_STEAL_SAMPLE  1
#endif

#define dprint(s, ...) (fprintf( stderr, s, ##__VA_ARGS__))

#ifndef MIGRATE_WORKERS
  #define MIGRATE_WORKERS 1
#endif

#ifndef WOOL_TIME
  #define WOOL_TIME 0
#endif

#ifndef WOOL_SYNC_NOLOCK
  #define WOOL_SYNC_NOLOCK 1
#endif

#ifndef WOOL_STEAL_NOLOCK
  #define WOOL_STEAL_NOLOCK (! THE_SYNC )
#endif

#ifndef WOOL_STEAL_SKIP
  #define WOOL_STEAL_SKIP 0
#endif

#ifndef STEAL_TRYLOCK
  #define STEAL_TRYLOCK 1
#endif

#ifndef WOOL_STEAL_SAMPLE
  #define WOOL_STEAL_SAMPLE 1
#endif

#if WOOL_STEAL_SAMPLE
  #define WOOL_STEAL_DKS 0
#endif

#ifndef WOOL_STEAL_DKS
  #define WOOL_STEAL_DKS 0
#endif

#ifndef WOOL_INIT_SPIN
  #define WOOL_INIT_SPIN 1
#endif

#ifndef WOOL_DEFER_BOT_DEC_OPT
  #define WOOL_DEFER_BOT_DEC_OPT 0
#endif

#ifndef WOOL_STEAL_NEW_SET
  #define WOOL_STEAL_NEW_SET 1
#endif

#ifndef WOOL_STEAL_BACK
  #define WOOL_STEAL_BACK 0
#endif

#ifndef WOOL_STEAL_OLD_SET
  #define WOOL_STEAL_OLD_SET 0
#endif

#ifndef WOOL_TRLF_ORIG_OFTEN
  #define WOOL_TRLF_ORIG_OFTEN 1
#endif

#ifndef WOOL_TRLF_RESET_SEEN
  #define WOOL_TRLF_RESET_SEEN 0
#endif

#ifndef WOOL_TRLF
  #define WOOL_TRLF 0
#endif

#ifndef WOOL_STEAL_REPF
  #define WOOL_STEAL_REPF 0
#endif

#define WOOL_STEAL_SET (WOOL_STEAL_NEW_SET || WOOL_STEAL_OLD_SET)

#ifndef EXACT_STEAL_OUTCOME
  #define EXACT_STEAL_OUTCOME 0
#endif

#ifndef STEAL_PEEK
  #define STEAL_PEEK 1
#endif

#define WOOL_STEAL_OO (WOOL_STEAL_NOLOCK && !SINGLE_FIELD_SYNC && !TWO_FIELD_SYNC)

#if defined(__TILECC__)
  MALLOC_USE_HASH(0);
  #include <sys/alloc.h>
#endif

#define SO_STOLE 0
#define SO_BUSY  1
#define SO_NO_WORK 2
#define SO_THIEF 3
#define SO_FAIL(n) (2+(n))

#if WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET
  #define SO_NUM_THIEVES( n ) ((n)-2)
  #define SO_IS_FAIL(n) ((n) > 1)
#elif WOOL_STEAL_NEW_SET
  #define SO_NUM_THIEVES( n ) 1
  #define SO_IS_FAIL(n) ((n) == SO_THIEF)
#endif

#define SO_CL    10 // A classical leap as outcome of transitive leap frogging

static void *look_for_work( void *arg );

WOOL_WHEN_MSPAN( hrtime_t __wool_sc = 1000; )

static Worker **workers;
static Task   **bases;
static int n_workers = 0, n_procs = 0, n_threads = 0;
static int backoff_mode = 960; __attribute__((unused)) // No of iterations of waiting after
static int n_stealable = -1;
#if defined(__TILECC__)
  static size_t worker_stack_size = 6*1024*1024;
#else
  static size_t worker_stack_size = 12*1024*1024;
#endif

int wool_get_nworkers(void)
{
  return n_workers;
}

int wool_get_worker_id(void)
{
  return _WOOL_(slow_get_self)()->pr.idx;
}

void work_for( workfun_t fun, void *arg )
{
  int i;
  int idx = wool_get_worker_id();
  for( i = 0; i < n_procs; i++ ) {
    // Skip locking against ourselves.
    if( i == idx) continue;
    wool_lock( &( workers[i]->pu.work_lock ) );
    workers[i]->pu.fun = fun;
    workers[i]->pu.fun_arg = arg;
    wool_unlock( &( workers[i]->pu.work_lock ) );
    pthread_cond_signal( &( workers[i]->pu.work_available ) );
  }
}

_wool_thread_local _WOOL_(key_t) tls_self;

#define THIEF_IDX_BITS       12
#define MAKE_THIEF_INFO(t,o) ( (wrapper_t) ( ((o) << THIEF_IDX_BITS) + (t) ) )
#define INFO_GET_THIEF(to)   ( ((unsigned long) (to)) & ((1 << THIEF_IDX_BITS) - 1) )
#define INFO_GET_BASE(to)     ( ((unsigned long) (to)) >> THIEF_IDX_BITS )


typedef enum { AA_HERE, AA_DIST } alloc_t;

static void *alloc_aligned( size_t nbytes, alloc_t  where )
{
  #if defined(__TILECC__)
    alloc_attr_t attr = ALLOC_INIT;
    int m = ALLOC_HOME_HASH;

    switch( where ) {
      case AA_HERE: m = ALLOC_HOME_TASK; break;
      case AA_DIST: m = ALLOC_HOME_HASH; break;
    }

    alloc_set_home( &attr, m );
    return alloc_map( &attr, nbytes );
  #else
    return valloc( nbytes );
  #endif
}

static void make_common_data( int n )
{
  void *block;

  block = alloc_aligned( 2 * n * sizeof(void *), AA_DIST );
  if( block == NULL ) {
    fprintf( stderr, "Out of memory" );
    exit( 1 );
  }
  workers     = (Worker **) (block);
  bases       = (Task **) (block + n*sizeof(void *));
}

static int global_pref_dist = 0;
static int global_trlf_threshold = 1;

#if WOOL_STEAL_OLD_SET
static int global_segment_size = 10;
static int global_refresh_interval = 100;
static int global_thieves_per_victim_x10 = 30;
#endif

#if WOOL_STEAL_DKS
static int global_n_thieves = 1;
static int global_n_blocks = 3;
#endif

static char *log_file_name = NULL;

static int steal_one( Worker *, Worker *, wrapper_t, int, volatile Task *, unsigned long );

static pthread_t *ts = NULL;

#define MAX_THREADS 1024

static int affinity_table[MAX_THREADS];

#if SYNC_MORE
static wool_lock_t more_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

static wool_cond_t sleep_cond;
static wool_lock_t sleep_lock = PTHREAD_MUTEX_INITIALIZER;
static int old_thieves = 0, max_old_thieves = -1;

static pthread_attr_t worker_attr;

#if LOG_EVENTS
static int event_mask = -1;
#endif

static int workers_per_thread = 0;

static long long unsigned us_elapsed(void)
{
  static long long unsigned start;
  static int            called = 0;
  struct timeval now;
  long long unsigned t;

  gettimeofday( &now, NULL );

  t = now.tv_sec * 1000000LL + now.tv_usec;

  if( !called ) {
    start = t;
    called = 1;
  }

  return t-start;
}

static long long unsigned
	             milestone_bcw,
	             milestone_acw,
	             milestone_air,
	             milestone_aid,
	             milestone_art,
	             milestone_bwj,
	             milestone_awj,
	             milestone_end;

#if WOOL_PIE_TIMES || WOOL_MEASURE_SPAN
static long long unsigned count_at_init_done;
#endif

#if WOOL_MEASURE_SPAN || LOG_EVENTS || WOOL_PIE_TIMES

static hrtime_t gethrtime(void);

static double ticks_per_ms;

// real time as a 64 bit unsigned
#if defined(__i386__) || defined(__x86_64__) || defined(__TILECC__)

static hrtime_t gethrtime()
{
  unsigned int hi,lo;
  unsigned long long t;

#if defined(__i386__) || defined(__x86_64__)

  asm volatile("rdtsc" : "=a"(lo), "=d"(hi));

#elif defined(__TILECC__)

  do {
    hi = __insn_mfspr( 0x4e06 );
    lo = __insn_mfspr( 0x4e07 );
  } while( /* lo < 700000 && */ hi != __insn_mfspr( 0x4e06 ) );

#endif

  t = hi;
  t <<= 32;
  t  += lo;

  return t;
}

#else

static hrtime_t gethrtime()
{
  struct timespec t;
  clock_gettime( CLOCK_REALTIME, &t );
  return 1000000000LL * t.tv_sec + t.tv_nsec;
}

#endif

#endif

#if WOOL_PIE_TIMES

void time_event( Worker *w, int event )
{
  hrtime_t now = gethrtime(),
           prev = w->pr.time;

  switch( event ) {

    // Enter application code
    case 1 :
        if(  w->pr.clock /* level */ == 0 ) {
          PR_ADD( w, CTR_init, now - prev );
          w->pr.clock = 1;
        } else if( w->pr.clock /* level */ == 1 ) {
          PR_ADD( w, CTR_wsteal, now - prev );
          PR_ADD( w, CTR_wstealsucc, now - prev );
        } else {
          PR_ADD( w, CTR_lsteal, now - prev );
          PR_ADD( w, CTR_lstealsucc, now - prev );
        }
        break;

    // Exit application code
    case 2 :
        if( w->pr.clock /* level */ == 1 ) {
          PR_ADD( w, CTR_wapp, now - prev );
        } else {
          PR_ADD( w, CTR_lapp, now - prev );
        }
        break;

    // Enter sync on stolen
    case 3 :
        if( w->pr.clock /* level */ == 1 ) {
          PR_ADD( w, CTR_wapp, now - prev );
        } else {
          PR_ADD( w, CTR_lapp, now - prev );
        }
        w->pr.clock++;
        break;

    // Exit sync on stolen
    case 4 :
        if( w->pr.clock /* level */ == 1 ) {
          fprintf( stderr, "This should not happen, level = %d\n", w->pr.clock );
        } else {
          PR_ADD( w, CTR_lsteal, now - prev );
        }
        w->pr.clock--;
        break;

    // Return from failed steal
    case 7 :
        if( w->pr.clock /* level */ == 0 ) {
          PR_ADD( w, CTR_init, now - prev );
        } else if( w->pr.clock /* level */ == 1 ) {
          PR_ADD( w, CTR_wsteal, now - prev );
        } else {
          PR_ADD( w, CTR_lsteal, now - prev );
        }
        break;

    // Signalling time
    case 8 :
        if( w->pr.clock /* level */ == 1 ) {
          PR_ADD( w, CTR_wsignal, now - prev );
          PR_ADD( w, CTR_wsteal, now - prev );
        } else {
          PR_ADD( w, CTR_lsignal, now - prev );
          PR_ADD( w, CTR_lsteal, now - prev );
        }
        break;

    // Done
    case 9 :
        if( w->pr.clock /* level */ == 0 ) {
          PR_ADD( w, CTR_init, now - prev );
        } else {
          PR_ADD( w, CTR_close, now - prev );
        }
        break;

    default: return;
  }

  w->pr.time = now;
}

#endif

#if WOOL_MEASURE_SPAN

static hrtime_t last_span, last_time, first_time;

#if defined(__i386__) || defined(__x86_64__)
static hrtime_t overhead = 10;
#elif defined(__TILECC__)
static hrtime_t overhead = 40;
#else
static hrtime_t overhead = 90;
#endif

hrtime_t __wool_update_time( void )
{
  hrtime_t now       = gethrtime();
  hrtime_t this_time = now - last_time;

  last_span += this_time < overhead ? 0 : this_time - overhead;
  last_time = now;

  first_time += overhead;

  // fprintf( stderr, "last = %lld\n", last_span );

  return last_span;
}

void __wool_set_span( hrtime_t span )
{
  // fprintf( stderr, "last = %lld, new = %lld\n", last_span, span );

  last_span = span;
}

#endif

#if LOG_EVENTS

static LogEntry *logbuff[100];

static volatile hrtime_t diff[100], trip[100], first_time;



static hrtime_t advance_time( Worker *self, hrtime_t delta_t )
{
  const hrtime_t time_field_range = COMPACT_LOG ? 256 : MINOR_TIME;

  while( delta_t >= MINOR_TIME ) {
    hrtime_t t = delta_t / MINOR_TIME;

    if( t >= time_field_range ) t = time_field_range-1;

    self->pr.logptr->what = 0;
    self->pr.logptr->time = (timefield_t) t;
    self->pr.logptr ++;

    delta_t -= t * MINOR_TIME;
  }

  return delta_t;
}


void logEvent( Worker *self, int what )
{
  LogEntry *p;
  int event_class = what < 20 ? what : ( what < 100 ? 20 : 24 + (what-100)/1024 );
  hrtime_t delta_t, now;

  if( ( event_mask & (1<<event_class) ) == 0 ) {
    return;
  }

  now = gethrtime( );
  delta_t = now - self->now;
  self->now = now - delta_t % TIME_STEP;
  if( delta_t >= MINOR_TIME ) {
    delta_t = advance_time( self, delta_t );
  }

  p = self->pr.logptr;
  p->what = what;
  p->time = (timefield_t) ( delta_t / TIME_STEP );

  self->pr.logptr ++;
  p++;

  if( ((unsigned long) p) % 64 == 0 ) {
     __builtin_prefetch( p + (64 / (sizeof(LogEntry))), 1 );
  }

}

static void sync_clocks( Worker *self )
{
  hrtime_t slave_time=0;
  int i,k;

  for( i=0; i<2; i++ ) {
    while( self->pr.clock != 1 ) ;
    self->pr.clock = 2;
    for( k=3; k<8; k+=2 ) {
      while( self->pr.clock != k ) ;
      if( k==5 ) slave_time = gethrtime();
      self->pr.clock = k+1;
    }
  }
  while( self->pr.clock != 9 ) ;
  self->pr.time = slave_time;
  COMPILER_FENCE;
  self->pr.clock = 10;
  for( i = self->pr.idx; i < self->pr.idx + workers_per_thread; i++ ) {
    workers[i]->now = first_time;
  }
}



static void master_sync(void)
{
  int i,j,k;
  hrtime_t master_time, round_trip;

  for( i=1; i<n_procs; i++ ) {
    Worker *slave = workers[i*workers_per_thread];
    for( j=0; j<2; j++ ) {
      slave->pr.clock = 1;
      while( slave->pr.clock != 2 ) ;
      master_time = gethrtime( );
      for( k=3; k<8; k+=2 ) {
        slave->pr.clock = k;
        while( slave->pr.clock != k+1 ) ;
      }
      round_trip = gethrtime() - master_time;
    }
    slave->pr.clock = 9;
    while( slave->pr.clock != 10 ) ;
    diff[i] = master_time + round_trip/2 - slave->pr.time;
    trip[i] = round_trip;
  }
  diff[0] = 0;
  trip[0] = 0;
  for( i = 0; i < workers_per_thread; i++ ) {
    workers[i]->now = first_time;
  }
}

#endif

// Call switch_to_other_worker with t==NULL from the search loop
// or with t pointing to the task we're waiting for

/* The logic is as follows: This function is called either when stealing or leap frogging
   has proved unsuccessful with the intent that we try another worker. Our first
   priority is to wake up a worker waiting at a sync that has become unblocked; we
   do this in preference to stealing. Second, if we are blocked at a sync, we try to go
   stealing, which we can do either by finding a new context and search for work, or
   by waking up a blocked worker now able to leap frog.
   A parked worker has wait_for==NULL and will search for work when woken
   A blocked worker has wait_for != NULL and will try to sync when woken
   When called with t==NULL:
     First, try to find a blocked worker that can be resumed (the leftmost)
     Second, return to look for work by stealing
   When called with t!=NULL:
     First, try to find a resumable worker (the leftmost)
     Second, try to find a parked worker
     Third, resume a blocked worker that might be able to steal (the clockwise one,
     certain to exist since we got here)
*/

static int switch_interval = 10000
#if WOOL_STEAL_DKS
           , worker_migration_interval = 10000
#endif
           ;
#if THREAD_GARAGE

static struct _Garage {
  pthread_mutex_t lck;
  pthread_cond_t cnd;
} *garage = NULL;
static void maybe_sleep( Worker *self ) __attribute__((unused));

static void maybe_sleep( Worker *self )
{
  pthread_mutex_lock( &(garage[self->pr.idx].lck) );

  if( self->pr.idx % workers_per_thread == 0 ) {
    // I am a leader (jIDev)
  } else {
    // I am a follower, go to sleep (jIDevlu', jIQong)
    self->pu.is_running = 0;
    #if WOOL_STEAL_SET || WOOL_STEAL_DKS
      self->pu.is_thief = 0;
    #endif
    self->pr.wait_for = NULL;
    pthread_cond_wait( &(garage[self->pr.idx].cnd), &(garage[self->pr.idx].lck) );

    self->pu.is_running = 1;
    #if WOOL_STEAL_SET || WOOL_STEAL_DKS
      self->pu.is_thief = 1;
    #endif
  }
}

static void evacuate_garage(void)
{
  int i;

  MFENCE;
  for( i = 1; i<n_workers; i++ ) {
    Worker *w = workers[i];
    // w will exit if is_running is true when it reads more_work in do_switch.
    if( !w->pu.is_running ) {
      pthread_mutex_lock( &(garage[w->pr.idx].lck) );
        pthread_cond_signal( &(garage[w->pr.idx].cnd) );
      pthread_mutex_unlock( &(garage[w->pr.idx].lck) );
    }
  }
}


#endif

static int do_switch( Worker *self, Worker *other, Task *t )
{
  // really switch
  // this includes
  //   marking the worker as not actively searching (for SET and DKS stealing)
  //   marking the worker as parked or blocked
  //   ensuring that we return after resumption
  //   doing the context switch

  if( other->pu.is_running || pthread_mutex_trylock( &(garage[other->pr.idx].lck) ) != 0 ) { // TODO: public idx
    return 0;
  }
  // We've locked other
  if( other->pu.is_running ) {
    pthread_mutex_unlock( &(garage[other->pr.idx].lck) );
    return 0;
  }
  // It was available for waking; not anymore, though
  other->pu.is_running = 1; // Will soon be true, anyway
  pthread_mutex_unlock( &(garage[other->pr.idx].lck) );

  // Now we wake the other thread
  pthread_cond_signal( &(garage[other->pr.idx].cnd) );

  // Now prepare to go to sleep
  #if WOOL_STEAL_SET || WOOL_STEAL_DKS
    if( t==NULL ) self->pu.is_thief = 0;
  #endif
  self->pr.wait_for = t;

  // Tell others I am available, effective when I release my lock
  self->pu.is_running = 0;

  MFENCE;

  // jIQong
  if( self->pr.more_work )
    pthread_cond_wait( &(garage[self->pr.idx].cnd), &(garage[self->pr.idx].lck) );

  // Ok, now someone has woken us, better make it official
  self->pu.is_running = 1;
  #if WOOL_STEAL_SET || WOOL_STEAL_DKS
    if( t==NULL ) self->pu.is_thief = 1;
  #endif

  // We should tell caller we're done
  return 1;

}

static int look_for_worker_to_resume( Worker *self, Task *t, int from, int to )
{
  int i;
  int self_idx = self->pr.idx;

  for( i = from; i < to; i++ ) {
    if( i != self_idx && workers[i]->pr.wait_for != NULL ) {
     #if TWO_FIELD_SYNC
      int is_done = workers[i]->pr.wait_for->f == SFS_DONE; // No longer blocked in join
     #else
      int is_done = workers[i]->pr.wait_for->balarm == STOLEN_DONE; // No longer blocked in join
     #endif
      if( is_done && do_switch( self, workers[i], t ) ) {
        return 1;
      }
    }
  }
  return 0; // We did not find a worker to resume
}


static int switch_to_other_worker( Worker *, Task *, int ) __attribute__((unused));

static int switch_to_other_worker( Worker *self, Task *t, int migrate )
{
  int self_idx = self->pr.idx;
  int lead_worker;
  int i;

  if( workers_per_thread == 1 ) return 1;

  lead_worker = self_idx - self_idx%workers_per_thread;

  if( look_for_worker_to_resume( self, t, lead_worker, lead_worker+workers_per_thread ) )
  {
    return 1;
  }

  // We did not find a resumable worker of our current thread
  // If we were looking for work, go back to that rather than park
  //
  if( t==NULL ) {
    if( migrate ) {
      look_for_worker_to_resume( self, t, 0, n_workers );
    }
    return 1;
  }

  // Ok, we were leap frogging and found nothing to do; unpark a worker if possible
  // the worker we wake up was previously looking for work, not leaping
  // Maybe we really should look at more workers here...
  //
  for( i = lead_worker; i < lead_worker+workers_per_thread; i++ ) {
    if( i != self_idx && workers[i]->pr.wait_for == NULL
        && do_switch( self, workers[i], t )
      ) {
      return 1;
    }
  }

  // Now we try to migrate another worker
  //
  if( look_for_worker_to_resume( self, t, 0, n_workers ) ) {
    return 1;
  }

  // We're out of parked workers as well, so we switch to another blocked
  // worker in the hope that it will be able to leapfrog.
  //
  if( lead_worker <= self_idx && self_idx < lead_worker+workers_per_thread ) {
    i = lead_worker + (self_idx + 1 - lead_worker) % workers_per_thread; // Go clockwise
    do_switch( self, workers[i], t );
  } else {
    for( i = lead_worker; i < lead_worker+workers_per_thread; i++ ) {
      if( do_switch( self, workers[i], t ) ) {
        return 1;
      }
    }
  }

  return 0;
}

static void reswitch_worker( Worker *self )
{
}

#if WOOL_INIT_SPIN
static volatile int *init_barrier;
#else
static wool_lock_t init_lock = PTHREAD_MUTEX_INITIALIZER;
static wool_cond_t init_cond = PTHREAD_COND_INITIALIZER;
static int n_initialized = 0;
#endif

static void wait_for_init_done(int p_idx)
{
#if WOOL_INIT_SPIN
  if( p_idx + 1 < n_procs ) {
    while( init_barrier[p_idx+1] == 0 ) ;
  }
  init_barrier[p_idx] = 1;
  while( init_barrier[0] == 0 ) ;
#else
  wool_lock( &init_lock );
  n_initialized++;
  // fprintf( stderr, "Init %d\n", n_initialized );
  if( n_initialized == n_procs ) {
    wool_unlock( &init_lock );
    wool_broadcast( &init_cond );
  } else {
    wool_wait( &init_cond, &init_lock );
    wool_unlock( &init_lock );
  }
#endif
}

static int yield_interval = 10000; // Also set from command line
static int sleep_interval = 100000; // Wait after so many attempts, also set by '-i'

// Decrement old thieves when an old thief successfully
// steals but before the call. Since old_thieves is always <= max_old_thieves,
// one old thief is freed from jail (pardoned).
// A worker becomes an old thief after old_thief_age unsuccessful steal
// attempts. If there are already max_old_thieves, it goes to sleep on
// the jail.

static int new_old_thief( Worker *self ) __attribute__((unused));

static int new_old_thief( Worker *self )
{
  int is_old;

  wool_lock( &sleep_lock );
  if( old_thieves >= max_old_thieves + 2 ) {
    while( self->pr.more_work && old_thieves >= max_old_thieves + 2 ) {
      wool_wait( &sleep_cond, &sleep_lock );
    }
    is_old = 0;
  } else {
    old_thieves++;
    is_old = 1;
  }
  wool_unlock( &sleep_lock );

  return is_old;
}

static void decrement_old_thieves(void)
{
  int new_old;

  wool_lock( &sleep_lock );
  old_thieves--;
  new_old = old_thieves;
  wool_unlock( &sleep_lock );
  if( new_old < max_old_thieves ) {
    wool_signal( &sleep_cond );
  }
}

#define LARGE_POINTER    ((Task *) -1024L)
#define log_first_block_size 8
#define     first_block_size (1 << log_first_block_size)

/* static */ unsigned block_size( int i )
{
  return first_block_size * (1<<i);
}

static unsigned long start_idx_of_block( int i )
{
  return block_size(i) - first_block_size;
}

#if !defined(WOOL_NO_CLZ)

static inline int block_of_idx( unsigned long t )
{
  int fblz = 8*sizeof(unsigned long) - log_first_block_size - 1;

  return fblz - __builtin_clzl( t + first_block_size );
}

#else

static int block_of_idx( unsigned long t )
{
  if( t < first_block_size ) {
    return 0;
  } else {
    int lo=0, hi=_WOOL_pool_blocks;

    while( hi-lo>1 ) {
      int m = (hi+lo)/2;
      if( t < start_idx_of_block( m ) ) {
        hi = m;
      } else {
        lo = m;
      }
    }
    return lo;
  }
}

#endif

// Currently unused
#if 0
static int block_of_ptr( Worker *w, Task *t )
{
  int i;

  for( i = 0; i < _WOOL_pool_blocks; i++ ) {
    if( (unsigned long) (t - w->pr.block_base[i]) < block_size(i) ) {
      return i;
    }
  }
  return -1;
}
#endif

inline TILE_INLINE unsigned long ptr2idx_curr( Worker *w, Task *t )
{
  Task *base = w->pr.curr_block_base;
  assert( t != NULL );
  assert( t >= base );
  assert( t < base + block_size(w->pr.t_idx) );
  return w->pr.curr_block_fidx + t - base;
}

// Currently unused
#if 0
static Task *idx2ptr_curr( Worker *w, unsigned long t )
{
  Task **blocks = &(w->pr.block_base[0]);
  int    idx = w->pr.t_idx;
  return blocks[idx] + t - start_idx_of_block(idx);
}
#endif

// Compute the size of the public tasks in a block, possibly except the last one
// If this value is n, then
//   low          = base+n
//   private_size = sizeof(Task) * (block_size-1-n)
static unsigned long new_local_public_size( int t_idx, unsigned long n_public )
{
  unsigned long this_base_idx = start_idx_of_block( t_idx ),
                next_base_idx = start_idx_of_block( t_idx+1 );

  if( n_public < this_base_idx ) {
    return 0;
  } else if( n_public < next_base_idx-1 ) {
    return sizeof(Task) * ( n_public - this_base_idx );
  } else {
    return sizeof(Task) * ( block_size( t_idx ) - 1 );
  }
}

static inline Task *idx_to_task_p_pu( Worker *w, unsigned long t, Task *b )
{
  int bidx;
  Task *bb;

  if( t < first_block_size ) {
    return b+t;
  }
  bidx = block_of_idx( t );
  bb = w->pu.pu_block_base[bidx];
  return bb==NULL ? NULL : bb + ( t - start_idx_of_block( bidx ) );
}

static Task *idx_to_task_p( Worker *w, unsigned long t )
{
  int bidx = block_of_idx( t );
  Task *bb = w->pr.block_base[bidx];

  return bb==NULL ? NULL : bb + ( t - start_idx_of_block( bidx ) );
}

// This will not affect syncs. Called by owner to arrange for a
// deferred bot ponter update.
static void throw_spawn_exception(Worker *w)
{
  #if _WOOL_ordered_stores
    w->pr.spawn_high = NULL;
  #else
    w->pr.spawn_first_private = NULL;
  #endif
}

// Affects both syncs and spawns. Called by thieves when requesting
// more public tasks.
static void throw_both_exceptions(Worker *w)
{
  w->pr.join_first_private = LARGE_POINTER;
  throw_spawn_exception( w );
}

// Adjusting the number of stealable tasks

/*
    Conventions for TWO_FIELD_SYNC
    - Private task descriptiors invariably have balarm==TF_OCC and can have any values for f
    - For public task descriptors:
      - A stolen descriptor has balarm==TF_OCC                           (below bot)
      - A valid, not stolen TD has balarm==TF_FREE except transiently    (between bot and top)
      - An empty task descriptor (not used or joined with)               (above top)
        - under TSO: balarm==TF_FREE, f==SFS_EMPTY
        - otherwise: balarm==TF_OCC, f can be anything

    Thus we have the following transitions:
    - on steal:                                 TF_FREE ?> TF_OCC  (balarm) (abort steal on failure)
                                                wrapper -> thief   (f)
                                                execute task
                                                thief   -> SFS_DONE (f)
    - on public join with not stolen under TSO: TF_FREE ?> TF_OCC  (balarm) (slow path on failure)
                                                wrapper -> SFS_EMPTY  (f)
                                                TF_OCC  => TF_FREE (balarm)
    - on public join with stolen under TSO:
                                                TF_OCC  => TF_FREE (balarm)
    - on public join with not stolen otherwise: TF_FREE ?> TF_OCC  (balarm) (slow path on failure)
    - on public join with stolen otherwise:     no transitions are needed
    - on public spawn not under TSO:            any     -> wrapper (f)
                                                TF_OCC  => TF_FREE (balarm)
    - on private spawn not under TSO:           any     -> wrapper (f)
    - on spawn under TSO:                       any     => wrapper (f)
    - on making empty TD public under TSO:      any     -> SFS_EMPTY  (f)
                                                TF_OCC  => TF_FREE (balarm) (balarm ready for spawn)
    - on making empty TD public otherwise:      no transitions are needed
    - on making valid TD public:                TF_OCC  => TF_FREE (balarm)
      Stolen task descriptors are never made public since they cannot be private
      Valid task descriptors are never made private since they may become stolen
    - on making an empty TD private under TSO:  TF_FREE ?> TF_OCC  (balarm) (spin on failure)
    - on making an empty TD private otherwise:  no transitions are needed

    Legend: -> ordinary store
            => store with release semantics
            ?> atomic test&set

    Note: all TF_FREE to TF_OCC transitions must be performed with an atomic instruction



*/

static int unstolen_per_decrement = 500;
static int stealable_chunk_size = 4;
static int steal_margin = 1; // Must be at least one

/*
  When more_stealable is called from slow_spawn(),
    p_idx is the index of the new task, and the new task has been fully initialized.
      It is thus in the valid state (and it is the highest task to be so)
  When more_stealable is called from slow_sync(),
    p_idx is the index of the task descriptor joined with
    - if n_public <= p_idx, the task descriptor we join with was private, hence we
                            certainly have not done exch_busy_balarm() on it.
                            If it becomes public, we will do that later by calling rts_sync()
                            Hence it should be treated as the last valid task.
    - if n_public > p_idx, the task descriptor was public, and is thus not affected
                           by more_stealable

*/

static void more_stealable( Worker *w, unsigned long p_idx )
{
  unsigned long now  = w->pr.n_public;
  unsigned long next = now + stealable_chunk_size;
  unsigned long i;

  logEvent(w,10);

  w->pr.more_public_wanted = 0;  // Nobody else will set it until the wire is enabled

  // At this point, a thief might set more_public_wanted and raise an exception,
  // and we'll catch that in reset_all_derived()

  w->pr.unstolen_stealable = unstolen_per_decrement;
  w->pr.n_public = next;

  // We also have a public version of n_public to avoid false sharing
  w->pu.pu_n_public = next;

  SFENCE;

  for( i = now; i < next; i++ ) {
    Task *t = idx_to_task_p( w, i );
    if( t==NULL ) break;
    // Under TSO, an invalid public task will have balarm==TF_FREE, so it must
    // have its f field cleared in order not to be stolen again
    if( _WOOL_ordered_stores && i > p_idx ) {
      t->f = SFS_EMPTY;
      SFENCE;
    }
    // When stores are not ordered, invalid public tasks have balarm==TF_OCC,
    // hence need not be written in that case
    if( _WOOL_ordered_stores || i <= p_idx ) {
      t->balarm = TF_FREE;
    }
    if( LOG_EVENTS && i <= p_idx ) {
      logEvent( w, 7 );
    }
  }

  PR_INC( w, CTR_add_stealable );

}

static inline int maybe_more_stealable( Worker *self, unsigned long p_idx )
{
  if( self->pr.more_public_wanted ) {
    more_stealable( self, p_idx );
    return 1;
  } else {
    return 0;
  }
}

// The argument task will be TF_FREE sooner or later since it is not stealable

static void synchronized_privatize( volatile Task *t )
{
  #if TWO_FIELD_SYNC && _WOOL_ordered_stores
    balarm_t a = TF_FREE;
    do {
      while( a == TF_OCC ) {
        a = t->balarm;
      }
      a = _WOOL_(exch_busy_balarm)( &(t->balarm) );
    } while( a == TF_OCC );
  #endif
}

static void less_stealable(Worker *self, Task *q )
{
  long unsigned top_idx = ptr2idx_curr( self, q );
  long unsigned curr = self->pr.n_public,
                next,i;
  long unsigned high;

  /* We can only privatize empty task descriptors
     q (and top_idx) is the lowest guaranteed empty one (top_idx is one above the idx of the
        task to join with).
     The new wire will be at top or above, thus the new value of n_public will
     be top_idx+steal_margin, which must be less than the old value
  */

  high = self->pr.highest_bot;
  high = high < self->pu.dq_bot ? self->pu.dq_bot : self->pr.highest_bot;
  self->pr.highest_bot = 0;

  if( curr <= top_idx + steal_margin || curr <= high ) {
    self->pr.unstolen_stealable = unstolen_per_decrement;
    return;
  }

  PR_INC( self, CTR_sub_stealable );

  next = top_idx + steal_margin;
  if( next < (curr + high) / 2 ) {
    next = (curr + high) / 2;
  }

  for( i = next; i<curr; i++ ) {
    Task *t = idx_to_task_p( self, i );
    if( t==NULL ) break;
    synchronized_privatize( t );
  }

  self->pr.n_public = next;
  self->pu.pu_n_public = next;
  self->pr.unstolen_stealable = unstolen_per_decrement;
}

static inline void maybe_less_stealable( Worker *self, Task *p )
{
  if( self->pr.unstolen_stealable <= 0 ) {
    less_stealable( self, p );
  }
}

// Run by a successfull thief before dispatching the task

static inline void
maybe_request_stealable( Worker *victim, unsigned long b_idx, unsigned long pub )
{
    #if 1 && WOOL_ADD_STEALABLE
    if( b_idx >= pub-1 ) {
      victim->pr.more_public_wanted = 1;
      SFENCE;
      throw_both_exceptions( victim );
    }
    #endif
}

static void init_block( Task *base, unsigned long size, unsigned long public_tasks )
{
  unsigned long i;

  // public_tasks are those in the present block and higher, hence nonegative

  for( i=0; i < size; i++ ) {
    #if TWO_FIELD_SYNC
      base[i].f = SFS_EMPTY;
      base[i].balarm = _WOOL_ordered_stores && i < public_tasks ? TF_FREE : TF_OCC;
      base[i].ssn = 0;
    #else
      base[i].f = T_BUSY;
      base[i].balarm = NOT_STOLEN;
    #endif
  }
}


/*
    The canonical representation of the task pool of worker w is
      w->pr.block_base[0..]  // the blocks of the pool
      w->pr.t_idx            // the index of the block containing top
      w->pu.dq_bot           // the number of stolen tasks in the pool
      w->n_public         // the number of public tasks in the pool

    From these values, the following values are computed, to be used primarily
    by the fast_spawn and fast_sync functions:

    The following two constitute the signalling mechanism
      w->high / w->spawn_first_private // points at the last task descriptor in the block, or NULL
                                       // only used when _WOOL_ordered_stores is true
      w->join_first_private

    The following are part of local signalling and of keeping track of public/private
    tasks:
      w->public_size
      w->private_size

*/

static void reset_all_derived( Worker *w, int maybe_skip )
{
  int           idx   = w->pr.t_idx;
  Task         *base  = w->pr.block_base[idx];
  unsigned long bsize = block_size(idx);
  unsigned long pub   = new_local_public_size( idx, w->pr.n_public );
  Task         *jfp   = base + ( pub / sizeof(Task) );
  #if _WOOL_ordered_stores
    Task *sph = base + bsize - 1;
  #else
    unsigned long ps = sizeof(Task) * ( bsize-1 ) - pub;
  #endif

  if( maybe_skip && w->pr.public_size == pub && w->pr.join_first_private == jfp &&
  #if _WOOL_ordered_stores
    w->pr.spawn_high == sph
  #else
    w->pr.spawn_first_private == jfp
  #endif
  ) {
    return;
  }

  w->pr.public_size = pub;
  w->pr.join_first_private = jfp;
  #if _WOOL_ordered_stores
    w->pr.spawn_high = sph;
  #else
    w->pr.private_size = ps;
    w->pr.spawn_first_private = jfp;
  #endif
  MFENCE;
  if( w->pr.more_public_wanted ) {
    throw_both_exceptions( w );
  } else if( w->pr.decrement_deferred ) {
    throw_spawn_exception( w );
  }
}

Task *_WOOL_(slow_spawn)( Worker *self, Task *p, wrapper_t f )
{
  /* This function is called for a spawn in either or both of the following cases
     - a signal has been delivered, either by another worker (eg more public)
       or by this worker (eg bot decrement deferred)
     - the spawn overflows the current block
     The parameter 'p' is the old value of top, that is, it points at the new task
     The parameter 'f' is the header of the task, to be appropriately stored
  */
  Task *next_free = p+1;
  int idx         = self->pr.t_idx;
  long unsigned p_idx = ptr2idx_curr(self,p);

  // fprintf( stderr, "+" );

  PR_INC(self, CTR_slow_spawns);

  #if WOOL_DEFER_BOT_DEC
    if( self->pr.decrement_deferred ) {
      if( SINGLE_FIELD_SYNC || TWO_FIELD_SYNC || !WOOL_STEAL_NOLOCK || self->pu.dq_bot > p_idx ) {
        self->pr.decrement_deferred = 0;
        self->pu.dq_bot = p_idx;
      }
    } else {
      /* PR_INC( self, CTR_sync_no_dec ); */
    }
  #endif

  #if TWO_FIELD_SYNC
    if( _WOOL_ordered_stores ) {
      STORE_WRAPPER_REL(p->f, f);
    } else {
      p->f = f;
      if( p_idx < self->pr.n_public ) {
        #if WOOL_BALARM_CACHING
          STORE_BALARM_T_REL(p->balarm, f);
        #else
          STORE_BALARM_T_REL(p->balarm, TF_FREE);
        #endif
      }
    }
  #else
    STORE_WRAPPER_REL(p->f, f);
  #endif

  // The new task is allocated and ready to steal. Meanwhile, we might need to make additional
  // tasks public and maybe also set up a new block.

  maybe_more_stealable( self, p_idx );

  if( next_free >= self->pr.block_base[idx] + block_size(idx) ) {
    // Make a new block
    unsigned long n_tasks = block_size(++idx);
    unsigned long s_idx = start_idx_of_block( idx );
    unsigned long n_public = self->pr.n_public;

    if( idx == _WOOL_pool_blocks ) {
      fprintf( stderr, "Out of space for task stack\n" );
      exit(1);
    }
    self->pr.t_idx = idx;
    if( self->pr.block_base[idx] == NULL ) {
      // fprintf( stderr, "%d %d\n", self->pr.idx, idx );
      self->pr.block_base[idx] = (Task *) alloc_aligned( n_tasks * sizeof(Task), AA_HERE );
      init_block( self->pr.block_base[idx], n_tasks,
                  s_idx < n_public ? n_public - s_idx : 0 );
      SFENCE;
      self->pu.pu_block_base[idx] = self->pr.block_base[idx];
    }
    next_free = self->pr.block_base[idx];
    // Support fast conversion of pointer to index
    self->pr.curr_block_fidx = start_idx_of_block( idx );
    self->pr.curr_block_base = self->pr.block_base[idx];
  }
  reset_all_derived( self, 1 );

  return next_free;

}

static Task *push_task( Worker *self, Task *p )
{
  int idx = self->pr.t_idx;

  if( p < self->pr.block_base[idx]+block_size(idx)-1 ) {
    self->pr.pr_top = p+1;
    return p+1;
  } else {
    Task *tmp;
    assert( self->pr.block_base[idx+1] != NULL );

    self->pr.t_idx = idx+1;
    tmp = self->pr.block_base[idx+1];
    self->pr.pr_top = tmp;
    self->pr.curr_block_base = tmp;
    self->pr.curr_block_fidx = start_idx_of_block( idx+1 );
    reset_all_derived( self, 1 );
    return self->pr.pr_top;
  }
}

static void pop_task( Worker *self, Task *p )
{
  Task *base = self->pr.curr_block_base;

  if( p > base ) {
     p--;
  } else {
    int idx = self->pr.t_idx - 1;  // Index of the *new* block we're poping into

    assert( idx >= 0 );

    base = self->pr.block_base[idx];
    self->pr.t_idx = idx;
    self->pr.curr_block_base = base;
    self->pr.curr_block_fidx = start_idx_of_block( idx );
    reset_all_derived( self, 1 );

    p = base + block_size(idx); // p is set to the very last element of the new block
  }
  self->pr.pr_top = p;
}

#if THE_SYNC
balarm_t _WOOL_(sync_get_balarm)( Task *t )
{
  Worker   *self;
  balarm_t  a;

  self = get_self( t );
  wool_lock( self->dq_lock );
    a = t->balarm;
  wool_unlock( self->dq_lock );
  PR_INC( self, CTR_sync_lock );
  return a;
}
#endif


static int spin( Worker *self, int n )
{
  int s=0;
#if !WOOL_STEAL_DKS && !WOOL_STEAL_SET
  int i;

  // This code should hopefully confuse every optimizer.
  for( i=1; i<=n; i++ ) {
    s |= i;
  }
  if( s > 0 ) {
    PR_ADD( self, CTR_spins, n );
  }
#endif
  return s&1;
}

static int trans_leap( Worker *self, wrapper_t card, volatile Task *orig, unsigned tb )
{

  volatile Task *prev = orig;
  long int thief_idx = INFO_GET_THIEF( tb );
  unsigned long thief_base = INFO_GET_BASE( tb );
  Worker *thief = workers[thief_idx];
  int sp = 0, i, n = n_workers;
  struct {
    wrapper_t       tb;
    unsigned long   ssn;
    volatile Task  *prev;
  } stack[n];
  unsigned long ssn = prev->ssn, orig_ssn = ssn;
  char seen[n];

  for( i=0; i<n; i++ ) {
    seen[i] = 0;
  }
  seen[self->pr.idx] = 1;

  do {
    int do_pop = 0;

    PR_INC( self, CTR_trlf_iters );

    // When we get here, we have already attempted a steal from the worker owning this
    // pool.

    if( thief_base >= thief->pu.dq_bot ) {
      // We have looked at all eligible tasks in this worker's task pool, so pop
      // back into previous worker

      do_pop = 1;

    } else {

      // We're going to look at a task in the pool of the current thief

      volatile Task      *t = idx_to_task_p_pu( thief, thief_base, thief->pu.pu_block_base[0] );
      wrapper_t           r = t->f;
      unsigned long new_ssn = t->ssn;

      // The following is subtle and relies on the reads of 'new_ssn' and 'r' to
      // create control dependences between the loads of 't->f' and 't->ssn'
      // and the subsequent load of 'prev->ssn'
      // which guarantees that the current thief is still eligible, at least
      // for some machines; see the JSR-133 Cookbook

      // The 'prev->ssn' read below must occur after the 't->f' and 't->ssn' reads above

      if( SFS_IS_STOLEN( r ) && new_ssn != 0 && prev->ssn == ssn ) {
        // We've found a candidate for transitive leap frogging

        unsigned long new_p = SFS_GET_THIEF( r );
        unsigned long new_t = INFO_GET_THIEF( new_p );

        if( seen[new_t] /* thief_seen( seen, new_t ) */ ) {
          thief_base++;
        } else {
          // This is a thief we have not yet seen; first try to steal from it

          int steal_outcome = steal_one( self, workers[new_t], card, 0, t, new_ssn );

          if( steal_outcome == SO_STOLE ) {
            // We stole, so we should restart with an attempt at nontransitive leap frogging
            return SO_STOLE;
          }
        #if WOOL_TRLF_ORIG_OFTEN
          else if( steal_one( self, workers[INFO_GET_THIEF( tb )], card, 0, orig, orig_ssn )
                    == SO_STOLE ){
            // This does not count as successful transitive leaping
            return SO_CL;
          }
        #endif
          else {
            // We did not steal, so we should push into this worker's pool instead.
            // But only if we do not have this worker on the stack already.
            // First mark it as seen
            // mark_thief_as_seen( seen, new_t );
            seen[new_t] = 1;

            // Then push the old context
            stack[sp].tb   = MAKE_THIEF_INFO( thief_idx, thief_base+1 );
            stack[sp].ssn  = ssn;
            stack[sp].prev = prev;
            sp++;
            thief_idx = new_t;
            thief_base = INFO_GET_BASE( new_p );
            ssn = new_ssn;
            prev = t;
            thief = workers[thief_idx];

          }
        }
      } else if( orig->f == SFS_DONE ) {
        return SO_NO_WORK;
      } else {
        // This task no longer contains an eligible thief, either because it
        // is no longer under execution or because the pool it is contained in
        // is no longer eligible. In the latter case we should pop into the previous
        // worker.

        if( SFS_IS_STOLEN( r ) ) {
          // The current thief is no longer eligible
          do_pop = 1;
        } else {
          thief_base++;
        }
      }
    }

    if( do_pop ) {

      sp--;
      if( sp < 0 ) return SO_NO_WORK;

      #if WOOL_TRLF_RESET_SEEN
        seen[ thief_idx ] = 0;
      #endif

      thief_idx  = INFO_GET_THIEF( stack[sp].tb );
      thief_base = INFO_GET_BASE( stack[sp].tb );
      ssn        = stack[sp].ssn;
      prev       = stack[sp].prev;
      thief      = workers[thief_idx];
    }

  } while( 1 );
}

// If the third argument is empty or equivalent, we always reread from the task
// until we read a valid wrapper or a valid thief index from balarm

#if COUNT_EVENTS

static void record_leap_fails( Worker *self, unsigned n )
{
  #if !WOOL_PIE_TIMES
  unsigned limits[] = {100, 320, 1000, 3200, 10000, 32000, 100000, 1000000000};
  int i;

  for( i=0; i<8; i++ ) {
    if( n < limits[i] ) {
      PR_ADD( self, CTR_lf_0 + i, n );
      break;
    }
  }
  #endif
}

#endif

static inline TILE_INLINE
void _WOOL_(rts_sync)( Worker *self, volatile Task *t, grab_res_t r )
{
  int a;
#if TWO_FIELD_SYNC
  wrapper_t f;
  balarm_t b;
#endif
  long unsigned t_idx;

#if ! WOOL_SYNC_NOLOCK
  wool_lock( self->dq_lock );
#endif

    #if TWO_FIELD_SYNC
      b = r;
      f = t->f;
      while( SFS_IS_TASK( f ) && b == TF_OCC ) {
        do {
          f = t->f;
          b = t->balarm;
        } while( SFS_IS_TASK( f ) && b == TF_OCC );
        if( b != TF_OCC ) {
          b = _WOOL_(exch_busy_balarm)( &(t->balarm) );
        }
      }
      if( _WOOL_ordered_stores ) {
        if( SFS_IS_TASK( f ) ) {
          t->f = SFS_EMPTY;
        }
        STORE_BALARM_T_REL( t->balarm, TF_FREE );
      }
      if( SFS_IS_TASK( f ) ) {
        // It was never stolen or thief backed out
        f( self, (Task *) t );
        a = INLINED;
      } else if( f == SFS_DONE ) {
        a = STOLEN_DONE;
      } else {
        // It is stolen
        a = SFS_GET_THIEF( f );
      }
    #else
      a = r;
    #endif

#if ! THE_SYNC && ! SINGLE_FIELD_SYNC && ! TWO_FIELD_SYNC
    // Thief might not yet have written
    while( a == NOT_STOLEN || a == STOLEN_BUSY ) a = t->balarm;
#endif

    if( a == STOLEN_DONE ||
        ( !SINGLE_FIELD_SYNC && !TWO_FIELD_SYNC && t->balarm == (balarm_t) STOLEN_DONE ) ) {

      /* Stolen and completed */
      PR_INC( self, CTR_read );

    } else if( a == INLINED ) {

      /* A late inline */
      PR_INC( self, CTR_inlined );

    } else if( a > B_LAST ) {

      /* Stolen and in progress; let's leapfrog! */
      int thief_idx = INFO_GET_THIEF(a);
      int done=0;
      long nfail = 0;
      Worker *thief = workers[thief_idx];
      Task *tp1 = push_task( self, (Task *) t );
      int trlf_threshold = self->pr.trlf_threshold;
      int trlf_timer = trlf_threshold;
      int self_idx = self->pr.idx;
      wrapper_t card = SFS_STOLEN( MAKE_THIEF_INFO( self_idx, ptr2idx_curr( self, tp1 ) ) );

      assert( thief_idx <= n_workers );

#if ! WOOL_SYNC_NOLOCK
      wool_unlock( self->dq_lock );
#endif
      PR_INC( self, CTR_waits ); // It isn't waiting any more, though ...

      // self->unstolen_stealable = unstolen_per_decrement;

      /* Now leapfrog */

      logEvent( self, 3 );
      // logEvent( self, thief_idx+20 );
      time_event( self, 3 );
      #if 0 // WOOL_STEAL_SET /* || WOOL_STEAL_DKS */
        self->pu.is_thief = 1;
      #endif

      do {
        int steal_outcome = SO_NO_WORK;

        if( !WOOL_FIXED_STEAL && switch_interval > 0 ) {
          steal_outcome = steal_one( self, thief, card, 0, t, t->ssn );
        }
        if( WOOL_TRLF && trlf_timer-- == 0 && steal_outcome != SO_STOLE ) {
          PR_INC( self, CTR_trlf );
          steal_outcome = trans_leap( self, card, t, a );
          if( steal_outcome == SO_STOLE ) {
            trlf_threshold /= 3;
          } else if( trlf_threshold < 300 ) {
            trlf_threshold = trlf_threshold*3 + 1;
            trlf_timer = trlf_threshold;
          }
          self->pr.trlf_threshold = trlf_threshold;
          if( steal_outcome == SO_CL ) {
            steal_outcome = SO_STOLE;
          }
        }
        if( steal_outcome == SO_STOLE ) {
          #if COUNT_EVENTS
            record_leap_fails( self, nfail );
          #endif
          nfail=0;
          trlf_timer = trlf_threshold;
        } else {
          nfail++;
        }
#if SINGLE_FIELD_SYNC || TWO_FIELD_SYNC
        if( READ_WRAPPER_ACQ( t->f ) == SFS_DONE ) {
          done = 1;
        }
#else
        if( READ_BALARM_T_ACQ( t->balarm ) == STOLEN_DONE ) { // Leapfrogging is over!
          done = 1;
        }
#endif
      } while( !done );
      COMPILER_FENCE;
      #if COUNT_EVENTS
        record_leap_fails( self, nfail );
      #endif

      pop_task( self, (Task *) tp1 );

      time_event( self, 4 );
      logEvent( self, 4 );
#if ! WOOL_SYNC_NOLOCK && ! WOOL_STEAL_NOLOCK
      wool_lock( self->dq_lock );
#endif

    } else {
      fprintf( stderr, "Unknown task state %lu in sync\n", (unsigned long) a );
      exit( 1 );
    }

#if WOOL_SYNC_NOLOCK && ! WOOL_STEAL_NOLOCK
    wool_lock( self->dq_lock );
#endif

    if( a != INLINED ) {
      assert( self->pr.curr_block_base <= t );
      t_idx = ptr2idx_curr( self, (Task *) t );
      #if ! WOOL_DEFER_BOT_DEC
        if( !WOOL_FIXED_STEAL && ( ! WOOL_STEAL_OO || self->pu.dq_bot > t_idx )  ) {
          if( WOOL_ADD_STEALABLE && self->pr.highest_bot < t_idx+1 ) {
            self->pr.highest_bot = t_idx+1;
          }
          self->pu.dq_bot = t_idx;
        } else {
          PR_INC( self, CTR_sync_no_dec );
        }
      #else
        if( WOOL_DEFER_BOT_DEC_OPT &&
            t == self->pr.block_base[0] &&
            ( ! WOOL_STEAL_OO || self->pu.dq_bot > t_idx ) )
        {
          if( WOOL_ADD_STEALABLE && self->pr.highest_bot < t_idx+1 ) {
            self->pr.highest_bot = t_idx+1;
          }
          self->pu.dq_bot = t_idx;
          self->pr.decrement_deferred = 0;
        } else if( !self->pr.decrement_deferred ) {
          if( WOOL_ADD_STEALABLE && self->pr.highest_bot < t_idx+1 ) {
            self->pr.highest_bot = t_idx+1;
          }
          self->pr.decrement_deferred = 1;
          if( ! WOOL_INLINED_BOT_DEC ) {
            throw_spawn_exception( self);
          }
        }
      #endif
    }

#if !WOOL_DEFER_NOT_STOLEN && !SINGLE_FIELD_SYNC
    t->balarm = NOT_STOLEN;
#endif

#if ! WOOL_STEAL_NOLOCK
    wool_unlock( self->dq_lock );
#endif

}

Task *_WOOL_(slow_sync)( Worker *self, Task *p, grab_res_t grab_res )
{
  // We get here for any of these reasons
  // - We're popping out of our current block
  // - Someone has tripped the wire
  // - We've been doing many joins with unstolen public tasks

  // fprintf( stderr, "." );

  unsigned long p_idx = ptr2idx_curr( self, p );

  PR_INC(self, CTR_slow_syncs);

#if TWO_FIELD_SYNC && WOOL_FAST_EXC
  if( __builtin_expect( grab_res == TF_EXC, 0 ) ) {
    grab_res = TF_OCC;
#endif

#if WOOL_ADD_STEALABLE
  maybe_more_stealable( self, p_idx - 1 );
  maybe_less_stealable( self, p );
#endif

  if( __builtin_expect( self->pr.curr_block_base < p, 1 ) ) {
    // No pop out of block
    p--;
  } else {
    // Now we pop back into a lower block
    self->pr.t_idx --;
    // Support fast conversion of pointer to index
    self->pr.curr_block_fidx = start_idx_of_block( self->pr.t_idx );
    self->pr.curr_block_base = self->pr.block_base[self->pr.t_idx];

    // set p to point at the last task descriptor in that block
    p = self->pr.block_base[self->pr.t_idx] + block_size(self->pr.t_idx) - 1;
  }
  // now recompute sizes etc, resetting both exceptions
  reset_all_derived( self, 1 );

#if TWO_FIELD_SYNC && WOOL_FAST_EXC
  } else {
    assert( p > self->pr.curr_block_base );
    p--;
  }
#endif

  self->pr.pr_top = p;
  // if p points at a public task, we join with it, otherwise inline it
  if( p_idx-1 < self->pr.n_public ) {
    _WOOL_(rts_sync)( self, p, grab_res );
  } else {
    wrapper_t f = p->f;
    assert( !GRAB_RES_IS_TASK( grab_res ) );
    assert( SFS_IS_TASK( f ) );
    assert( p->balarm == TF_OCC );
    PR_INC( self, CTR_inlined );
    // p->f = SFS_EMPTY; /* Temporary */
    GET_TASK(f)( self, p );
  }
  assert( self->pr.pr_top == p );

  return p;
}

struct _WorkerData {
  Worker w;
  Task   p[];
};

static int worker_offset = LINE_SIZE;

static void init_worker( int w_idx )
{
  int i;
  Worker *w;
  struct _WorkerData *d;
  int offset = w_idx * worker_offset;
  int size = first_block_size * sizeof(Task);

  // We're offsetting the worker data a bit to avoid cache conflicts
  d = (struct _WorkerData *)
      ( ( (char *) alloc_aligned( sizeof(Worker) + size + offset, AA_HERE ) ) + offset );
  w = &(d->w);

  w->pr.dq_base = &(d->p[0]);
  bases[w_idx] = w->pr.dq_base;
  w->pu.flag = w_idx == 0 ? 1 : 0;
  w->pu.is_running = THREAD_GARAGE ? 1 : 0;
  w->pr.thread_leader = -1;
  w->pr.more_work = 2;
  assert( n_stealable >= 0 );
  init_block( w->pr.dq_base, first_block_size, (unsigned long) n_stealable );
  w->pr.block_base[0] = w->pr.dq_base;
  w->pu.pu_block_base[0] = w->pr.dq_base;
  for( i = 1; i < _WOOL_pool_blocks; i++ ) {
    w->pr.block_base[i] = NULL;
  }
  w->pr.t_idx = 0;

  w->pr.curr_block_fidx = 0;
  w->pr.curr_block_base = w->pr.dq_base;

  w->pu.dq_bot = 0;
  w->pu.ssn = 1;
  w->pr.n_public = n_stealable;
  w->pu.pu_n_public = n_stealable;
  w->pr.storage = NULL;
  w->pr.highest_bot = 0;
  w->pu.dq_lock = &( w->pu.the_lock );
  pthread_mutex_init( w->pu.dq_lock, NULL );
  pthread_mutex_init( &( w->pu.work_lock ), NULL );
  pthread_cond_init( &( w->pu.work_available ), NULL );
  w->pu.fun = NULL;
  w->pu.fun_arg = NULL;
  for( i=0; i < CTR_MAX; i++ ) {
    w->pr.ctr[i] = 0;
  }
  w->pr.idx = w_idx;
  w->pr.clock = 0;
#if WOOL_PIE_TIMES
  w->pr.time = gethrtime();
#else
  w->pr.time = 0;
#endif
  w->pr.decrement_deferred = 0;
  w->pr.more_public_wanted = 0;
  w->pr.unstolen_stealable = unstolen_per_decrement;
  w->pr.pr_top = w->pr.block_base[0];
  w->pr.trlf_threshold = global_trlf_threshold;
  #if LOG_EVENTS
    logbuff[w->pr.idx] = malloc( 6000000 * sizeof( LogEntry ) );
    w->pr.logptr = logbuff[w->pr.idx];
  #endif
  w->pu.is_thief = 0;
  w->pr.wait_for = NULL;

  reset_all_derived( w, 0 );

#if THREAD_GARAGE
  pthread_mutex_init( &(garage[w_idx].lck), NULL );
  pthread_cond_init( &(garage[w_idx].cnd), NULL );
#endif

  workers[w_idx] = w;
}

// CPU affinity stuff

#ifdef __APPLE__
#define set_worker_affinity(x) /* Nothing */
#else

static int affinity_mode = 0;

static int chip_major[] = {0, 2, 4, 6, 1, 3, 5, 7};
static int chip_minor[] = {0, 1, 2, 3, 4, 5, 6, 7};

static void set_worker_affinity( int w_idx )
{
  int desired_core = -1;
  int thread_idx = w_idx / workers_per_thread;

  switch( affinity_mode ) {
    case 0 : break;
    case 1 : /* Pack chip first */
             desired_core = chip_major[ thread_idx ];
             break;
    case 2 : /* Spread out among chips */
             desired_core = chip_minor[ thread_idx ];
             break;
    case 3 : /* Individual choices */
             desired_core = affinity_table[ thread_idx ] - 1;
             break;
    case 4 : /* Worker no to cpu no */
             desired_core = thread_idx;
             break;
  }
  if( desired_core != -1 ) {
    const int size = sizeof(cpu_set_t);
    cpu_set_t set;

    CPU_ZERO( &set );
    CPU_SET( desired_core, &set );
    if( desired_core >= 0 ) sched_setaffinity( 0, size, &set );
  }

}
#endif

static void init_workers( int w_idx, int n )
{
  int i;

  // Maybe set processor affinity
  set_worker_affinity( w_idx );

  for( i = w_idx; i < w_idx+n; i++ ) {
    init_worker( i );
  }
  _WOOL_(setspecific)( &tls_self, workers[w_idx] );

  for( i = w_idx+1; i < w_idx+n; i++ ) {
   #if THREAD_GARAGE
    pthread_create( ts+i-1, &worker_attr, (void *(*)(void *)) look_for_work, workers[i] );
    _WOOL_(setspecific)( &tls_self, workers[i] );
   #endif
  }

}

#if WOOL_FIXED_STEAL
static int bits( int i )
{
  int k = 0;

  while( i >= (1<<k) ) k++;
  return k;
}
#endif

#if WOOL_SLOW_STEAL
int global_steal_delay = 1000;
#endif

/*
   Main steal variants to implement:
   - TWO_FIELD_SYNC (no worker lock, peek/no peek) Because it is fast and works on Tilera
   - THE_SYNC       (worker lock,    peek/no peek) Compare with Cilk
   - LOCK_FREE      (no worker lock, peek)         Later

*/

static const int parsamp_size = WOOL_STEAL_PARSAMP ? 1 : 0;

#if WOOL_FAST_TIME
  #define FAST_TIME(t) ((t) = (unsigned) __insn_mfspr( 0x4e07 ))
#else
  #define FAST_TIME(t) /* Empty */
#endif

static int
steal( Worker *self, Worker **victim_p, wrapper_t card, int flags, volatile Task *jt, unsigned long ssn )
{
  volatile Task   *tp;
  wrapper_t        f = T_BUSY;
#if TWO_FIELD_SYNC
  balarm_t         alarm;
#endif
  Worker          *victim;
  int              is_old_thief = flags & ST_OLD;
  long unsigned    bot_idx;
#if WOOL_TRLF
  long unsigned    booty_ssn = 0;
#endif
  long unsigned    tmp_ssn;
  int              is_thief;

#if WOOL_FAST_TIME
  unsigned         t_start, t_vread, t_bread, t_peek, t_pre_x, t_post_x,
                   t_post_t, t_post_c, t_pre_rs, t_post_rs, t_pre_e;
#endif

#if WOOL_STEAL_PARSAMP
  Worker          *victim0,   *victim1;
  int              bot_idx0,   bot_idx1;
  int              is_thief0,  is_thief1;
  Task            *base0,     *base1;
  int              flag0,      flag1;
  volatile Task   *tp0,       *tp1;
  balarm_t         balarm0,    balarm1;
  int              depth0,     depth1;
  unsigned long    booty0,     booty1;

  const int        marianer = 1000000000;
#endif

#if WOOL_SLOW_STEAL
  volatile int v;
#endif

#if WOOL_FIXED_STEAL
  int self_idx = self->pr.idx;
  int b = bits( self_idx );

  int victim_idx = self_idx - (1 << (b-1));
  int idx = b - bits( victim_idx ) - 1;
  victim = workers[victim_idx];
#else
  volatile Task   *base;
#endif

    //  po0-kyrurew79,kiyyyyyu8767uuhk,.ttttttttt

  FAST_TIME(t_start);
  logEvent( self, 100 + (*victim_p)->pr.idx );

#if WOOL_STEAL_PARSAMP && TWO_FIELD_SYNC
 // if(jt==NULL) {

  victim0 = victim_p[0];
  victim1 = victim_p[1];

  bot_idx0  = victim0->pu.dq_bot;
  flag0     = victim0->pu.flag;               // The depth of the task at the base of the stack
  is_thief0 = victim0->pu.is_thief;
  base0     = victim0->pu.pu_block_base[0];
  bot_idx1  = victim1->pu.dq_bot;
  flag1     = victim1->pu.flag;               // The depth of the task at the base of the stack
  is_thief1 = victim1->pu.is_thief;
  base1     = victim1->pu.pu_block_base[0];

  tp0 = idx_to_task_p_pu( victim0, bot_idx0, base0 );
  tp1 = idx_to_task_p_pu( victim1, bot_idx1, base1 );

  if( tp0 != NULL ) {
    balarm0 = tp0->balarm;
    #if WOOL_TRLF
      booty0 =tp0->ssn;
    #endif
  }

  if( tp1 != NULL ) {
    balarm1 = tp1->balarm;
    #if WOOL_TRLF
      booty1 =tp1->ssn;
    #endif
  }

  if( !is_thief0 && balarm0 != TF_OCC ) {
    depth0 = bot_idx0 + flag0;
  } else {
    depth0 = marianer;
  }

  if( !is_thief1 && balarm1 != TF_OCC ) {
    depth1 = bot_idx1 + flag1;
  } else {
    depth1 = marianer;
  }

  if( depth1 < depth0 ) {
  // if( depth1 > depth0 && depth1 < marianer ) {
  // if( depth0 == marianer ) {
    victim = victim1;
    bot_idx = bot_idx1;
    tp = tp1;
    base = base1;
    #if WOOL_TRLF
      booty_ssn = booty1;
    #endif
  } else if( depth0 < marianer ) {
    victim = victim0;
    bot_idx = bot_idx0;
    tp = tp0;
    base = base0;
    #if WOOL_TRLF
      booty_ssn = booty0;
    #endif
  } else {
    time_event( self, 7 );
    return SO_FAIL( is_thief0 + is_thief1 );
  }
 // } else {
 //  #endif
  #else // WOOL_STEAL_PARSAMP

  victim = *victim_p;

  FAST_TIME(t_vread);

  is_thief = victim->pu.is_thief;
#if WOOL_FIXED_STEAL
  tp = victim->pr.dq_base + idx; // idx must be less than size of first block!
#else
  bot_idx = victim->pu.dq_bot;
  base    = victim->pu.pu_block_base[0];
  if( bot_idx < first_block_size ) {
    tp = base + bot_idx;
  } else {
    tp = idx_to_task_p_pu( victim, bot_idx, (Task *) base );
    if( tp==NULL ) {
      time_event( self, 7 );
      return SO_NO_WORK;
    }
  }
  FAST_TIME(t_bread);
#endif

#if WOOL_STEAL_SET || WOOL_STEAL_DKS
  if( is_thief ) {
    time_event( self, 7 );
    return SO_THIEF;
  }
#endif

#if STEAL_PEEK || SINGLE_FIELD_SYNC || TWO_FIELD_SYNC


#if TWO_FIELD_SYNC
  #if WOOL_TRLF
    booty_ssn = tp->ssn;
  #endif
  if( tp->balarm == TF_OCC || ( _WOOL_ordered_stores && ! SFS_IS_TASK(tp->f) ) ) {
    time_event( self, 7 );
    return SO_NO_WORK;
  }
  FAST_TIME(t_peek);
#else
  // This version assumes that we use locks
  if( tp->balarm != NOT_STOLEN || tp->f <= T_LAST || !( bot_idx < victim->pu.pu_n_public ) ) {
    time_event( self, 7 );
    return SO_NO_WORK;
  }
#endif

#else
  // neither peek nor sfs or tfs
  if( ! ( bot_idx < victim->pu.pu_n_public ) ) {
    time_event( self, 7 );
    return SO_NO_WORK;
  }

#endif


#if !WOOL_STEAL_NOLOCK
  PREFETCH( tp->f ); // Start getting exclusive access

#if STEAL_TRYLOCK
  if( wool_trylock( victim->dq_lock ) != 0 ) {
    time_event( self, 7 );
    return SO_BUSY;
  }
#else
  wool_lock( victim->dq_lock );
#endif
  // now locked!
  // but only if we use locks!

    // Yes, we need to reread after aquiring lock!
    bot_idx = victim->pu.dq_bot;
    tp = idx_to_task_p_pu( victim, bot_idx, base );
    if( tp == NULL ) {
      wool_unlock( victim->dq_lock );
      return SO_NO_WORK;
    }

#endif
// #if WOOL_STEAL_PARSAMP
// }
#endif // WOOL_STEAL_PARSAMP

    // __builtin_prefetch( (void *) tp, 1 );
    __builtin_prefetch( (void *) self, 1 );

    // The victim might have sync'ed or somebody else might have stolen
    // while we were obtaining the lock;
    // no point in getting exclusive access in that case.
    if( WOOL_STEAL_NOLOCK ||
        ( bot_idx < victim->pu.pu_n_public && tp->balarm == (balarm_t) NOT_STOLEN
                        && tp->f > (wrapper_t) T_LAST  ) ) {
#if THE_SYNC
      // THE version, uses locks between thieves (ie !WOOL_STEAL_NOLOCK)
      tp->balarm = self_idx;
      MFENCE;
      f = tp->f;

      if( f > T_LAST ) {  // Check again after the fence!
        victim->pu.dq_bot = bot_idx+1;
      } else {
        tp->balarm = NOT_STOLEN;
        tp = NULL;
      }
#elif TWO_FIELD_SYNC
      FAST_TIME(t_pre_x);
      alarm = _WOOL_(exch_busy_balarm)( &(tp->balarm) );
      if( __builtin_expect( alarm != TF_OCC, 1 ) ) {
        FAST_TIME(t_post_x);
        #if WOOL_BALARM_CACHING
          if( __builtin_expect( alarm == TF_FREE, 0 ) ) {
            PR_INC( self, CTR_wrapper_reread );
            f = tp->f;
          } else {
            f = alarm;
          }
        #else
          f = tp->f;
        #endif
        #if WOOL_STEAL_REPF
          __builtin_prefetch( tp );
        #endif
        if( ( !_WOOL_ordered_stores || SFS_IS_TASK( f ))
             && __builtin_expect( victim->pu.dq_bot == bot_idx, 1 )
             && (__builtin_expect( jt == NULL, 1 )
                 || ( tmp_ssn = jt->ssn, __builtin_expect( jt->f != SFS_DONE, 1 )
                   && __builtin_expect( tmp_ssn == ssn, 1 ) ) ) ) {
          // _wool_unbundled_mf(); // If the exchange does not have mf semantics, we do an mf
          FAST_TIME(t_post_t);
          victim->pu.dq_bot = bot_idx+1;
          tp->f = card;
          #if WOOL_TRLF
            tp->ssn = booty_ssn+1;
          #endif
          FAST_TIME(t_post_c);
        } else {
          tp->balarm = WOOL_BALARM_CACHING ? alarm : TF_FREE;
          tp = NULL;
          PR_INC( self, CTR_steal_no_inc ); // does the job of counting back outs
        }
      } else {
        // This also covers the case where *tp is no longer public
        tp = NULL;
        f = NULL;
      }
#else
      // Exchange version, can result in out of order steals

      int skips = 0;
      unsigned long e_idx;

      do {
        f = _wool_exch_busy( &(tp->f) );
        COMPILER_FENCE;

        if( f > T_LAST && bot_idx < victim->pu.pu_n_public ) {
          int all_stolen = 1;
          volatile Task *ep;

          _wool_unbundled_mf(); // If the exchange does not have mf semantics, we do an mf

          tp->balarm = self_idx;

#if !WOOL_FIXED_STEAL
          for( e_idx = victim->pu.dq_bot; e_idx < bot_idx; e_idx++ ) {
            ep = idx_to_task_p_pu( victim, e_idx, base );
            if( ep->f > T_LAST ) {
              all_stolen = 0;
            }
          }
          if( all_stolen ) {
            victim->pu.dq_bot = bot_idx+1;
          } else {
            PR_INC( self, CTR_steal_no_inc );
          }
          if( skips != 0 ) {
            PR_INC( self, CTR_skip );
          }
#endif
        } else {
          tp = NULL;
        }
      } while( 0 );

#endif

    } else {
      tp = NULL; // Already stolen by someone else
    }
#if !WOOL_STEAL_NOLOCK
  wool_unlock( victim->dq_lock );
#endif

  if( tp != NULL ) {

    // fprintf( stderr, "S %d %d %lu\n", self_idx, victim_idx, tp - victim->pr.block_base[0] );

    FAST_TIME(t_pre_rs);
    maybe_request_stealable( victim, bot_idx, victim->pu.pu_n_public );

    // One less old thief?
    if( is_old_thief ) {
      decrement_old_thieves( );
    }

    FAST_TIME(t_post_rs);
    #if ( WOOL_STEAL_SET || WOOL_STEAL_DKS )
      if( flags & ST_THIEF ) {
        self->pu.is_thief = 0;
      }
    #endif

    #if WOOL_STEAL_SAMPLE
      if( flags & ST_THIEF ) {
        self->pu.flag = victim->pu.flag + bot_idx + 1;
      }
    #endif

    #if WOOL_SLOW_STEAL
      v = spin( self, global_steal_delay );
    #endif

    time_event( self, 1 );
    logEvent( self, 1 );

    FAST_TIME(t_pre_e);

    #if WOOL_FAST_TIME

    if( jt==NULL ) {
      PR_ADD( self, CTR_t_vread,   t_vread   - t_start   );
      PR_ADD( self, CTR_t_bread,   t_bread   - t_vread   );
      PR_ADD( self, CTR_t_peek,    t_peek    - t_bread   );
      PR_ADD( self, CTR_t_pre_x,   t_pre_x   - t_peek    );
      PR_ADD( self, CTR_t_post_x,  t_post_x  - t_pre_x   );
      PR_ADD( self, CTR_t_post_t,  t_post_t  - t_post_x  );
      PR_ADD( self, CTR_t_post_c,  t_post_c  - t_post_t  );
      PR_ADD( self, CTR_t_pre_rs,  t_pre_rs  - t_post_c  );
      PR_ADD( self, CTR_t_post_rs, t_post_rs - t_pre_rs  );
      PR_ADD( self, CTR_t_pre_e,   t_pre_e   - t_post_rs );
    }

    #endif

    f( self, (Task *) tp );

    logEvent( self, 2 );
    time_event( self, 2 );

    #if WOOL_STEAL_SET || WOOL_STEAL_DKS
      // self->pu.is_thief = 1; // Only if we're called from look_for_work
    #endif

      // instead of locking, so that the return value is really updated
      // the return value is already written by the wrapper (f)

      COMPILER_FENCE;
      #if SINGLE_FIELD_SYNC || TWO_FIELD_SYNC
        STORE_WRAPPER_REL( tp->f, /* (volatile wrapper_t) */ SFS_DONE );
      #else
        STORE_BALARM_T_REL( tp->balarm, /* (volatile balarm_t) */ STOLEN_DONE );
      #endif

    time_event( self, 8 );
    return SO_STOLE;
  }
  time_event( self, 7 );
  return SO_BUSY;
}

static int steal_one( Worker *self, Worker * victim, wrapper_t card, int flags, volatile Task *jt, unsigned long ssn )
{
  Worker* v[] = {victim, victim};
  int steal_outcome = steal( self, v, card, flags, jt, ssn );

  #if COUNT_EVENTS
    PR_INC( self, CTR_leap_tries );
    if( steal_outcome == SO_STOLE ) {
      PR_INC( self, CTR_leaps );
    } else if( steal_outcome == SO_BUSY ) {
      PR_INC( self, CTR_leap_locks );
    }
  #endif

  return steal_outcome;
}

static int myrand( unsigned int *seedp, int max )
{
  return rand_r( seedp ) % max;
}

static int rand_interval = 40; // By default, scan sequentially for 0..39 attempts

#define to_widx(n,i) ( (n)>(i) ? (i) : (n) > (i)-(n) ? (i)-(n) : (i)%(n) )
// #define to_widx(n,i) ( (i)%(n) )

#define BITS_PER_LONG  ( sizeof(long)*8 )
#define WORD(m,i) ( m[(i)/BITS_PER_LONG] )
#define BIT(i) (1<<((i)%BITS_PER_LONG))

static inline int record_steal( Worker *self, int n, int attempts, int steal_outcome )
{

  if( steal_outcome == SO_BUSY ) {
    PR_INC( self, CTR_steal_locks );
  }
  if( steal_outcome == SO_STOLE ) {
    PR_INC( self, CTR_steals );

    // Ok, this is clunky, but the idea is to record if we had to try many times
    // before we managed to steal.

    #if !WOOL_PIE_TIMES
    if( attempts == 1 ) {
      PR_INC( self, CTR_steal_1s );
      PR_ADD( self, CTR_steal_1t, attempts );
    } else if( attempts < n ) {
      PR_INC( self, CTR_steal_ps );
      PR_ADD( self, CTR_steal_pt, attempts );
    } else if( attempts < 3*n ) {
      PR_INC( self, CTR_steal_hs );
      PR_ADD( self, CTR_steal_ht, attempts );
    } else {
      PR_INC( self, CTR_steal_ms );
      PR_ADD( self, CTR_steal_mt, attempts );
    }
    #endif
    return 0;
  } else {
    return attempts;
  }
}

static int global_poll_size = 0;

static inline TILE_INLINE int task_appears_stealable( Task *p )
{
  #if TWO_FIELD_SYNC
    return p->balarm != TF_OCC && ( !_WOOL_ordered_stores || SFS_IS_TASK( p->f ) );
  #elif THE_SYNC
    return p->stealable && p->balarm == NOT_STOLEN && p->f > T_LAST;
  #else
    return p->stealable && p->f > T_LAST;
  #endif
}

static int poll( Worker *w )
{
  long unsigned  bot      = w->pu.dq_bot;
  Task          *base     = w->pu.pu_block_base[0];
  int            depth    = w->pu.flag;
  Task          *p;

  #if WOOL_STEAL_SET
    if( w->pu.is_thief ) {
      return -2;
    }
    #if WOOL_FAST_SAMPLING
      return depth + bot;
    #endif
  #endif

  p = idx_to_task_p_pu( w, bot, base );
  if( task_appears_stealable( p ) ) {
    return depth + bot;
  } else {
    return -1;
  }
}

#if WOOL_STEAL_NEW_SET
static int global_max_thieves = 4,
           global_min_set_size = 12;
#endif

static int global_max_fail_while_sampling =
              WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET ? 1 : 0;
static int global_max_fail_while_searching =
              WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET ? 1 : 0;

// There are three phases in look_for_work regardless of configuration:
//
//   1) Prepare for stealing.
//   2) Steal.
//   3) Fiddle around with counters after stealing.
//
//   The set-based stealing does a lot of preparation, potentially steals,
//   and then fiddles around with counters. The non-set-based stealing
//   has no preparation and just steals.

// Takes an optional int * of workers to steal from. Steals from everywhere
// if the argument is NULL.
static void *look_for_work( void *arg )
{
  Worker *self = _WOOL_(slow_get_self)();
  int self_idx = self->pr.idx;
  int n = n_workers;
  int more;
  int attempts = 0;
  int is_old_thief = 0;
  int v_pos = 0;
  // Default search order should be different for different workers.
  // parsamp_size is 0 if PARSAMP_STEALING disabled.
  Worker* scramble[n-1+parsamp_size];
  int j;
  wrapper_t card = SFS_STOLEN( MAKE_THIEF_INFO( self_idx, 0L ) );
#if WOOL_STEAL_NEW_SET || (!WOOL_FIXED_STEAL && !WOOL_STEAL_NEW_SET)
  unsigned int seed = self_idx;
  int i = 0;
#endif
  // State related to non-sampling version.
  int local_sleep_interval = sleep_interval;
  int  next_yield = yield_interval, next_sleep = local_sleep_interval;
  int victim_idx = WOOL_STEAL_NEW_SET ? self_idx : 0;
  int next_spin = n-1;
  volatile int v = 0; // To ensure that a delay loop is executed
#if WOOL_STEAL_NEW_SET
  int since_rand = 0;
  const int max_rand_interval = 1000;
  // State related to set stealing
  int n_seen = 0,
      n_thieves = 0,
      first_victim = 0;
  int min_set_size = global_min_set_size,
      max_thieves = global_max_thieves;
#endif

  // State related to sampling
  int max_fail_while_sampling  = global_max_fail_while_sampling,
      max_fail_while_searching = global_max_fail_while_searching;
  const int v_depth_default = 1000000000;
  int poll_size = global_poll_size;
  int polling = max_fail_while_searching;
  int v_depth = v_depth_default;

  if( 0 && self_idx % 4 == 1 ) {
    polling = max_fail_while_searching = 1000;
  }

  for( j=0; j<n-1; j++ ) {
    scramble[j] = j < self_idx ? workers[j] : workers[j+1];
  }
#if WOOL_STEAL_NEW_SET
  // Use the scramble array in every code path. Only scramble the
  // array if set based stealing is enabled though.
  for( j=0; j<n-1; j++ ) {
    Worker* tmp = scramble[j];
    int other = myrand( &seed, n-1 );
    scramble[j] = scramble[other];
    scramble[other] = tmp;
  }
  // Finally, add cyclic suffix
  for( j=n-1; j<n-1+parsamp_size; j++ ) {
    scramble[j] = scramble[j-(n-1)];
  }

#if !WOOL_STEAL_SAMPLE
  self->pu.is_thief = 1;
#endif

#endif

  do {
    int steal_outcome = SO_NO_WORK;
    int poll_outcome = 0;
    int poll_ctr = 0;
    int skip_steal = 0;

    /*
     * Preparatory phase.
     */

    // We either come in from the start or return from a steal attempt.
    //
    // Depending on our greediness state we should either poll or go
    // straight for a steal.

    if( WOOL_WHEN_IF_FULL_STEAL( polling ) ) {
      poll_outcome = poll( scramble[i] );

      if( poll_outcome >= 0 ) {
        // poll_ctr++;
        if( poll_ctr == 0 ) {
          polling = max_fail_while_sampling;
          poll_ctr = 1;
        } else if( 1 && poll_outcome != v_depth ) {
          poll_ctr = poll_size;
        } else {
          poll_ctr++;
        }
        if( poll_outcome <= v_depth ) {
          v_pos = i;
          v_depth = poll_outcome;
        }
      } else {
        polling--;
        if( poll_outcome == -2 ) {
          steal_outcome = SO_THIEF;
        }
        if( ( polling | poll_ctr ) == 0 ) {
          // We're giving up sampling after finding no samples
          skip_steal = 1;
        }
      }

      if( COUNT_EVENTS && poll_ctr == 0 ) {
        PR_INC( self, CTR_steal_tries );
        time_event( self, 7 );
      }
    } else {
      v_pos = i;
    }

    // Computing a random number for every steal is too slow, so we
    // do some amount of sequential scanning of the workers and only
    // randomize once in a while, just to be sure.

    if( !WOOL_STEAL_NEW_SET && !WOOL_FIXED_STEAL && attempts > next_spin ) {
      if( attempts > next_yield ) {
        if( attempts > next_sleep ) {
          if( !is_old_thief ) {
            is_old_thief = new_old_thief( self );
          }
          next_sleep += local_sleep_interval;
        } else {
          sched_yield( );
        }
        next_yield += yield_interval;
      } else {
        v |= spin( self, backoff_mode ); // Delay and 'or' into volatile variable
        next_spin += n-1;
      }
    }

    if( !WOOL_STEAL_NEW_SET && !WOOL_FIXED_STEAL && i>0 ) {
      i--;
      victim_idx ++;
      // A couple of if's is faster than a %...
      if( victim_idx == self_idx ) victim_idx++;
      if( victim_idx >= n-1 ) victim_idx = 0;
    } else if( !WOOL_STEAL_NEW_SET && !WOOL_FIXED_STEAL && !AVOID_RANDOM ) {
      i = rand_interval > 0 ? myrand( &seed, rand_interval ) : 0;
      victim_idx = ( myrand( &seed, n-1 ) + self_idx + 1 ) % (n-1);
    } else if ( !WOOL_STEAL_NEW_SET && !WOOL_FIXED_STEAL ) {
      i=10000;
      victim_idx = self_idx >= n-1 ? 0 : self_idx+1;
    }

    /*
     * Steal phase.
     */

    // Always true for non-sampling versions.
    if( !( WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET ) ||
	( !skip_steal && ( poll_ctr == poll_size || polling == 0 ) ) ) {
      if( poll_ctr<0 || poll_ctr > poll_size ) dprint( "Unexpected poll_ctr = %d\n", poll_ctr );
#if !(WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET)
      v_pos = victim_idx;
#endif

      // Now steal
      PR_INC( self, CTR_steal_tries );
      steal_outcome = steal( self, scramble+v_pos, card, is_old_thief | ST_THIEF, NULL, 0 );

      attempts++;
      attempts = record_steal( self, n, attempts, steal_outcome );

      if( WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET &&
          steal_outcome == SO_STOLE ) {
        polling = max_fail_while_searching;
        v_pos = v_depth = v_depth_default;
      } else if( WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET ) {
        polling = 0;
      }
    }

    /*
     * Post-steal phase.
     */

    // Now find the next index
    if( steal_outcome == SO_STOLE ) {
#if WOOL_STEAL_NEW_SET
      #if WOOL_STEAL_SAMPLE && WOOL_STEAL_BACK
        i-= min_set_size/2;
        if( i<0 ) i+=n-1;
        first_victim = i;
      #else
        i = first_victim = myrand( &seed, n-1 );
        since_rand = 0;
      #endif
      n_seen = n_thieves = 0;
      self->pu.is_thief = 1;
#else
      next_yield = yield_interval;
      next_sleep = sleep_interval;
      next_spin = n-1;
      local_sleep_interval = sleep_interval;
      is_old_thief = 0;
#endif
    } else if ( WOOL_STEAL_NEW_SET ) {
      if( SO_IS_FAIL(steal_outcome) ) {
        n_thieves += SO_NUM_THIEVES(steal_outcome);
      }
      n_seen+=1+parsamp_size;
      if( n_seen > min_set_size && n_thieves >= max_thieves ) {
        // Start from the beginning of our set
        i = first_victim;
        since_rand += n_seen;
        n_seen = n_thieves = 0;
        // Check if we should do a new set
        if( since_rand > max_rand_interval ) {
          first_victim = i = myrand( &seed, n-1 );
          since_rand = 0;
        }
      } else {
        // Continue within the set
        for( j=0; j<parsamp_size+1; j++ ) {
          i++;
          if( i>n-2 ) i = 0;
          if( i==first_victim ) {
            n_thieves = n_seen = 0;
          }
        }
      }
    }
    #if WOOL_STEAL_NEW_SET && !WOOL_STEAL_SAMPLE
      victim_idx = i;
    #endif

    WOOL_WHEN_SYNC_MORE( wool_lock( &more_lock ); )
      more = self->pr.more_work;
    WOOL_WHEN_SYNC_MORE( wool_unlock( &more_lock ); )
  } while( more > 1 );

  reswitch_worker( self );
  return NULL;
}


static void *do_work( void *arg )
{
  Worker **self_p = (Worker **) arg;
  int self_idx = self_p - workers;

  // The thread leader always initializes the helpers, for NUMA reasons
  init_workers( self_idx, workers_per_thread );

  wait_for_init_done(self_idx / workers_per_thread);

  #if LOG_EVENTS
    sync_clocks( workers[self_idx] );
  #endif

  #if WOOL_PIE_TIMES
    (*self_p)->pr.time = gethrtime();
  #endif

  wool_lock( &( (*self_p)->pu.work_lock )  );
  if( (*self_p)->pu.fun == NULL && (*self_p)->pr.more_work > 0 ) {
    pthread_cond_wait( &( (*self_p)->pu.work_available ), &( (*self_p)->pu.work_lock ) );
  }

  while( (*self_p)->pr.more_work > 0 ) {
    (*self_p)->pu.fun( (*self_p)->pu.fun_arg );
    (*self_p)->pu.fun = NULL;
    pthread_cond_wait( &( (*self_p)->pu.work_available ), &( (*self_p)->pu.work_lock ) );
  }
  wool_unlock( &( (*self_p)->pu.work_lock ) );

  time_event( workers[self_idx], 9 );

  return NULL;
}

#if COUNT_EVENTS

typedef enum {
  REPORT_NONE,
  REPORT_COUNTS,
  REPORT_PIE,
  REPORT_END
} report_t;

static const struct { const char *s; const report_t v; } report_names[] = {
  { "none", REPORT_NONE },
  { "counts", REPORT_COUNTS },
  { "pie", REPORT_PIE },
  { NULL, REPORT_END }
};

static report_t report_type( const char *str )
{
  int i = 0;
  while( report_names[i].s != NULL ) {
    if( !strcmp( str, report_names[i].s ) ) {
      return report_names[i].v;
    }
    i++;
  }
  return REPORT_COUNTS; // default
}

// default if not set from command line
static report_t global_report_type = WOOL_STAT ? REPORT_NONE : WOOL_PIE_TIMES ? REPORT_PIE : REPORT_COUNTS;

#if WOOL_PIE_TIMES

char *ctr_h[] = {
  "    Spawns",
  "   Inlined",
  "     Read",
  "     Wait",
  NULL, // "Sync_lck",
  "Steal_tries",
  "S_lck",
  "  Steals",
  "Leap_tries",
  "L_lck",
  "   Leaps",
  NULL, // "     Spins",
  "1_steal",
  " 1_tries",
  "p_steal",
  " p_tries",
  "h_steal",
  " h_tries",
  "m_steal",
  " m_tries",
  NULL, // "Sync_ND",
  " SNI",
  NULL, // "Skip_try",
  NULL, // "    Skip",
  " Add_st",
  " Sub_st",
  " Unst_steal",
  " Init_time",
  "  W_app_time",
  "W_steal_time",
  "  L_app_time",
  "L_steal_time",
  " Exit_time",
  "W_succ_time",
  "L_succ_time",
  "W_sig_time",
  "L_sig_time",
  "Sl_syncs",
  "Sl_spawns",
  "Pub_syncs",
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
#if WOOL_FAST_TIME
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
#endif
#if WOOL_BALARM_CACHING
  " w_reread",
#endif
  "   trlf",
  "trlf_iters",
};

#else

char *ctr_h[] = {
  "    Spawns",
  "   Inlined",
  "    Read",
  "    Wait",
  NULL, // "Sync_lck",
  "Steal_tries",
  "S_lck",
  "  Steals",
  "Leap_tries",
  "L_lck",
  "   Leaps",
  NULL, // "     Spins",
  "1_steal",
  " 1_tries",
  "p_steal",
  " p_tries",
  "h_steal",
  " h_tries",
  "m_steal",
  " m_tries",
  NULL, // "Sync_ND",
  " SNI",
  NULL, // "Skip_try",
  NULL, // "    Skip",
  " Add_st",
  " Sub_st",
  " Unst_steal",
  NULL, // " Init_time",
  NULL, // "  W_app_time",
  NULL, // "W_steal_time",
  NULL, // "  L_app_time",
  NULL, // "L_steal_time",
  NULL, // " Exit_time",
  NULL, // "W_succ_time",
  NULL, // "L_succ_time",
  NULL, // "W_sig_time",
  NULL, // "L_sig_time",
  "Sl_syncs",
  "Sl_spawns",
  "Pub_syncs",
  NULL, // " L_fail_0",
  NULL, // " L_fail_1",
  NULL, // " L_fail_2",
  NULL, // " L_fail_3",
  NULL, // " L_fail_4",
  NULL, // " L_fail_5",
  NULL, // " L_fail_6",
  NULL, // " L_fail_7",
#if WOOL_FAST_TIME
  "  vic_read",
  "  bot_read",
  "      peek",
  "  pre_xchg",
  " post_xchg",
  " post_test",
  " post_cmit",
  "    pre_rs",
  "   post_rs",
  "     pre_e",
#endif
#if WOOL_BALARM_CACHING
  " w_reread",
#endif
  "   trlf",
  "trlf_iters",
};

#endif

unsigned long long ctr_all[ CTR_MAX ];

#endif

static void signal_worker_shutdown( void )
{
  int i;
  WOOL_WHEN_SYNC_MORE( wool_lock( &more_lock ); )
  for( i = 0; i < n_workers; i++) {
    // Quit look_for_work().
    workers[i]->pr.more_work = 1;
    // This releases work_lock, set more_work under work_lock.
    wool_lock( &( workers[i]->pu.work_lock ) );
    workers[i]->pr.more_work = 0;
    wool_unlock( &( workers[i]->pu.work_lock ) );
    //  Signal the worker to release it.
    pthread_cond_signal( &( workers[i]->pu.work_available) );
  }
  WOOL_WHEN_SYNC_MORE( wool_unlock( &more_lock ); )
}

void wool_fini( void )
{
  int i;
  FILE *log_file;
#if COUNT_EVENTS
  int j;
#endif
#if WOOL_PIE_TIMES
  unsigned long long count_at_end;
#endif

  milestone_art = us_elapsed();

  #if LOG_EVENTS
    logEvent( workers[0], 2 );
  #endif
  time_event( workers[0], 2 );

  #if WOOL_MEASURE_SPAN
    __wool_update_time();
    last_time -= first_time;
  #endif

  signal_worker_shutdown();
  // fprintf( stderr, "Exiting with thread leader %d\n", workers[0]->thread_leader  );
  // More work is false here
  wool_lock( &sleep_lock );
    wool_broadcast( &sleep_cond );
  wool_unlock( &sleep_lock );

#if THREAD_GARAGE
  evacuate_garage();
#endif

  milestone_bwj = us_elapsed();
  for( i = 0; i < n_threads-1; i++ ) {
    // fprintf( stderr, "Joining with thread %d running some worker \n", i+1 );
    pthread_join( ts[i], NULL );
  }
  milestone_awj = us_elapsed();

  #if WOOL_PIE_TIMES || WOOL_MEASURE_SPAN
    count_at_end = gethrtime();
    ticks_per_ms = 1000 * (count_at_end - count_at_init_done) / ((double) milestone_awj - milestone_aid );
  #endif

  log_file = log_file_name == NULL ? stderr : fopen( log_file_name, "w" );

#if WOOL_MEASURE_SPAN
  fprintf( log_file, "TIME        %10.2f ms\n", ( (double) last_time ) / ticks_per_ms );
  fprintf( log_file, "SPAN        %10.2f ms\n", ( (double) last_span ) / ticks_per_ms );
  fprintf( log_file, "PARALLELISM %9.1f\n\n",
                    ( (double) last_time ) / (double) last_span );

  for( i=2; i<=64; i++ ) {
    double time_by_i = (double) last_time / i;
    double span = (double) last_span;
    double opt = span > time_by_i ? span : time_by_i;

    fprintf( log_file, "SPEEDUP %3.1f -- %3.1f on %d processors\n",
                     (double) last_time / (span+time_by_i),
                     (double) last_time / opt,
                     i );
  }
  fprintf( log_file, "\n" );

#endif

#if COUNT_EVENTS

  if( global_report_type == REPORT_COUNTS ) {

    fprintf( log_file, "SIZES  Worker %lu  Task %lu Lock %lu\n",
	     (unsigned long) sizeof(Worker),
	     (unsigned long) sizeof(Task),
	     (unsigned long) sizeof(wool_lock_t) );

    fprintf( log_file, "WorkerNo " );
    for( j = 0; j < CTR_MAX; j++ ) {
      if( ctr_h[j] != NULL ) {
        fprintf( log_file, "%s ", ctr_h[j] );
        ctr_all[j] = 0;
      }
    }
    for( i = 0; i < n_workers; i++ ) {
      unsigned long long *lctr = workers[i]->pr.ctr;
      lctr[ CTR_spawn ] = lctr[ CTR_inlined ] + lctr[ CTR_read ] + lctr[ CTR_waits ];
      fprintf( log_file, "\nSTAT %3d ", i );
      for( j = 0; j < CTR_MAX; j++ ) {
        if( ctr_h[j] != NULL ) {
          ctr_all[j] += lctr[j];
          fprintf( log_file, "%*llu ", (int) strlen( ctr_h[j] ), lctr[j] );
        }
      }
    }
    fprintf( log_file, "\n     ALL " );
    for( j = 0; j < CTR_MAX; j++ ) {
      if( ctr_h[j] != NULL ) {
        fprintf( log_file, "%*llu ", (int) strlen( ctr_h[j] ), ctr_all[j] );
      }
    }
    fprintf( log_file, "\n" );
  } else if( global_report_type == REPORT_PIE ) {
    unsigned long long sum_count;
    double dcpm = ticks_per_ms;
    int initial_steals = 0;

    fprintf( log_file, "\nMeasurement clock (tick) frequency:  %.2f GHz\n\n", ticks_per_ms / 1000000.0 );
    for( i = 0; i < n_workers; i++ ) {
      int j;
      unsigned long long *lctr = workers[i]->pr.ctr;
      lctr[ CTR_spawn ] = lctr[ CTR_inlined ] + lctr[ CTR_read ] + lctr[ CTR_waits ];
      for( j = 0; j < CTR_MAX; j++ ) {
        ctr_all[j] += lctr[j];
      }
      if( lctr[CTR_wsteal] > 0 ) {
        initial_steals++;
      }
    }
    sum_count = ctr_all[CTR_init] + ctr_all[CTR_wapp] + ctr_all[CTR_lapp] + ctr_all[CTR_wsteal] + ctr_all[CTR_lsteal]
              + ctr_all[CTR_close] + ctr_all[CTR_wstealsucc] + ctr_all[CTR_lstealsucc] + ctr_all[CTR_wsignal]
              + ctr_all[CTR_lsignal];

    fprintf( log_file,  "Aggregated time per pie slice, total time: %.2f CPU seconds\n", sum_count / (1000*dcpm) );
    fprintf( log_file,  "Startup time:    %10.2f ms\n", ctr_all[CTR_init] / dcpm );
    fprintf( log_file,  "Steal work:      %10.2f ms\n", ctr_all[CTR_wapp] / dcpm );
    fprintf( log_file,  "Leap work:       %10.2f ms\n", ctr_all[CTR_lapp] / dcpm );
    fprintf( log_file,  "Steal overhead:  %10.2f ms\n", (ctr_all[CTR_wstealsucc]+ctr_all[CTR_wsignal]) / dcpm );
    fprintf( log_file,  "Leap overhead:   %10.2f ms\n", (ctr_all[CTR_lstealsucc]+ctr_all[CTR_lsignal]) / dcpm );
    fprintf( log_file,  "Steal search:    %10.2f ms\n", (ctr_all[CTR_wsteal]-ctr_all[CTR_wstealsucc]-ctr_all[CTR_wsignal]) / dcpm );
    fprintf( log_file,  "Leap search:     %10.2f ms\n", (ctr_all[CTR_lsteal]-ctr_all[CTR_lstealsucc]-ctr_all[CTR_lsignal]) / dcpm );
    fprintf( log_file,  "Exit time:       %10.2f ms\n", ctr_all[CTR_close] / dcpm );
    fprintf( log_file,  "\n" );

    double all_work_ms     = (ctr_all[CTR_wapp] + ctr_all[CTR_lapp]) / dcpm;
    double all_overhead_ms = (ctr_all[CTR_wstealsucc] + ctr_all[CTR_wsignal] + ctr_all[CTR_lstealsucc] + ctr_all[CTR_lsignal]) / dcpm;
    double all_search_ms   = (ctr_all[CTR_init] + ctr_all[CTR_wsteal] + ctr_all[CTR_lsteal] + ctr_all[CTR_close]) / dcpm
                           - all_overhead_ms;
    double all_ms = all_work_ms + all_overhead_ms + all_search_ms;

    fprintf( log_file,  "Aggregated time categories\n" );
    fprintf( log_file,  "Work:       %10.2f ms (%6.2f of total)\n", all_work_ms, 100 * all_work_ms / all_ms );
    fprintf( log_file,  "Overhead:   %10.2f ms (%6.2f of total)\n", all_overhead_ms, 100 * all_overhead_ms / all_ms );
    fprintf( log_file,  "Search:     %10.2f ms (%6.2f of total)\n", all_search_ms, 100 * all_search_ms / all_ms );
    fprintf( log_file,  "\n" );

    fprintf( log_file,  "Time per operation\n" );
    if( ctr_all[CTR_steals] - initial_steals != 0 ){
      fprintf( log_file,  "Steal success:  %5.0f ticks\n", (double) ctr_all[CTR_wstealsucc]/(ctr_all[CTR_steals]-initial_steals) );
    }
    if( ctr_all[CTR_steals] != ctr_all[CTR_steal_tries] ) {
      fprintf( log_file,  "Steal failure:  %5.0f ticks\n",
                         (double) (ctr_all[CTR_wsteal]-ctr_all[CTR_wstealsucc]) / (ctr_all[CTR_steal_tries]-ctr_all[CTR_steals]) );
    }
    if( ctr_all[CTR_leaps] != 0 ){
      fprintf( log_file,  "Leap success:   %5.0f ticks\n", (double) ctr_all[CTR_lstealsucc]/ctr_all[CTR_leaps] );
    }
    if( ctr_all[CTR_leaps] != ctr_all[CTR_leap_tries] ) {
      fprintf( log_file,  "Leap failure:   %5.0f ticks\n",
                         (double) (ctr_all[CTR_lsteal]-ctr_all[CTR_lstealsucc]) / (ctr_all[CTR_leap_tries]-ctr_all[CTR_leaps]) );
    }
    if( ctr_all[CTR_steals] != 0 ){
      fprintf( log_file,  "Steal signal:   %5.0f ticks\n", (double) ctr_all[CTR_wsignal]/ctr_all[CTR_steals] );
    }
    if( ctr_all[CTR_leaps] != 0 ){
      fprintf( log_file,  "Leap signal:    %5.0f ticks\n", (double) ctr_all[CTR_lsignal] / ctr_all[CTR_leaps] );
    }
    fprintf( log_file,  "\nSome more statistics on task sizes and stealing\n" );
    fprintf( log_file,  "Tasks spawned: %10llu (average work per task:  %10.0f ticks)\n",
                         ctr_all[CTR_spawn], (ctr_all[CTR_wapp]+ctr_all[CTR_lapp]) / (double) (ctr_all[CTR_spawn]+1) );
    fprintf( log_file,  "Tasks stolen:  %10llu (average work per steal: %10.0f ticks)\n",
                         ctr_all[CTR_steals]+ctr_all[CTR_leaps],
                         (ctr_all[CTR_wapp]+ctr_all[CTR_lapp]) / (double) (ctr_all[CTR_steals]+ctr_all[CTR_leaps]+1) );
  }
#endif

  milestone_end = us_elapsed();

#if WOOL_TIME

  fprintf( log_file, "\nMILESTONES %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f\n",
	             milestone_bcw / 1000.0,
	             milestone_acw / 1000.0,
	             milestone_air / 1000.0,
	             milestone_aid / 1000.0,
	             milestone_art / 1000.0,
	             milestone_bwj / 1000.0,
	             milestone_awj / 1000.0,
	             milestone_end / 1000.0);

#endif

#if LOG_EVENTS

  fprintf( log_file, "\n" );
  for( i = 1; i < n_procs; i++ ) {
    fprintf( log_file, "CLOCK %d, %15lld %15lld\n", i, diff[i], trip[i] );
  }
  fprintf( log_file, "\n" );

  for( i = 0; i < n_workers; i++ ) {
    LogEntry *p;
    hrtime_t curr_time = diff[i];

    fprintf( log_file, "\n" );
    for( p = logbuff[i]; p < workers[i]->pr.logptr; p++ ) {
      if( p->what == 0 ) {
        curr_time += p->time * MINOR_TIME;
        fprintf( log_file, "ADVANCE %d %d\n", i, p->time );
      } else {
        curr_time += p->time*TIME_STEP;
        fprintf( log_file, "EVENT %2d %3d %15lld\n", i, p->what, curr_time );
      }
    }
  }

#endif

#if 0
  for( i = 0; i < n_workers; i++ ) {
    Worker *w = workers[i];
    fprintf( stderr, "", w->pu.dq_bot - w->pr.dq_base );
    // fprintf( stderr, "%ld\n", w->pu.dq_bot - w->pr.dq_base );
  }
#endif

  if( log_file_name != NULL ) {
    fclose( log_file );
  }

}

// Starts the runtime system with workstealing if start_ws is true.
static void rts_init_start( int start_ws )
{
  int i;

  us_elapsed();

  if( sizeof( Worker ) % LINE_SIZE != 0 || sizeof( Task ) % LINE_SIZE != 0 ) {
    fprintf( stderr, "Unaligned Task (%lu) or Worker (%lu) size\n",
                     (unsigned long) sizeof(Task), (unsigned long) sizeof(Worker) );
    exit(1);
  }

  // fprintf( stderr, "Entering \n" );

  if( n_stealable == -1 ) {
    n_stealable = 3;
    for( i=n_procs; i>0; i >>= 1 ) {
      n_stealable += 2;
    }
    n_stealable -= n_stealable/4;
    if( n_procs == 1 ) {
      n_stealable = 0;
    }
  }

  if( workers_per_thread == 0 ) {
    // One worker per thread is typically enough with (transitive)
    // leap frogging.
    workers_per_thread = 1;
  }

  // If we handle joins by having more than one worker per processor, these extra worker might
  // be full scale threads or just fibres (user level threads).
  n_workers = n_procs * workers_per_thread;
  n_threads = THREAD_GARAGE ? n_workers : n_procs;

  // By default, we poll up to the square root of the number of workers
  #if WOOL_STEAL_SAMPLE
    if( global_poll_size == 0 ) {
      do {
        global_poll_size++;
      } while( global_poll_size * global_poll_size < n_workers );
    }
    if( global_poll_size > n_workers-1 ) global_poll_size = n_workers-1;
  #endif

  if( max_old_thieves == -1 ) {
    max_old_thieves = n_procs / 4 + 1;
  }

  ts      = malloc( (n_threads-1) * sizeof(pthread_t) );
  garage  = malloc( n_threads * sizeof( struct _Garage ) );
  make_common_data( n_workers );

  #if WOOL_INIT_SPIN
  {
    int i;
    init_barrier = malloc( n_procs * sizeof(int) );
    for( i = 0; i < n_procs; i++ ) {
      init_barrier[i] = 0;
    }
  }
  #endif

  pthread_attr_init( &worker_attr );
  pthread_attr_setscope( &worker_attr, PTHREAD_SCOPE_SYSTEM );
  pthread_attr_setstacksize( &worker_attr, worker_stack_size );

  tls_self = _WOOL_(key_create)();

  milestone_bcw = us_elapsed();

  // We only start thread leaders here; the helpers are either fibres or started later
  for( i=1; i < n_procs; i++ ) {
    pthread_create( ts+i*(n_threads/n_procs)-1,
                    &worker_attr,
                    &do_work,
                    workers+i*workers_per_thread );
  }

  milestone_acw = us_elapsed();

  init_workers( 0, workers_per_thread );
  milestone_air = us_elapsed();
  wait_for_init_done( 0 );
  milestone_aid = us_elapsed();

  if( start_ws ) {
    work_for( (workfun_t) look_for_work, NULL );
  }

  #if WOOL_PIE_TIMES || WOOL_MEASURE_SPAN
    count_at_init_done = gethrtime();
  #endif



  #if LOG_EVENTS
    first_time = gethrtime( );
  #endif

  #if LOG_EVENTS
    master_sync( );
    logEvent( workers[0], 1 );
  #endif
  time_event( workers[0], 1 );

  #if WOOL_MEASURE_SPAN
    first_time = gethrtime();
    last_time = first_time;
    last_span = 0;
  #endif

}

void wool_init_start( void )
{
  rts_init_start( 1 );
}

int wool_init_options( int argc, char **argv )
{
  int a_ctr = 0, i = 0;
#ifndef __APPLE__
  cpu_set_t mask;

  // Default number of processes and worker affinities are given by looking at the
  // affinity of the root worker.
  affinity_mode = 3;
  sched_getaffinity( 0, sizeof(cpu_set_t), &mask );
  n_procs = CPU_COUNT( &mask );
  while( a_ctr < n_procs ) {
    if( CPU_ISSET( i, &mask ) ) {
      affinity_table[a_ctr] = i+1;  // There are no zeros in the affinity_table
      a_ctr++;
    }
    i++;
  }
#endif
  a_ctr = 0; // In case there are command line options for affinity

  workers_per_thread = 0;
  opterr = 0;

  if( argc == 0 ) return 0; // Sometimes we start Wool without giving it any command line options, but we still want the affinity set.

  // An old Solaris box I love does not support long options...
  while( 1 ) {
    int c;

    c = getopt( argc, argv, "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:L:R:" );

    if( c == -1 || c == '?' ) break;

    switch( c ) {
      case 'p': n_procs = atoi( optarg );
                break;
      case 's': n_stealable = atoi( optarg );
                break;
#if 1
      case 'b': backoff_mode = atoi( optarg );
                break;
      case 'r': rand_interval = atoi( optarg );
                break;
#endif
      case 't': worker_stack_size = atoi( optarg );
                break;
      case 'y': yield_interval = atoi( optarg );
                break;
      case 'i': sleep_interval = atoi( optarg );
                break;
      case 'o': max_old_thieves = atoi( optarg );
                break;
#if LOG_EVENTS
      case 'e': event_mask = atoi( optarg );
                break;
#endif
      case 'w': workers_per_thread = atoi( optarg );
                break;
#if WOOL_MEASURE_SPAN
      case 'c': __wool_sc = (hrtime_t) atoi( optarg );
                break;
#else
      case 'h': switch_interval = atoi( optarg );
                break;
#if WOOL_STEAL_DKS
      case 'j': worker_migration_interval = atoi( optarg );
                break;
#endif
#endif
#if WOOL_STEAL_OLD_SET
      case 'f': global_refresh_interval  = atoi( optarg );
                break;
      case 'g': global_segment_size  = atoi( optarg );
                break;
      case 'x': global_thieves_per_victim_x10  = atoi( optarg );
                break;
#endif
#if WOOL_STEAL_NEW_SET
      case 'f': global_max_thieves = atoi( optarg );
                break;
      case 'x': global_min_set_size = atoi( optarg );
                break;
#endif
#if WOOL_STEAL_DKS
      case 'f': global_n_thieves  = atoi( optarg );
                break;
      case 'g': global_n_blocks  = atoi( optarg );
                break;
#endif
#if WOOL_ADD_STEALABLE && !WOOL_MEASURE_SPAN
      case 'c': stealable_chunk_size = atoi( optarg );
                break;
      case 'm': steal_margin = atoi( optarg );
                break;
      case 'u': unstolen_per_decrement = atoi( optarg );
                break;
#endif
#if WOOL_STEAL_SAMPLE
      case 'g': global_poll_size  = atoi( optarg );
                break;
#endif
#if WOOL_STEAL_SAMPLE && WOOL_STEAL_NEW_SET
      case 'n': global_max_fail_while_searching = atoi( optarg );
                break;
      case 'q': global_max_fail_while_sampling = atoi( optarg );
                break;
#endif
#ifndef __APPLE__
      case 'a': {
                  int arg = atoi( optarg );
                  if( arg < 0 ) {
                    affinity_mode = -arg;
                  } else {
                    affinity_table[ a_ctr++ ] = arg+1;
                    affinity_mode = 3;
                  }
                }
                break;
#endif
      case 'k': global_pref_dist  = atoi( optarg );
                break;
      case 'l': log_file_name = optarg;
                break;
      case 'z': worker_offset  = atoi( optarg );
                break;
#if WOOL_SLOW_STEAL
      case 'v': global_steal_delay  = atoi( optarg );
                break;
#endif
      case 'L': global_trlf_threshold = atoi( optarg );
                break;
#if COUNT_EVENTS
      case 'R': global_report_type = report_type( optarg );
                break;
#endif
    }
  }

  for( i = 1; i < argc-optind+1; i++ ) {
    argv[i] = argv[ i+optind-1 ];
  }
  argc = argc-optind+1;
  optind = 1;

  return argc;

}

int wool_init( int argc, char **argv )
{

  argc = wool_init_options( argc, argv );

  wool_init_start();

  return argc;

}

