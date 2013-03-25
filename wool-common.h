/*
   This file is part of Wool, a library for fine-grained independent 
   task parallelism

   Copyright (C) 2009- Karl-Filip Faxen
      kff@sics.se

   This Source Code Form is subject to the terms of the Mozilla Public
   License, v. 2.0. If a copy of the MPL was not distributed with this
   file, You can obtain one at http://mozilla.org/MPL/2.0/.

*/

#ifndef WOOL_COMMON_H
#define WOOL_COMMON_H

#ifndef WOOL_DEBUG
  #define NDEBUG
#endif

// Temporary fix!
#if !defined(__sparc__) && !defined(__i386__) && !defined(__x86_64__) && !defined(__ia64__)
  #if !defined(__TILECC__)
    #define __TILECC__ 1
  #endif
#endif

#include <pthread.h>
#include <stdio.h>

#include <assert.h>

#define _WOOL_(V) _wool_##V
#define _WOOL_MAX_ARITY 10

#ifndef THE_SYNC
  #if defined(__ia64__) && ! defined(__INTEL_COMPILER)
    #define THE_SYNC 1
  #else
    #define THE_SYNC 0
  #endif
#endif

#ifndef WOOL_PUB_OUTLINE
  #define WOOL_PUB_OUTLINE 1
#endif

#if defined(__TILECC__)

  #include <atomic.h>
  #include <tmc/mem.h>

  /* Now turn off features that do not work with the tile64 */
  #define SINGLE_FIELD_SYNC 0
  #ifndef TWO_FIELD_SYNC
    #define TWO_FIELD_SYNC (!THE_SYNC)
  #endif

#endif

#if defined(__ia64__) && defined(__INTEL_COMPILER)
  #include <ia64intrin.h>
#endif

#ifndef WOOL_INLINED_BOT_DEC
  #define WOOL_INLINED_BOT_DEC 1
#endif

#ifndef WOOL_FIXED_STEAL
  #define WOOL_FIXED_STEAL 0
#endif

#if WOOL_FIXED_STEAL
  #define WOOL_STEAL_DKS 0
  #define SINGLE_FIELD_SYNC 0
  #define WOOL_DEFER_BOT_DEC 0
#endif

#ifndef TWO_FIELD_SYNC
  #define TWO_FIELD_SYNC 1
#endif

#ifndef WOOL_BALARM_CACHING
  #define WOOL_BALARM_CACHING 1
#endif

#ifndef WOOL_FAST_EXC
  #define WOOL_FAST_EXC 0
#endif

#define SYNC_MORE 0

#ifndef WOOL_PIE_TIMES
  #define WOOL_PIE_TIMES 0
#endif

#ifndef WOOL_FAST_TIME
  #define WOOL_FAST_TIME 0
#endif

#ifndef COUNT_EVENTS
  #define COUNT_EVENTS ( WOOL_PIE_TIMES | WOOL_FAST_TIME )
#endif

#ifndef COUNT_EVENTS_EXP
  #define COUNT_EVENTS_EXP 0
#endif

#ifndef SPAWN_FENCE
  #define SPAWN_FENCE 0  /* correct for x86 */
#endif

#ifndef FINEST_GRAIN
  #define FINEST_GRAIN 2000
#endif

#ifndef LOG_EVENTS
  #define LOG_EVENTS 0
#endif

#ifndef WOOL_DEFER_BOT_DEC
  #define WOOL_DEFER_BOT_DEC 1
#endif

#ifndef WOOL_DEFER_NOT_STOLEN
  #define WOOL_DEFER_NOT_STOLEN 1
#endif

#ifndef WOOL_READ_STOLEN
  #define WOOL_READ_STOLEN 0
#endif

#ifndef WOOL_STEAL_TWO_P_NB
  #define WOOL_STEAL_TWO_P_NB 0
#endif

#ifndef WOOL_ADD_STEALABLE
  #define WOOL_ADD_STEALABLE 1
#endif

#ifndef SINGLE_FIELD_SYNC
  #define SINGLE_FIELD_SYNC 0
#endif

#ifndef WOOL_MEASURE_SPAN
  #define WOOL_MEASURE_SPAN 0
#endif

#if WOOL_MEASURE_SPAN
  #define WOOL_WHEN_MSPAN( x ) x
#else
  #define WOOL_WHEN_MSPAN( x )
#endif

#ifndef LINE_SIZE
  #if defined(__ia64__)
    #define LINE_SIZE 128 /* Good for SGI Altix; who else uses Itanic? */
  #else
    #define LINE_SIZE 64  /* A common value for current processors */
  #endif
#endif

#define _WOOL_pool_blocks 8

#define SMALL_BODY             2
#define MEDIUM_BODY          100
#define LARGE_BODY  FINEST_GRAIN

#define P_SZ (sizeof(void *))
#define I_SZ (sizeof(int))
#define L_SZ (sizeof(long int))
#define LL_SZ (sizeof(long long int))

#define PAD(x,b) ( ( (b) - ((x)%(b)) ) & ((b)-1) ) /* b must be power of 2 */ 
#define ROUND(x,b) ( (x) + PAD( (x), (b) ) )

#ifndef TASK_PAYLOAD
  #define TASK_PAYLOAD (_WOOL_MAX_ARITY*8)
#endif

unsigned block_size(int);

#define IN_CURRENT(self,p) (self->block_base[self->t_idx] <= p && \
                            p < self->block_base[self->t_idx] + block_size( self->t_idx ) )

typedef long long unsigned int hrtime_t; 
typedef volatile unsigned long exarg_t;

#if 1 || defined(__TILECC__)
  #define TILE_INLINE __attribute__((__always_inline__))
#else
  #define TILE_INLINE
#endif

#if !defined(_WOOL_ordered_stores)
  #if defined(__TILECC__)
    #define _WOOL_ordered_stores 0
  #else
    // Currently, the ordered stores protocol appears not to work
    #define _WOOL_ordered_stores 0
  #endif
#endif

#if defined(__sparc__)
  #define SFENCE        asm volatile( "membar #StoreStore" )
  #define MFENCE        asm volatile( "membar #StoreLoad|#StoreStore" )
  #define PREFETCH(a)   asm ( "prefetch %0, 2" : : "m"(a) )
#elif defined(__TILECC__)
  #define PREFETCH(a)   /*  */
  #define SFENCE        tmc_mem_fence()
  #define MFENCE        tmc_mem_fence()
  #define EXCHANGE(R,M) (R = (typeof(R)) atomic_exchange_acq((int*) &(M), (int) R))
#elif defined(__i386__)
  #define SFENCE        asm volatile( "sfence" )
  #define MFENCE        asm volatile( "mfence" )
  #define PREFETCH(a)   /*  */
  #define EXCHANGE(R,M) asm volatile ( "xchg   %1, %0" : "+m" (M), "+r" (R) )
#elif defined(__x86_64__)
  #define SFENCE        asm volatile( "sfence" )
  #define MFENCE        asm volatile( "mfence" )
  /* { volatile int i=1; EXCHANGE( i, i ); } */
  #define PREFETCH(a)   /*  */
  #define EXCHANGE(R,M) asm volatile ( "xchg   %1, %0" : "+m" (M), "+r" (R) )
  #define CAS(R,M,V)  asm volatile ( "lock cmpxchg %2, %1" \
                                     : "+a" (V), "+m"(M) : "r" (R) : "cc" )
#elif defined(__ia64__)
  #define SFENCE       /* */
  #define MFENCE        __sync_synchronize()
  #define PREFETCH(a)   /* */
  #define EXCHANGE(R,M) \
     ((R) = (typeof(R)) _InterlockedExchangeU64((exarg_t *) &(M), (exarg_t) R ))
#endif

#if defined(__ia64__) && defined(__INTEL_COMPILER)
  #define COMPILER_FENCE  __memory_barrier()
  #define STORE_PTR_REL(var,val) __st8_rel(&(var), (__int64) (val))
  #define STORE_INT_REL(var,val) __st4_rel(&(var),(int) (val))
  #define READ_PTR_ACQ(var,ty) ((ty) __ld8_acq( &(var) ))
  #define READ_INT_ACQ(var,ty) ((ty) __ld4_acq( &(var) ))
#elif defined(__TILECC__)
  #define COMPILER_FENCE  /* __memory_barrier() */
  #define STORE_PTR_REL(var,val) (SFENCE, (var) = (val))
  #define STORE_INT_REL(var,val) (SFENCE, (var) = (val))
  // The code below works if there is a dependence (including
  // control dependence) from this read to any read of a value 
  // that was protected by the value read here.
  #define READ_PTR_ACQ(var,ty) (var)
  #define READ_INT_ACQ(var,ty) (var)
#else
  #define COMPILER_FENCE  asm volatile( "" )
  // x86, amd64 and SPARC v9 can do without a store barrier
  #define STORE_PTR_REL(var,val) ((var) = (val))
  #define STORE_INT_REL(var,val) ((var) = (val))
  // Depends on when reads are reordered with reads
  #define READ_PTR_ACQ(var,ty) (var)
  #define READ_INT_ACQ(var,ty) (var)
#endif

#define STORE_WRAPPER_REL(var,val)  STORE_PTR_REL( (var), (val) )
#define READ_WRAPPER_ACQ(var)       READ_PTR_ACQ( (var), wrapper_t )
#if WOOL_BALARM_CACHING
  #define STORE_BALARM_T_REL(var,val) STORE_PTR_REL( (var), (val) )
  #define READ_BALARM_T_ACQ(var)      READ_PTR_ACQ( (var), balarm_t )
#else
  #define STORE_BALARM_T_REL(var,val) STORE_INT_REL( (var), (val) )
  #define READ_BALARM_T_ACQ(var)      READ_INT_ACQ( (var), balarm_t )
#endif

WOOL_WHEN_MSPAN( extern hrtime_t __wool_sc; )
WOOL_WHEN_MSPAN( extern hrtime_t __wool_update_time(void); )
WOOL_WHEN_MSPAN( extern void __wool_set_span( hrtime_t ); )



#if COUNT_EVENTS
#define PR_ADD(s,i,k) ( ((s)->ctr[i])+= k )
#else
#define PR_ADD(s,i,k) /* Empty */
#endif
#define PR_INC(s,i)  PR_ADD(s,i,1)

#if COUNT_EVENTS_EXP
#define PR_INC_EXP(s,i) (PR_INC(s,i))
#else
#define PR_INC_EXP(s,i) /* Empty */
#endif

typedef enum {
  CTR_spawn=0,
  CTR_inlined,
  CTR_read,
  CTR_waits,
  CTR_sync_lock,
  CTR_steal_tries,
  CTR_steal_locks,
  CTR_steals,
  CTR_leap_tries,
  CTR_leap_locks,
  CTR_leaps,
  CTR_spins,
  CTR_steal_1s,
  CTR_steal_1t,
  CTR_steal_ps,
  CTR_steal_pt,
  CTR_steal_hs,
  CTR_steal_ht,
  CTR_steal_ms,
  CTR_steal_mt,
  CTR_sync_no_dec,
  CTR_steal_no_inc,
  CTR_skip_try,
  CTR_skip,
  CTR_add_stealable,
  CTR_sub_stealable,
  CTR_unstolen_stealable,
  CTR_init,
  CTR_wapp,
  CTR_wsteal,
  CTR_lapp,
  CTR_lsteal,
  CTR_close,
  CTR_wstealsucc,
  CTR_lstealsucc,
  CTR_wsignal,
  CTR_lsignal,
  CTR_slow_syncs,
  CTR_slow_spawns,
  CTR_sync_public,
  CTR_lf_0,
  CTR_lf_1,
  CTR_lf_2,
  CTR_lf_3,
  CTR_lf_4,
  CTR_lf_5,
  CTR_lf_6,
  CTR_lf_7,
#if WOOL_FAST_TIME
  CTR_t_vread,
  CTR_t_bread,
  CTR_t_peek,
  CTR_t_pre_x,
  CTR_t_post_x,
  CTR_t_post_t,
  CTR_t_post_c,
  CTR_t_pre_rs,
  CTR_t_post_rs,
  CTR_t_pre_e,
#endif
#if WOOL_BALARM_CACHING
  CTR_wrapper_reread,
#endif
  CTR_trlf,
  CTR_trlf_iters,
  CTR_MAX
} CTR_index;

typedef pthread_mutex_t wool_lock_t;
typedef pthread_cond_t  wool_cond_t;

#define wool_lock(l)      pthread_mutex_lock( l )
#define wool_unlock(l)    pthread_mutex_unlock( l )
#define wool_trylock(l)   pthread_mutex_trylock( l )

#define wool_wait(c,l)    pthread_cond_wait( c, l )
#define wool_signal(c)    pthread_cond_signal( c )
#define wool_broadcast(c) pthread_cond_broadcast( c )

#if TWO_FIELD_SYNC
#define TASK_COMMON_FIELDS(ty)    \
  WOOL_WHEN_MSPAN( hrtime_t spawn_span; ) \
  void (*f)(struct _Worker *, struct _Task *, ty );  \
  unsigned long ssn;   \
  balarm_t balarm; 
#else
#define TASK_COMMON_FIELDS(ty)    \
  WOOL_WHEN_MSPAN( hrtime_t spawn_span; ) \
  void (*f)(struct _Worker *, struct _Task *, ty );  \
  balarm_t balarm;
#endif

struct _Task;
struct _Worker;

typedef void (* wrapper_t)( struct _Worker *, struct _Task *, struct _Task * );

#if WOOL_BALARM_CACHING
  typedef wrapper_t balarm_t;
#else
  typedef int       balarm_t;
#endif

typedef struct {
  TASK_COMMON_FIELDS( struct _Task * )
} __wool_task_common;

#define COMMON_FIELD_SIZE sizeof( __wool_task_common )

typedef struct _Task {
  TASK_COMMON_FIELDS( struct _Task * )
  char p1[ PAD( COMMON_FIELD_SIZE, P_SZ ) ];
  char d[ TASK_PAYLOAD ];
  char p2[ PAD( ROUND( COMMON_FIELD_SIZE, P_SZ ) + TASK_PAYLOAD, LINE_SIZE ) ];
} Task;

#if SINGLE_FIELD_SYNC
  typedef wrapper_t grab_res_t;
  #define GRAB_RES_IS_TASK( r ) ( SFS_IS_TASK( r ) )
  #define GRAB_RES_TASK_CONST   ( SFS_TASK( NULL ) )
  #define GRAB_RES_EMPTY_CONST  ( SFS_EMPTY )
  #define GET_TASK(f)           ( SFS_GET_TASK(f) )
#elif TWO_FIELD_SYNC
  typedef balarm_t  grab_res_t;
  #if WOOL_BALARM_CACHING
    #define TF_FREE ((balarm_t) 0) 
    #define TF_OCC  ((balarm_t) 1) // Necessary for test&set instructions
    #define TF_EXC  ((balarm_t) 2) 
    #define TF_LAST  TF_EXC
  #else
    #define TF_FREE 0 
    #define TF_OCC  1             // Necessary for test&set instructions
    #define TF_EXC  2
  #endif
  #define GRAB_RES_IS_TASK( r ) ( (r) == TF_FREE )
  #define GRAB_RES_TASK_CONST   ( TF_FREE ) // The following are a bit odd
  #define GRAB_RES_EMPTY_CONST  ( TF_OCC )
  #define GET_TASK(f)           ( f )
#else
  typedef balarm_t  grab_res_t;
  #define GRAB_RES_IS_TASK( r ) ( (r) == NOT_STOLEN )
  #define GRAB_RES_TASK_CONST   ( NOT_STOLEN )
  #define GRAB_RES_EMPTY_CONST  ( STOLEN_BUSY )
  #define GET_TASK(f)           ( f )
#endif

#define T_DONE ((wrapper_t) 0)
#define T_BUSY ((wrapper_t) 1)
#define T_LAST ((wrapper_t) 1)

#if SINGLE_FIELD_SYNC || TWO_FIELD_SYNC

#define SFS_EMPTY        ((wrapper_t) 1)
/* #define SFS_BUSY         ((wrapper_t) 3) */
#define SFS_DONE         ((wrapper_t) 5)
#define SFS_STOLEN(t)    ((wrapper_t) (2*((long)(t)) + 7)) /* Assumes thieves use index */
#define SFS_TASK(f)      (f)
#define SFS_IS_TASK(s)   (!(((unsigned long) (s)) & 1))
#define SFS_GET_TASK(s)  (s)
#define SFS_GET_THIEF(s) ( (((unsigned long) (s))-7) / 2 )
#define SFS_IS_STOLEN(s) ( ( ((unsigned long) (s)) & 1 ) && ((unsigned long) (s)) >= 7 )

#endif

#define INLINED     ( -4 )
#define NOT_STOLEN  ( -3 )
#define STOLEN_BUSY ( -2 ) // Not used with LF 
#define STOLEN_DONE ( -1 )
#define B_LAST      STOLEN_DONE

#ifndef COMPACT_LOG
 #define COMPACT_LOG 1
#endif

#if LOG_EVENTS
  #if COMPACT_LOG
    typedef unsigned char timefield_t;
    #define TIME_STEP   32
    #define MINOR_TIME (256*TIME_STEP)
    typedef struct _LogEntry {
      unsigned char time;
      unsigned char what;
    } LogEntry;
  #else
    typedef unsigned short timefield_t;
    #define TIME_STEP 1
    #define MINOR_TIME 65536
    typedef struct _LogEntry {
      unsigned short time;
      short what;
    } LogEntry;
  #endif
#endif

typedef union { 
    struct { unsigned int t,b; } p; 
    unsigned long long v;
} ppair_t;

typedef struct _Worker {
  // First cache line, private stuff often written by the owner
  unsigned long long ctr[CTR_MAX]; 
  volatile hrtime_t time;
  hrtime_t          now;
  Task             *wait_for;
  // The following fields maintain the blocked task pool, update on block shift
  Task             *join_first_private;   // Low boundary for underflow and signal check
#if _WOOL_ordered_stores
  Task             *spawn_high;  // High boundary for overflow and signal check
#else
  Task             *spawn_first_private; // Used in a fast check for spawns
#endif
  unsigned long     public_size; // Used in a fast check for absence of underflow and signal
  unsigned long     private_size; // sizeof(Task) less than the size of the private part
                                  // of the current block
  Task             *pr_top;             // A copy of the top pointer, used for forests
  unsigned long     n_public;           // total number of public task descriptors in pool
  Task             *dq_base;            // Always pointing the base of the dequeue
  volatile int      more_public_wanted; // Infrequently written by thieves, hence volatile 
  int               t_idx;              // Index of current block in pool
  int               unstolen_stealable; // Counts number of joins with unstolen public tasks
  int               idx;
  int               decrement_deferred;
  int               trlf_threshold;    // Number of failed classic leap attempts before trlf
  unsigned long     curr_block_fidx;
  Task             *curr_block_base;
  Task             *block_base[_WOOL_pool_blocks];

  unsigned long     highest_bot;       // The highest value of bot since the last less_stealable 
  Task             *privatizing_bound; // Only below this point is it sensible to call
                                       // less_stealable()
  Task             *dq_top;            // Not used in this version
#if LOG_EVENTS
  LogEntry         *logptr;
#else
  void             *logptr;
#endif
  volatile int      clock;
  int               thread_leader;
  // added block_idx, array of block base
  char pad2[ PAD( (_WOOL_pool_blocks+14)*P_SZ+2*sizeof(hrtime_t)+8*I_SZ+CTR_MAX*LL_SZ, LINE_SIZE ) ];

  // Second cache line, public stuff seldom written by the owner
  // These two ints are either the size of two pointers or one pointer
  volatile int            is_thief;    // Used with set and friends
  volatile int            flag;        // Something to write to just to not peek, also used as depth for sampling
  volatile unsigned long  dq_bot;      // The next task to steal
  volatile unsigned long  ssn;         // Sequence number, incremented when bot is decreased
  volatile unsigned long  pu_n_public; // Number of public task descriptors in pool, == n_public
           Task          *pu_block_base[_WOOL_pool_blocks]; // Public copy of the block pointers 
  wool_lock_t            *dq_lock;     // Mainly used for mutex among thieves, 
                                       // but also as backup for victim
  int            is_running;  // Used when stealing workers
  int            pad0;        // Padding for the lock
  wool_lock_t    the_lock;    // dq_lock points here
  char      pad1[ PAD( 4*I_SZ+(4+_WOOL_pool_blocks)*P_SZ+sizeof(wool_lock_t), LINE_SIZE ) ];

} Worker;

#if LOG_EVENTS
  void logEvent( Worker*, int );
#elif 0 && WOOL_PIE_TIMES
  #define logEvent( w, i ) time_event( w, i )
#else
  #define logEvent( w, i ) /* Nothing */
#endif

#define get_self( t ) ( t->self )

#if WOOL_MEASURE_SPAN

static hrtime_t _wool_mspan_before_inline( Task *t )
{
  hrtime_t e_span = __wool_update_time();
  __wool_set_span( t->spawn_span );
  return e_span;
}

static void _wool_mspan_after_inline( hrtime_t e_span, Task *t )
{
  hrtime_t c_span = __wool_update_time();
  hrtime_t one_span = e_span - t->spawn_span;
  hrtime_t two_span = c_span - t->spawn_span;

  if( __wool_sc > one_span || __wool_sc > two_span ) {
    __wool_set_span( c_span + one_span );
  } else if( c_span < e_span ) {
    __wool_set_span( e_span+__wool_sc );
  } else {
    __wool_set_span( c_span + __wool_sc );
  }
}

#define WOOL_MSPAN_BEFORE_INLINE( e_span, t ) \
    e_span = _wool_mspan_before_inline( t )


#define WOOL_MSPAN_AFTER_INLINE( e_span, t ) \
    _wool_mspan_after_inline( e_span, t )

#else

#define WOOL_MSPAN_BEFORE_INLINE( e_span, t ) /* Empty */
#define WOOL_MSPAN_AFTER_INLINE( e_span, t )  /* Empty */
#endif

 __attribute__((unused)) 
static const Task *__dq_top = NULL;
__attribute__((unused))
static const Worker *__self = NULL; 
__attribute__((unused))
static const int _WOOL_(in_task) = 0;

static inline TILE_INLINE wrapper_t 
cas_state( wrapper_t *mem, wrapper_t old, wrapper_t new_val )
{
  #if defined(__TILECC__)
     return
       (wrapper_t)
         atomic_compare_and_exchange_val_acq(
              (int*) mem, (int) new_val, (int) old );
  #elif defined(__GNUC__) && !defined(__INTEL_COMPILER)
    return __sync_val_compare_and_swap( mem, old, new_val );
  #elif defined(__INTEL_COMPILER) && defined(__x86_64__)
    CAS( new_val, *mem, old );
    return old;
  #elif defined(__INTEL_COMPILER)
    return (wrapper_t) __sync_val_compare_and_swap( 
                                (long *) mem, (long) old, (long) new_val );
  #endif
}

#ifdef __cplusplus
extern "C" {
#endif


#if THE_SYNC
  balarm_t _WOOL_(sync_get_balarm)( Task * );
#endif
// int main_CALL( Worker *, Task *, int, char ** );
Task *_WOOL_(slow_spawn)( Worker *, Task *, wrapper_t );
Task *_WOOL_(new_slow_sync)( Worker *, Task *, grab_res_t );
Worker *_WOOL_(slow_get_self)( void );
Task *_WOOL_(slow_get_top)( Worker * );

int  wool_init( int, char ** );
void wool_fini( void );

#ifdef __cplusplus
}
#endif

#define __wool_pop_task( w ) 0

static inline wrapper_t _WOOL_(exch_busy)( volatile wrapper_t *a )
{
  #if defined(__TILECC__)
    return (wrapper_t) __insn_tns((volatile int *) a);
  #else
    wrapper_t s = 
     #if SINGLE_FIELD_SYNC
       SFS_EMPTY;
     #else
       T_BUSY;
     #endif
    EXCHANGE( s, *(a) );
    return s;
  #endif
}

static inline balarm_t _WOOL_(exch_busy_balarm)( volatile balarm_t *a )
{
  #if defined(__TILECC__)
    return (balarm_t) __insn_tns((volatile int *) a);
  #else
    balarm_t s = TF_OCC;
    EXCHANGE( s, *(a) );
    return s;
  #endif
}

static inline void _wool_unbundled_mf(void)
{
  #if defined(__TILECC__)
    __insn_mf();
  #endif
  /* Maybe do something for ia64? */
}

static inline grab_res_t _WOOL_(owner_grab)( volatile Task *t )
{
#if THE_SYNC
  balarm_t a;

  if( WOOL_READ_STOLEN && t->balarm != NOT_STOLEN ) {
    return t->balarm;
  }
  t->f = T_BUSY;
  MFENCE;
  a = t->balarm;
  if( a==NOT_STOLEN ) {
    return a;
  } else {
    return sync_get_balarm( t );
  }
#elif SINGLE_FIELD_SYNC

  return _WOOL_(exch_busy)( (volatile wrapper_t *) &(t->f) );

#elif TWO_FIELD_SYNC

  return _WOOL_(exch_busy_balarm)( (volatile grab_res_t *) &(t->balarm) );

#else
  wrapper_t f = T_BUSY;

  if( WOOL_READ_STOLEN && t->f <= T_LAST ) {
    return STOLEN_BUSY;
  }
  f = _WOOL_(exch_busy)( (volatile wrapper_t *) &(t->f) );
  COMPILER_FENCE;
  if( f > T_LAST ) { // Needs to change!
    return NOT_STOLEN;
  } else {
    _wool_unbundled_mf();
    return STOLEN_BUSY;
  }
#endif
}

/* The fast functions are responsible for 
   1. determining if the fast case applies
   2. generating a new value for top
   3. if the slow case applies, by calling the slow path: 
        checking overflow of the task pool
        aquiring the task using synchronization (if stealable) (for sync), on fast case
        checking for deferred bot update (for spawn)
        checking for requests for more stealable tasks
*/

/* 
   *top should point to where the task will be spawned
   - it might point to the first task in a block
   - it might point to the last task in a block (but then we will push into the new block)
   low should point at the first task in the block, just beyond the header 
   high should point at the last (highest) task in the block
*/

extern unsigned long ptr2idx_curr( Worker *, Task * );

static inline __attribute__((__always_inline__))
void _WOOL_(fast_spawn)( Worker *self, Task **top, wrapper_t f )
{

  #if WOOL_MEASURE_SPAN
    p->spawn_span = __wool_update_time();
  #endif

  #if !defined(NDEBUG) && !_WOOL_ordered_stores
    if( !IN_CURRENT( self, *top ) ) {
      fprintf( stderr, "SP %d %lu %ld %ld %lu\n", 
                       self->idx, 
                       (unsigned long) f, 
                       (*top)-self->spawn_first_private, 
                       (*top) - self->block_base[0], 
                       self->n_public );
  }
  #endif

  assert( *top == self->pr_top );
  assert( IN_CURRENT( self, *top ) );

  /*
    Either self->spawn_first_private points to self->first_private, which points somewhere 
    within the block, or it points away from all blocks.

    On machines with total store order and similar (x86, x86_64, SPARC v9),
    there is no special case for spawn public task. Hence we use self->spawn_high which 
    points at the last (sentinel) task in the block.
  */

  #if LOG_EVENTS
    if( ptr2idx_curr( self, *top ) < self->n_public ) {
      logEvent( self, 5 );
    }
  #endif

  #if WOOL_BALARM_CACHING
    // (*top)->f = f;
  #endif


  #if _WOOL_ordered_stores
    /* We make no distinction between public and private spawn since no 
       additional fences are needed for the public case */
    if( *top < self->spawn_high ) {
      /* Fast case, public and private */
      #if WOOL_DEFER_NOT_STOLEN && !SINGLE_FIELD_SYNC && !TWO_FIELD_SYNC
        (*top)->balarm = NOT_STOLEN;
      #endif
      COMPILER_FENCE;
      (*top)->f = f;
      (*top) ++;
    } else {
      /* Slow case */
      *top = _WOOL_(slow_spawn)( self, *top, f );
    }
  #else
    /* We now make a distinction since the public case need extra memory synchronization */
    Task *sfp = self->spawn_first_private; // Might be used several times
   #if WOOL_INLINED_BOT_DEC
    int dec_def = self->decrement_deferred;
   #endif
    if( __builtin_expect( ((unsigned long) (*top) - (unsigned long) sfp ) 
			  < self->private_size, 1 ) ) {
      /* Fast case, private */
      (*top)->f = f;
      (*top) ++;
    } else if( __builtin_expect( *top < sfp, 1 ) ) {
      /* Semi fast case, public spawn */
      #if WOOL_INLINED_BOT_DEC
      if( __builtin_expect( dec_def, 0 ) ) {
        self->dq_bot = self->curr_block_fidx + ((*top) - self->curr_block_base);
        self->decrement_deferred = 0;
      }
      #endif
      #if WOOL_DEFER_NOT_STOLEN && !SINGLE_FIELD_SYNC && !TWO_FIELD_SYNC
        (*top)->balarm = NOT_STOLEN;
      #endif
      #if TWO_FIELD_SYNC
        #if WOOL_BALARM_CACHING
         (*top)->f = f;
          STORE_BALARM_T_REL( (*top)->balarm, f );
        #else
          (*top)->f = f;
          STORE_BALARM_T_REL( (*top)->balarm, TF_FREE );
        #endif
      #else
        STORE_WRAPPER_REL( (*top)->f, f );
      #endif
      (*top) ++;
      (*top)->f = SFS_EMPTY;
    } else {
      /* Slow case */
      *top = _WOOL_(slow_spawn)( self, *top, f );
    }
  #endif
  self->pr_top = *top;
}

static inline __attribute__((__always_inline__)) 
void _wool_when_sync_on_public( Worker *self, Task *top )
{
  #if  WOOL_ADD_STEALABLE
    // self->unstolen_stealable --;
  #endif

  PR_INC(self, CTR_sync_public);

}

#if WOOL_ADD_STEALABLE
// #define WOOL_LS_TEST(w,top) ( w->unstolen_stealable > 0 /* || w->privatizing_bound <= top */ )
#define WOOL_LS_TEST(us) ( us > 0 )
#define WOOL_WHEN_AS( x ) x
#define WOOL_WHEN_AS_C( x ) x,
#else
// #define WOOL_LS_TEST(w,top) 1
#define WOOL_LS_TEST(us) 1
#define WOOL_WHEN_AS( x ) /* Nothing */
#define WOOL_WHEN_AS_C( x ) /* Nothing */
#endif

static inline __attribute__((__always_inline__)) 
grab_res_t _WOOL_(grab_in_sync)( Worker *self, Task *top )
{
  #if TWO_FIELD_SYNC
    balarm_t res = _WOOL_(exch_busy_balarm)( &(top->balarm) );

    // fprintf( stderr, "?\n" );

    _WOOL_(when_sync_on_public)( self, top );
    #if _WOOL_ordered_stores
      if( res != TF_OCC ) {
        top->f = SFS_EMPTY;
        COMPILER_FENCE;
        top->balarm = TF_FREE;
        return TF_FREE;
      } else {
        return TF_OCC;
      }
    #else
      return res;
    #endif
  #endif
}

static inline __attribute__((__always_inline__)) 
int _WOOL_(new_fast_sync)( Worker *self, Task **top )
{
  Task *jfp = self->join_first_private;
  unsigned long ps = self->public_size;
#if WOOL_ADD_STEALABLE
  int us;
#endif
#if WOOL_FAST_EXC
  grab_res_t res = TF_EXC;
#else
  grab_res_t res = TF_OCC;
#endif

  assert( *top == self->pr_top );
  assert( IN_CURRENT( self, *top ) );
  if( __builtin_expect( jfp < *top, 1 ) ||
      ( (
        #if WOOL_ADD_STEALABLE
         us = self->unstolen_stealable,
        #endif
        __builtin_expect( (unsigned long) jfp - (unsigned long) *top < ps, 1 ) )
         && __builtin_expect( WOOL_LS_TEST(us), 1 ) 
        #if TWO_FIELD_SYNC
         && (res = _WOOL_(grab_in_sync)( self, (*top)-1 ),
        #else
         && (res = _WOOL_(owner_grab)((*top)-1), 
             _wool_when_sync_on_public( self, (*top)-1 ),
        #endif
             (
        #if WOOL_ADD_STEALABLE
               self->unstolen_stealable = us-1,
        #endif
             __builtin_expect( res != TF_OCC, 1 ) ) ) ) ) {
    /* Fast case */
    (*top)--;
    self->pr_top = *top;
    PR_INC( self, CTR_inlined );
    return 1;
  } else {
      /* An exceptional case */
      *top = _WOOL_(new_slow_sync)( self, *top, res );
      return 0;
  }
}

static inline __attribute__((always_inline)) 
unsigned long _WOOL_(max)( unsigned long a, unsigned long b )
{
  return b<a ? b : a;
}

#define SYNC( f )       ( f##_SYNC_DSP( (Worker *) __self, &__dq_top, _WOOL_(in_task) ) )
#define SPAWN( f, ... ) ( f##_SPAWN_DSP( (Worker *) __self, &__dq_top, _WOOL_(in_task) ,##__VA_ARGS__ ) )
#define CALL( f, ... )  ( f##_CALL_DSP( (Worker *) __self, (Task *) __dq_top, _WOOL_(in_task) , ##__VA_ARGS__ ) )
#define FOR( f, ... )   ( CALL( f##_TREE , ##__VA_ARGS__ ) )

#define _WOOL_OFFCON( a, t, before ) ( __alignof__(t) > a ? sizeof(t) : \
                                       before && __alignof__(t) == a ? sizeof(t) : 0 )

#endif /* not defined WOOL_COMMON_H */

